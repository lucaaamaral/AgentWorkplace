//! `workplace cli`: the interactive TUI (docs/architecture/daemon.md).
//!
//! One persistent broker connection: admin-registers, starts an unfiltered
//! watch, renders the live stream in the top pane, and takes slash commands
//! on the input line. Admin-tap deliveries are auto-acknowledged (the stream
//! pane is fed by watch/event, not by deliveries).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use protocol::methods as m;
use protocol::*;
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

enum Ui {
    Net(String),         // formatted line for the stream pane
    Input(KeyEvent),
    Disconnected,
    Tick,
}

struct Client {
    out: mpsc::UnboundedSender<Message>,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>,
}

impl Client {
    async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let _ = self.out.send(Message::Request(Request {
            jsonrpc: "2.0".into(),
            id: Id::Num(id),
            method: method.into(),
            params: Some(params),
        }));
        match tokio::time::timeout(std::time::Duration::from_secs(30), rx).await {
            Ok(Ok(r)) => r,
            _ => Err(RpcError { code: -32000, message: "broker timeout".into(), data: None }),
        }
    }
}

pub async fn run(addr: SocketAddr, name: String, version: String) -> anyhow::Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let (read_half, mut write_half) = stream.into_split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<Ui>();

    let client = Arc::new(Client {
        out: out_tx.clone(),
        next_id: AtomicU64::new(1),
        pending: Mutex::new(HashMap::new()),
    });

    // Writer.
    tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let mut line = msg.to_line();
            line.push('\n');
            if write_half.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    // Reader: watch events → pane lines; deliveries → auto-ack; responses → pending.
    {
        let client = client.clone();
        let ui_tx = ui_tx.clone();
        let out_tx = out_tx.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(read_half).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                match Message::parse(&line) {
                    Ok(Message::Notification(n)) if n.method == m::WATCH_EVENT => {
                        if let Ok(ev) =
                            serde_json::from_value::<WatchEvent>(n.params.unwrap_or(Value::Null))
                        {
                            let _ = ui_tx.send(Ui::Net(format_event(&ev)));
                        }
                    }
                    Ok(Message::Request(req)) if req.method == m::MESSAGE_DELIVER => {
                        // Admin tap: acknowledge; display comes from watch.
                        let _ = out_tx.send(Message::Response(ok_response(
                            req.id,
                            DeliverResult { status: "relayed".into() },
                        )));
                    }
                    Ok(Message::Response(resp)) => {
                        let key = match &resp.id {
                            Id::Num(n) => *n,
                            Id::Str(_) => continue,
                        };
                        if let Some(tx) = client.pending.lock().unwrap().remove(&key) {
                            let _ = tx.send(match resp.error {
                                Some(e) => Err(e),
                                None => Ok(resp.result.unwrap_or(Value::Null)),
                            });
                        }
                    }
                    _ => {}
                }
            }
            let _ = ui_tx.send(Ui::Disconnected);
        });
    }

    // Handshake: hello, admin/register, watch everything.
    let hello = HelloParams {
        client_info: ClientInfo {
            harness: None,
            version: version.clone(),
            pid: std::process::id(),
            cwd: std::env::current_dir().map(|p| p.display().to_string()).unwrap_or_default(),
        },
    };
    client.call(m::SESSION_HELLO, serde_json::to_value(&hello)?).await.map_err(rpc_fail)?;
    client
        .call(m::ADMIN_REGISTER, json!({ "name": name }))
        .await
        .map_err(|e| anyhow::anyhow!("admin registration as {name} failed: {}", e.message))?;
    client.call(m::WATCH_START, json!({})).await.map_err(rpc_fail)?;

    // Input thread (blocking crossterm reads).
    {
        let ui_tx = ui_tx.clone();
        std::thread::spawn(move || {
            loop {
                match crossterm::event::read() {
                    Ok(Event::Key(k)) => {
                        if ui_tx.send(Ui::Input(k)).is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
    }

    // Tick for redraw.
    {
        let ui_tx = ui_tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
            loop {
                interval.tick().await;
                if ui_tx.send(Ui::Tick).is_err() {
                    break;
                }
            }
        });
    }

    // Terminal.
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;

    let mut app = App::new(addr, name);
    app.push(format!("connected to {addr} as admin; watching everything. /help for commands."));

    let result = event_loop(&mut terminal, &mut app, &client, &mut ui_rx).await;

    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen)?;
    result
}

fn rpc_fail(e: RpcError) -> anyhow::Error {
    anyhow::anyhow!("{}", e.message)
}

struct App {
    addr: SocketAddr,
    principal: String,
    lines: Vec<String>,
    input: String,
    scroll_from_bottom: u16,
    focus: Option<String>,
    pending_delete: Option<(String, String)>, // (channel, token)
    quit: bool,
}

impl App {
    fn new(addr: SocketAddr, principal: String) -> Self {
        App {
            addr,
            principal,
            lines: Vec::new(),
            input: String::new(),
            scroll_from_bottom: 0,
            focus: None,
            pending_delete: None,
            quit: false,
        }
    }

    fn push(&mut self, line: String) {
        for part in line.split('\n') {
            self.lines.push(part.to_string());
        }
        if self.lines.len() > 5000 {
            let excess = self.lines.len() - 5000;
            self.lines.drain(..excess);
        }
    }
}

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
    client: &Arc<Client>,
    ui_rx: &mut mpsc::UnboundedReceiver<Ui>,
) -> anyhow::Result<()> {
    draw(terminal, app)?;
    while let Some(ev) = ui_rx.recv().await {
        match ev {
            Ui::Net(line) => app.push(line),
            Ui::Disconnected => app.push("!! broker connection lost — /quit and relaunch".into()),
            Ui::Tick => {}
            Ui::Input(key) => handle_key(app, client, key).await,
        }
        if app.quit {
            return Ok(());
        }
        draw(terminal, app)?;
    }
    Ok(())
}

async fn handle_key(app: &mut App, client: &Arc<Client>, key: KeyEvent) {
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => app.quit = true,
        KeyCode::Char(c) => app.input.push(c),
        KeyCode::Backspace => {
            app.input.pop();
        }
        KeyCode::PageUp => app.scroll_from_bottom = app.scroll_from_bottom.saturating_add(10),
        KeyCode::PageDown => app.scroll_from_bottom = app.scroll_from_bottom.saturating_sub(10),
        KeyCode::Enter => {
            let line = std::mem::take(&mut app.input);
            let line = line.trim().to_string();
            if !line.is_empty() {
                app.scroll_from_bottom = 0;
                run_command(app, client, &line).await;
            }
        }
        _ => {}
    }
}

async fn run_command(app: &mut App, client: &Arc<Client>, line: &str) {
    if !line.starts_with('/') {
        // Plain text posts to the focused channel.
        let Some(channel) = app.focus.clone() else {
            app.push("no focus channel: /focus #chan first, or /send <targets> <body>".into());
            return;
        };
        send(app, client, vec![channel], vec![], line.to_string()).await;
        return;
    }
    let mut parts = line.splitn(2, ' ');
    let cmd = parts.next().unwrap_or_default();
    let rest = parts.next().unwrap_or("").trim().to_string();
    match cmd {
        "/help" => app.push(HELP.trim().to_string()),
        "/quit" => app.quit = true,
        "/focus" => {
            if rest.starts_with('#') {
                app.focus = Some(rest.clone());
                app.push(format!("focus: {rest} (plain text now posts there)"));
            } else {
                app.push("usage: /focus #channel".into());
            }
        }
        "/send" => {
            let (targets, body) = split_targets(&rest);
            if body.is_empty() || targets.is_empty() {
                app.push("usage: /send <#chan|@principal ...> <body>".into());
                return;
            }
            let channels = targets.iter().filter(|t| t.starts_with('#')).cloned().collect();
            let principals = targets.iter().filter(|t| t.starts_with('@')).cloned().collect();
            send(app, client, channels, principals, body).await;
        }
        "/create" => simple(app, client, m::CHANNEL_CREATE, json!({ "name": rest })).await,
        "/sub" | "/unsub" => {
            let mut it = rest.split_whitespace();
            let (Some(p), Some(c)) = (it.next(), it.next()) else {
                app.push(format!("usage: {cmd} @principal #channel"));
                return;
            };
            let method = if cmd == "/sub" { m::ADMIN_SUBSCRIBE } else { m::ADMIN_UNSUBSCRIBE };
            simple(app, client, method, json!({ "principal": p, "channel": c })).await;
        }
        "/rename" => {
            let mut it = rest.split_whitespace();
            let (Some(old), Some(new)) = (it.next(), it.next()) else {
                app.push("usage: /rename #old #new".into());
                return;
            };
            simple(app, client, m::CHANNEL_RENAME, json!({ "channel": old, "new_name": new })).await;
        }
        "/archive" => simple(app, client, m::CHANNEL_ARCHIVE, json!({ "channel": rest })).await,
        "/unarchive" => simple(app, client, m::CHANNEL_UNARCHIVE, json!({ "channel": rest })).await,
        "/delete" => match client.call(m::CHANNEL_DELETE, json!({ "channel": rest })).await {
            Ok(v) => {
                let token = v["confirmation_token"].as_str().unwrap_or_default().to_string();
                app.pending_delete = Some((rest.clone(), token));
                app.push(format!(
                    "PERMANENT deletion of {rest} requested. Type `/confirm {rest}` within 30s to proceed."
                ));
            }
            Err(e) => app.push(format!("error: {}", e.message)),
        },
        "/confirm" => {
            let Some((channel, token)) = app.pending_delete.take() else {
                app.push("nothing pending confirmation".into());
                return;
            };
            if rest != channel {
                app.push(format!("confirmation aborted: typed name does not match {channel}"));
                return;
            }
            simple(
                app,
                client,
                m::CHANNEL_DELETE_CONFIRM,
                json!({ "channel": channel, "confirmation_token": token }),
            )
            .await;
        }
        "/status" => match rest.parse::<u64>() {
            Ok(id) => match client.call(m::MESSAGE_STATUS, json!({ "message_id": id })).await {
                Ok(v) => {
                    if let Ok(r) = serde_json::from_value::<MessageStatusResult>(v) {
                        app.push(format!("acks for message {id}:"));
                        for a in r.acks {
                            app.push(format!(
                                "  {} → {:?}{}",
                                a.recipient,
                                a.state,
                                a.reason.map(|r| format!(" ({r})")).unwrap_or_default()
                            ));
                        }
                    }
                }
                Err(e) => app.push(format!("error: {}", e.message)),
            },
            Err(_) => app.push("usage: /status <message-id>".into()),
        },
        "/history" => {
            let mut it = rest.split_whitespace();
            let Some(target) = it.next() else {
                app.push("usage: /history <#channel|@principal> [limit]".into());
                return;
            };
            let limit = it.next().and_then(|v| v.parse::<u32>().ok()).unwrap_or(20);
            let scope = if target.starts_with('#') {
                json!({ "channel": target })
            } else if app.principal != target {
                json!({ "dm_with": target })
            } else {
                app.push("that's you".into());
                return;
            };
            match client.call(m::HISTORY_GET, json!({ "scope": scope, "limit": limit })).await {
                Ok(v) => {
                    if let Ok(r) = serde_json::from_value::<HistoryResult>(v) {
                        app.push(format!("history {target} ({} records):", r.records.len()));
                        for rec in r.records {
                            app.push(format!("  {}", format_record(&rec)));
                        }
                    }
                }
                Err(e) => app.push(format!("error: {}", e.message)),
            }
        }
        "/who" => match client.call(m::DIRECTORY_WHO, json!({})).await {
            Ok(v) => {
                if let Ok(r) = serde_json::from_value::<WhoResult>(v) {
                    for c in r.channels {
                        app.push(format!("{}: {}", c.channel, c.subscribers.join(" ")));
                    }
                    let ps: Vec<String> = r
                        .principals
                        .into_iter()
                        .map(|p| format!("{}{}", p.principal, if p.active { "*" } else { "" }))
                        .collect();
                    app.push(format!("principals: {} (* = active)", ps.join(" ")));
                }
            }
            Err(e) => app.push(format!("error: {}", e.message)),
        },
        "/daemon" => match client.call(m::DAEMON_STATUS, json!({})).await {
            Ok(v) => app.push(v.to_string()),
            Err(e) => app.push(format!("error: {}", e.message)),
        },
        "/shutdown" => simple(app, client, m::DAEMON_SHUTDOWN, json!({})).await,
        other => app.push(format!("unknown command {other}; /help")),
    }
}

async fn send(app: &mut App, client: &Arc<Client>, channels: Vec<String>, principals: Vec<String>, body: String) {
    let params = json!({ "channels": channels, "principals": principals, "body": body });
    match client.call(m::MESSAGE_SEND, params).await {
        Ok(v) => {
            if let Ok(r) = serde_json::from_value::<SendResult>(v) {
                let mut note = format!("sent #{} (thread {})", r.message_id, r.thread_id);
                if !r.delivery.failed.is_empty() {
                    let f: Vec<String> = r
                        .delivery
                        .failed
                        .iter()
                        .map(|f| format!("{} ({})", f.principal, f.reason))
                        .collect();
                    note.push_str(&format!(" — failed: {}", f.join(", ")));
                }
                if let Some(ea) = r.delivery.empty_audience {
                    note.push_str(&format!(" — nobody heard it ({ea:?})"));
                }
                app.push(note);
            }
        }
        Err(e) => app.push(format!("error: {}", e.message)),
    }
}

async fn simple(app: &mut App, client: &Arc<Client>, method: &str, params: Value) {
    match client.call(method, params).await {
        Ok(_) => app.push("ok".into()),
        Err(e) => app.push(format!("error: {}", e.message)),
    }
}

fn split_targets(rest: &str) -> (Vec<String>, String) {
    let mut targets = Vec::new();
    let mut body_start = 0;
    for token in rest.split_whitespace() {
        if token.starts_with('#') || token.starts_with('@') {
            targets.push(token.to_string());
            body_start = rest.find(token).map(|i| i + token.len()).unwrap_or(body_start);
        } else {
            break;
        }
    }
    (targets, rest[body_start..].trim().to_string())
}

fn hhmmss(ms: u64) -> String {
    let secs = (ms / 1000) % 86_400;
    format!("{:02}:{:02}:{:02}", secs / 3600, (secs / 60) % 60, secs % 60)
}

fn format_event(ev: &WatchEvent) -> String {
    match ev {
        WatchEvent::Record(rec) => format_record(rec),
        WatchEvent::Ack(a) => format!(
            "{} · ack: msg {} → {:?} for {}{}",
            hhmmss(a.timestamp),
            a.message_id,
            a.state,
            a.recipient,
            a.reason.as_ref().map(|r| format!(" ({r})")).unwrap_or_default()
        ),
    }
}

fn format_record(rec: &Record) -> String {
    match rec {
        Record::Message { envelope: e } => {
            let target = if e.recipients.channels.is_empty() {
                format!("dm:{}", e.recipients.principals.join(","))
            } else {
                e.recipients.channels.join(",")
            };
            let at = if e.recipients.channels.is_empty() || e.recipients.principals.is_empty() {
                String::new()
            } else {
                format!(" @ {}", e.recipients.principals.join(","))
            };
            format!(
                "{} [{}] {}{}: {} (msg {}, thread {})",
                hhmmss(e.timestamp),
                target,
                e.sender,
                at,
                e.body,
                e.message_id,
                e.thread_id
            )
        }
        Record::System { timestamp, event, .. } => {
            format!("{} ⚙ {}", hhmmss(*timestamp), format_system(event))
        }
    }
}

fn format_system(e: &SystemEvent) -> String {
    match e {
        SystemEvent::Registered { principal, admin } => {
            format!("{principal} registered{}", if *admin { " (admin)" } else { "" })
        }
        SystemEvent::RegistrationDenied { name, reason } => {
            format!("registration of {name} denied: {reason}")
        }
        SystemEvent::Deregistered { principal } => format!("{principal} deregistered"),
        SystemEvent::Disconnected { principal } => format!("{principal} disconnected"),
        SystemEvent::Subscribed { principal, channel, by_admin } => {
            format!("{principal} subscribed to {channel}{}", if *by_admin { " (by admin)" } else { "" })
        }
        SystemEvent::Unsubscribed { principal, channel, by_admin } => {
            format!("{principal} unsubscribed from {channel}{}", if *by_admin { " (by admin)" } else { "" })
        }
        SystemEvent::ChannelCreated { channel, by } => format!("{by} created {channel}"),
        SystemEvent::ChannelRenamed { old_name, new_name, by } => {
            format!("{by} renamed {old_name} → {new_name}")
        }
        SystemEvent::ChannelArchived { channel, by } => format!("{by} archived {channel}"),
        SystemEvent::ChannelUnarchived { channel, by } => format!("{by} unarchived {channel}"),
        SystemEvent::ChannelDeleted { name, record_count, by, .. } => {
            format!("{by} PERMANENTLY DELETED {name} ({record_count} records) — tombstone")
        }
    }
}

const HELP: &str = "
commands:
  /focus #chan            plain text posts to the focused channel
  /send <targets> <body>  targets: #channels and/or @principals
  /create #chan           /rename #old #new
  /sub @p #chan           /unsub @p #chan        (admin overrides)
  /archive #chan          /unarchive #chan
  /delete #chan           then /confirm #chan    (PERMANENT)
  /history <#c|@p> [n]    /status <msg-id>
  /who                    /daemon
  /shutdown               /quit
";

fn draw(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &App,
) -> anyhow::Result<()> {
    terminal.draw(|f| {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(3)])
            .split(f.area());

        let height = chunks[0].height.saturating_sub(2) as usize;
        let total = app.lines.len();
        let bottom = total.saturating_sub(app.scroll_from_bottom as usize);
        let start = bottom.saturating_sub(height);
        let visible: Vec<Line> =
            app.lines[start..bottom].iter().map(|l| Line::from(l.as_str())).collect();
        let title = format!(
            " workplace · {} · {}{} ",
            app.addr,
            app.principal,
            app.focus.as_ref().map(|c| format!(" · focus {c}")).unwrap_or_default()
        );
        f.render_widget(
            Paragraph::new(visible)
                .wrap(Wrap { trim: false })
                .block(Block::default().borders(Borders::ALL).title(title)),
            chunks[0],
        );
        f.render_widget(
            Paragraph::new(app.input.as_str())
                .block(Block::default().borders(Borders::ALL).title(" input (/help) ")),
            chunks[1],
        );
        let cursor_x = chunks[1].x + 1 + app.input.chars().count() as u16;
        f.set_cursor_position((cursor_x.min(chunks[1].right().saturating_sub(2)), chunks[1].y + 1));
    })?;
    Ok(())
}
