//! `workplace cli`: the interactive TUI (docs/architecture/daemon.md).
//!
//! One persistent broker connection: admin-registers, starts an unfiltered
//! watch, renders the live stream in the top pane, and takes slash commands
//! on the input line. Admin-tap deliveries are auto-acknowledged (the stream
//! pane is fed by watch/event, not by deliveries).
//!
//! RPC never runs on the UI loop: every broker call is a spawned task whose
//! output comes back through the Ui channel, so redraw and input stay live
//! while a call is in flight.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use protocol::methods as m;
use protocol::*;
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

enum Ui {
    Net(String), // formatted line for the stream pane
    Input(KeyEvent),
    /// A /delete command produced a confirmation token.
    PendingDelete {
        channel: String,
        token: String,
    },
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
            out => {
                self.pending.lock().unwrap().remove(&id);
                let reason = match out {
                    Err(_) => "broker timeout",
                    _ => "broker connection lost",
                };
                Err(RpcError {
                    code: -32000,
                    message: reason.into(),
                    data: None,
                })
            }
        }
    }
}

/// Raw-mode/alternate-screen guard: restores the terminal on every exit path,
/// including error returns and unwinds.
struct TermGuard;

impl TermGuard {
    fn enter() -> anyhow::Result<TermGuard> {
        crossterm::terminal::enable_raw_mode()?;
        // Construct the guard BEFORE entering the alternate screen: if that
        // fails, the guard's drop still disables raw mode.
        let guard = TermGuard;
        crossterm::execute!(std::io::stdout(), crossterm::terminal::EnterAlternateScreen)?;
        Ok(guard)
    }
}

impl Drop for TermGuard {
    fn drop(&mut self) {
        let _ = crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen);
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

pub async fn run(
    addr: SocketAddr,
    name: String,
    version: String,
    auth_token: Option<String>,
    admin_token: String,
) -> anyhow::Result<()> {
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
                            DeliverResult {
                                status: "relayed".into(),
                            },
                        )));
                    }
                    Ok(Message::Response(resp)) => {
                        let key = match &resp.id {
                            Id::Num(n) => *n,
                            Id::Str(_) | Id::Null => continue,
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
            cwd: std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
        },
        auth_token,
    };
    client
        .call(m::SESSION_HELLO, serde_json::to_value(&hello)?)
        .await
        .map_err(rpc_fail)?;
    client
        .call(
            m::ADMIN_REGISTER,
            json!({ "name": name, "admin_token": admin_token }),
        )
        .await
        .map_err(|e| anyhow::anyhow!("admin registration as {name} failed: {}", e.message))?;
    client
        .call(m::WATCH_START, json!({}))
        .await
        .map_err(rpc_fail)?;

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

    // Terminal (RAII: restored on every exit path).
    let _term = TermGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(std::io::stdout()))?;

    let mut app = App::new(addr, name);
    app.push(format!(
        "connected to {addr} as admin; watching everything. /help for commands."
    ));

    event_loop(&mut terminal, &mut app, &client, &ui_tx, &mut ui_rx).await
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
    thread_focus: Option<u64>,
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
            thread_focus: None,
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
    ui_tx: &mpsc::UnboundedSender<Ui>,
    ui_rx: &mut mpsc::UnboundedReceiver<Ui>,
) -> anyhow::Result<()> {
    draw(terminal, app)?;
    while let Some(ev) = ui_rx.recv().await {
        match ev {
            Ui::Net(line) => app.push(line),
            Ui::PendingDelete { channel, token } => {
                app.push(format!(
                    "PERMANENT deletion of {channel} requested. Type `/confirm {channel}` within 30s to proceed."
                ));
                app.pending_delete = Some((channel, token));
            }
            Ui::Disconnected => app.push("!! broker connection lost — /quit and relaunch".into()),
            Ui::Tick => {}
            Ui::Input(key) => handle_key(app, client, ui_tx, key),
        }
        if app.quit {
            return Ok(());
        }
        draw(terminal, app)?;
    }
    Ok(())
}

fn handle_key(
    app: &mut App,
    client: &Arc<Client>,
    ui_tx: &mpsc::UnboundedSender<Ui>,
    key: KeyEvent,
) {
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => app.quit = true,
        KeyCode::Char(c) => app.input.push(c),
        KeyCode::Backspace => {
            app.input.pop();
        }
        KeyCode::Tab => complete_command(app),
        // Scroll state is in RENDERED rows (wrap-aware); draw clamps it to
        // the actual content height.
        KeyCode::Up => app.scroll_from_bottom = app.scroll_from_bottom.saturating_add(1),
        KeyCode::Down => app.scroll_from_bottom = app.scroll_from_bottom.saturating_sub(1),
        KeyCode::PageUp => app.scroll_from_bottom = app.scroll_from_bottom.saturating_add(10),
        KeyCode::PageDown => app.scroll_from_bottom = app.scroll_from_bottom.saturating_sub(10),
        KeyCode::End => app.scroll_from_bottom = 0,
        KeyCode::Enter => {
            let line = std::mem::take(&mut app.input);
            let line = line.trim().to_string();
            if !line.is_empty() {
                app.scroll_from_bottom = 0;
                run_command(app, client, ui_tx, &line);
            }
        }
        _ => {}
    }
}

const COMMANDS: &[&str] = &[
    "/archive",
    "/confirm",
    "/create",
    "/daemon",
    "/delete",
    "/focus",
    "/help",
    "/history",
    "/quit",
    "/rename",
    "/reply",
    "/send",
    "/shutdown",
    "/status",
    "/sub",
    "/thread",
    "/unarchive",
    "/unsub",
    "/who",
];

/// Tab completion for slash commands on the input line (daemon.md).
fn complete_command(app: &mut App) {
    if !app.input.starts_with('/') || app.input.contains(' ') {
        return;
    }
    let matches: Vec<&&str> = COMMANDS
        .iter()
        .filter(|c| c.starts_with(&app.input))
        .collect();
    match matches.as_slice() {
        [] => {}
        [one] => {
            app.input = format!("{one} ");
        }
        many => {
            let list: Vec<&str> = many.iter().map(|c| **c).collect();
            app.push(list.join("  "));
        }
    }
}

/// Spawn one broker call off the UI loop; its output lines come back through
/// the Ui channel.
fn spawn_rpc<F, Fut>(client: &Arc<Client>, ui_tx: &mpsc::UnboundedSender<Ui>, f: F)
where
    F: FnOnce(Arc<Client>, mpsc::UnboundedSender<Ui>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let client = client.clone();
    let ui_tx = ui_tx.clone();
    tokio::spawn(async move { f(client, ui_tx).await });
}

fn out(ui_tx: &mpsc::UnboundedSender<Ui>, line: String) {
    let _ = ui_tx.send(Ui::Net(line));
}

fn run_command(app: &mut App, client: &Arc<Client>, ui_tx: &mpsc::UnboundedSender<Ui>, line: &str) {
    if !line.starts_with('/') {
        // Plain text posts to the focused channel — into the focused thread
        // if one is set, otherwise starting a new thread.
        let Some(channel) = app.focus.clone() else {
            app.push("no focus channel: /focus #chan first, or /send <targets> <body>".into());
            return;
        };
        let thread = app.thread_focus;
        let body = line.to_string();
        spawn_rpc(client, ui_tx, move |client, ui_tx| async move {
            send(&client, &ui_tx, vec![channel], vec![], body, thread).await;
        });
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
            let channels: Vec<String> = targets
                .iter()
                .filter(|t| t.starts_with('#'))
                .cloned()
                .collect();
            let principals: Vec<String> = targets
                .iter()
                .filter(|t| t.starts_with('@'))
                .cloned()
                .collect();
            spawn_rpc(client, ui_tx, move |client, ui_tx| async move {
                send(&client, &ui_tx, channels, principals, body, None).await;
            });
        }
        "/reply" => {
            // /reply <thread_id> <#chan|@principal ...> <body>
            let mut it = rest.splitn(2, ' ');
            let Some(tid) = it.next().and_then(|t| t.parse::<u64>().ok()) else {
                app.push("usage: /reply <thread_id> <#chan|@principal ...> <body>".into());
                return;
            };
            let (targets, body) = split_targets(it.next().unwrap_or("").trim());
            if body.is_empty() || targets.is_empty() {
                app.push("usage: /reply <thread_id> <#chan|@principal ...> <body>".into());
                return;
            }
            let channels: Vec<String> = targets
                .iter()
                .filter(|t| t.starts_with('#'))
                .cloned()
                .collect();
            let principals: Vec<String> = targets
                .iter()
                .filter(|t| t.starts_with('@'))
                .cloned()
                .collect();
            spawn_rpc(client, ui_tx, move |client, ui_tx| async move {
                send(&client, &ui_tx, channels, principals, body, Some(tid)).await;
            });
        }
        "/thread" => {
            if rest.is_empty() {
                app.thread_focus = None;
                app.push("thread focus cleared (plain text starts new threads)".into());
            } else if let Ok(tid) = rest.parse::<u64>() {
                app.thread_focus = Some(tid);
                app.push(format!(
                    "thread focus: {tid} (plain text now replies into it)"
                ));
            } else {
                app.push("usage: /thread <thread_id>  (or /thread to clear)".into());
            }
        }
        "/create" => simple(client, ui_tx, m::CHANNEL_CREATE, json!({ "name": rest })),
        "/sub" | "/unsub" => {
            let mut it = rest.split_whitespace();
            let (Some(p), Some(c)) = (it.next(), it.next()) else {
                app.push(format!("usage: {cmd} @principal #channel"));
                return;
            };
            let method = if cmd == "/sub" {
                m::ADMIN_SUBSCRIBE
            } else {
                m::ADMIN_UNSUBSCRIBE
            };
            simple(
                client,
                ui_tx,
                method,
                json!({ "principal": p, "channel": c }),
            );
        }
        "/rename" => {
            let mut it = rest.split_whitespace();
            let (Some(old), Some(new)) = (it.next(), it.next()) else {
                app.push("usage: /rename #old #new".into());
                return;
            };
            simple(
                client,
                ui_tx,
                m::CHANNEL_RENAME,
                json!({ "channel": old, "new_name": new }),
            );
        }
        "/archive" => simple(
            client,
            ui_tx,
            m::CHANNEL_ARCHIVE,
            json!({ "channel": rest }),
        ),
        "/unarchive" => simple(
            client,
            ui_tx,
            m::CHANNEL_UNARCHIVE,
            json!({ "channel": rest }),
        ),
        "/delete" => {
            spawn_rpc(client, ui_tx, move |client, ui_tx| async move {
                match client
                    .call(m::CHANNEL_DELETE, json!({ "channel": rest }))
                    .await
                {
                    Ok(v) => {
                        let token = v["confirmation_token"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string();
                        let _ = ui_tx.send(Ui::PendingDelete {
                            channel: rest,
                            token,
                        });
                    }
                    Err(e) => out(&ui_tx, format!("error: {}", e.message)),
                }
            });
        }
        "/confirm" => {
            let Some((channel, token)) = app.pending_delete.take() else {
                app.push("nothing pending confirmation".into());
                return;
            };
            if rest != channel {
                app.push(format!(
                    "confirmation aborted: typed name does not match {channel}"
                ));
                return;
            }
            simple(
                client,
                ui_tx,
                m::CHANNEL_DELETE_CONFIRM,
                json!({ "channel": channel, "confirmation_token": token }),
            );
        }
        "/status" => match rest.parse::<u64>() {
            Ok(id) => {
                spawn_rpc(client, ui_tx, move |client, ui_tx| async move {
                    match client
                        .call(m::MESSAGE_STATUS, json!({ "message_id": id }))
                        .await
                    {
                        Ok(v) => {
                            if let Ok(r) = serde_json::from_value::<MessageStatusResult>(v) {
                                out(&ui_tx, format!("acks for message {id}:"));
                                for a in r.acks {
                                    out(
                                        &ui_tx,
                                        format!(
                                            "  {} → {:?}{}",
                                            a.recipient,
                                            a.state,
                                            a.reason.map(|r| format!(" ({r})")).unwrap_or_default()
                                        ),
                                    );
                                }
                            }
                        }
                        Err(e) => out(&ui_tx, format!("error: {}", e.message)),
                    }
                });
            }
            Err(_) => app.push("usage: /status <message-id>".into()),
        },
        "/history" => {
            let mut it = rest.split_whitespace();
            let Some(target) = it.next().map(String::from) else {
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
            spawn_rpc(client, ui_tx, move |client, ui_tx| async move {
                match client
                    .call(m::HISTORY_GET, json!({ "scope": scope, "limit": limit }))
                    .await
                {
                    Ok(v) => {
                        if let Ok(r) = serde_json::from_value::<HistoryResult>(v) {
                            out(
                                &ui_tx,
                                format!("history {target} ({} records):", r.records.len()),
                            );
                            for rec in r.records {
                                out(&ui_tx, format!("  {}", format_record(&rec)));
                            }
                        }
                    }
                    Err(e) => out(&ui_tx, format!("error: {}", e.message)),
                }
            });
        }
        "/who" => {
            spawn_rpc(client, ui_tx, move |client, ui_tx| async move {
                match client.call(m::DIRECTORY_WHO, json!({})).await {
                    Ok(v) => {
                        if let Ok(r) = serde_json::from_value::<WhoResult>(v) {
                            for c in r.channels {
                                out(
                                    &ui_tx,
                                    format!("{}: {}", c.channel, c.subscribers.join(" ")),
                                );
                            }
                            let ps: Vec<String> = r
                                .principals
                                .into_iter()
                                .map(|p| {
                                    format!("{}{}", p.principal, if p.active { "*" } else { "" })
                                })
                                .collect();
                            out(&ui_tx, format!("principals: {} (* = active)", ps.join(" ")));
                        }
                    }
                    Err(e) => out(&ui_tx, format!("error: {}", e.message)),
                }
            });
        }
        "/daemon" => {
            spawn_rpc(client, ui_tx, move |client, ui_tx| async move {
                match client.call(m::DAEMON_STATUS, json!({})).await {
                    Ok(v) => out(&ui_tx, v.to_string()),
                    Err(e) => out(&ui_tx, format!("error: {}", e.message)),
                }
            });
        }
        "/shutdown" => simple(client, ui_tx, m::DAEMON_SHUTDOWN, json!({})),
        other => app.push(format!("unknown command {other}; /help")),
    }
}

async fn send(
    client: &Arc<Client>,
    ui_tx: &mpsc::UnboundedSender<Ui>,
    channels: Vec<String>,
    principals: Vec<String>,
    body: String,
    thread_id: Option<u64>,
) {
    let mut params = json!({ "channels": channels, "principals": principals, "body": body });
    if let Some(tid) = thread_id {
        params["thread_id"] = json!(tid);
    }
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
                out(ui_tx, note);
            }
        }
        Err(e) => out(ui_tx, format!("error: {}", e.message)),
    }
}

fn simple(
    client: &Arc<Client>,
    ui_tx: &mpsc::UnboundedSender<Ui>,
    method: &'static str,
    params: Value,
) {
    spawn_rpc(client, ui_tx, move |client, ui_tx| async move {
        match client.call(method, params).await {
            Ok(_) => out(&ui_tx, "ok".into()),
            Err(e) => out(&ui_tx, format!("error: {}", e.message)),
        }
    });
}

/// Leading `#`/`@` tokens are targets; everything after the first non-target
/// token is the body (position-tracked, so repeated targets cannot confuse
/// the split).
fn split_targets(rest: &str) -> (Vec<String>, String) {
    let mut targets = Vec::new();
    let mut remainder = rest;
    loop {
        let trimmed = remainder.trim_start();
        let end = trimmed.find(char::is_whitespace).unwrap_or(trimmed.len());
        let token = &trimmed[..end];
        if token.starts_with('#') || token.starts_with('@') {
            targets.push(token.to_string());
            remainder = &trimmed[end..];
        } else {
            return (targets, trimmed.to_string());
        }
    }
}

/// Render a UTC millisecond timestamp as local HH:MM:SS (chrono handles the
/// platform's timezone database, Windows included).
fn hhmmss(ms: u64) -> String {
    use chrono::TimeZone;
    match chrono::Local.timestamp_millis_opt(ms as i64) {
        chrono::LocalResult::Single(dt) => dt.format("%H:%M:%S").to_string(),
        _ => "??:??:??".into(),
    }
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
            a.reason
                .as_ref()
                .map(|r| format!(" ({r})"))
                .unwrap_or_default()
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
        Record::System {
            timestamp, event, ..
        } => {
            format!("{} ⚙ {}", hhmmss(*timestamp), format_system(event))
        }
    }
}

fn format_system(e: &SystemEvent) -> String {
    match e {
        SystemEvent::Registered { principal, admin } => {
            format!(
                "{principal} registered{}",
                if *admin { " (admin)" } else { "" }
            )
        }
        SystemEvent::RegistrationDenied { name, reason } => {
            format!("registration of {name} denied: {reason}")
        }
        SystemEvent::Deregistered { principal } => format!("{principal} deregistered"),
        SystemEvent::Disconnected { principal } => format!("{principal} disconnected"),
        SystemEvent::Subscribed {
            principal,
            channel,
            by_admin,
        } => {
            format!(
                "{principal} subscribed to {channel}{}",
                if *by_admin { " (by admin)" } else { "" }
            )
        }
        SystemEvent::Unsubscribed {
            principal,
            channel,
            by_admin,
        } => {
            format!(
                "{principal} unsubscribed from {channel}{}",
                if *by_admin { " (by admin)" } else { "" }
            )
        }
        SystemEvent::ChannelCreated { channel, by } => format!("{by} created {channel}"),
        SystemEvent::ChannelRenamed {
            old_name,
            new_name,
            by,
        } => {
            format!("{by} renamed {old_name} → {new_name}")
        }
        SystemEvent::ChannelArchived { channel, by } => format!("{by} archived {channel}"),
        SystemEvent::ChannelUnarchived { channel, by } => format!("{by} unarchived {channel}"),
        SystemEvent::ChannelDeleted {
            name,
            record_count,
            by,
            ..
        } => {
            format!("{by} PERMANENTLY DELETED {name} ({record_count} records) — tombstone")
        }
    }
}

const HELP: &str = "
commands:
  /focus #chan               plain text posts to the focused channel
  /thread <id>               plain text replies into that thread (empty clears)
  /send <targets> <body>     targets: #channels and/or @principals
  /reply <tid> <targets> …   post <body> into thread <tid>
  /create #chan              /rename #old #new
  /sub @p #chan              /unsub @p #chan        (admin overrides)
  /archive #chan             /unarchive #chan
  /delete #chan              then /confirm #chan    (PERMANENT)
  /history <#c|@p> [n]       /status <msg-id>
  /who                       /daemon
  /shutdown                  /quit
";

/// Bottom-anchored viewport math in RENDERED rows: given the wrap-aware
/// content height, the pane height, and the requested scroll-back, returns
/// the Paragraph scroll offset from the top and the clamped scroll state
/// (over-scrolling past the oldest row sticks at the top).
fn scroll_offset_rows(total_rows: usize, viewport_rows: usize, from_bottom: u16) -> (u16, u16) {
    let max_from_bottom = total_rows.saturating_sub(viewport_rows);
    let clamped = (from_bottom as usize).min(max_from_bottom);
    let offset = max_from_bottom - clamped;
    (
        offset.min(u16::MAX as usize) as u16,
        clamped.min(u16::MAX as usize) as u16,
    )
}

fn draw(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
) -> anyhow::Result<()> {
    terminal.draw(|f| {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(3)])
            .split(f.area());

        // Scroll in RENDERED rows, not logical lines: long bus messages wrap
        // to several terminal rows, and slicing logical lines used to leave
        // the tail clipped below the pane. line_count is the renderer's own
        // wrap math (recomputed per frame; bounded by the 5000-line cap).
        let inner_width = chunks[0].width.saturating_sub(2);
        let viewport = chunks[0].height.saturating_sub(2) as usize;
        let stream = Paragraph::new(
            app.lines
                .iter()
                .map(|l| Line::from(l.as_str()))
                .collect::<Vec<_>>(),
        )
        .wrap(Wrap { trim: false });
        let total_rows = stream.line_count(inner_width);
        let (offset, clamped) = scroll_offset_rows(total_rows, viewport, app.scroll_from_bottom);
        app.scroll_from_bottom = clamped;
        let title = format!(
            " workplace · {} · {}{}{} ",
            app.addr,
            app.principal,
            app.focus
                .as_ref()
                .map(|c| format!(" · focus {c}"))
                .unwrap_or_default(),
            app.thread_focus
                .map(|t| format!(" · thread {t}"))
                .unwrap_or_default()
        );
        f.render_widget(
            stream
                .scroll((offset, 0))
                .block(Block::default().borders(Borders::ALL).title(title)),
            chunks[0],
        );
        f.render_widget(
            Paragraph::new(app.input.as_str()).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" input (/help) "),
            ),
            chunks[1],
        );
        let cursor_x = chunks[1].x + 1 + app.input.chars().count() as u16;
        f.set_cursor_position((
            cursor_x.min(chunks[1].right().saturating_sub(2)),
            chunks[1].y + 1,
        ));
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // C9 — characterization of the pure helpers before the R2 command
    // extraction moves them.

    #[test]
    fn split_targets_positions() {
        // Leading #/@ tokens are targets; the body starts at the first
        // non-target token, position-tracked.
        let (t, b) = split_targets("#a @b hello world");
        assert_eq!(t, vec!["#a", "@b"]);
        assert_eq!(b, "hello world");

        // A repeated target must not confuse the body split.
        let (t, b) = split_targets("#a #a #a done");
        assert_eq!(t, vec!["#a", "#a", "#a"]);
        assert_eq!(b, "done");

        // Body containing target-like tokens is untouched.
        let (t, b) = split_targets("#chan ping @someone about #topic");
        assert_eq!(t, vec!["#chan"]);
        assert_eq!(b, "ping @someone about #topic");

        // No targets / empty input.
        let (t, b) = split_targets("just words");
        assert!(t.is_empty());
        assert_eq!(b, "just words");
        let (t, b) = split_targets("");
        assert!(t.is_empty());
        assert_eq!(b, "");

        // Targets only, no body.
        let (t, b) = split_targets("#a @b");
        assert_eq!(t, vec!["#a", "@b"]);
        assert_eq!(b, "");
    }

    #[test]
    fn hhmmss_shape() {
        // Local-time rendering: assert the stable shape, not the timezone.
        let s = hhmmss(1_700_000_000_000);
        assert_eq!(s.len(), 8);
        assert_eq!(s.as_bytes()[2], b':');
        assert_eq!(s.as_bytes()[5], b':');
    }

    fn envelope(channels: &[&str], principals: &[&str]) -> Envelope {
        Envelope {
            message_id: 42,
            thread_id: 7,
            timestamp: 0,
            sender: "@a".into(),
            recipients: Recipients {
                channels: channels.iter().map(|s| s.to_string()).collect(),
                principals: principals.iter().map(|s| s.to_string()).collect(),
            },
            body: "the body".into(),
            truncated: false,
        }
    }

    #[test]
    fn format_record_channel_dm_and_intersection() {
        let ch = format_record(&Record::Message {
            envelope: envelope(&["#g"], &[]),
        });
        assert!(ch.contains("[#g]") && ch.contains("@a") && ch.contains("the body"));
        assert!(ch.contains("msg 42") && ch.contains("thread 7"));

        let dm = format_record(&Record::Message {
            envelope: envelope(&[], &["@b"]),
        });
        assert!(dm.contains("dm:@b"));

        let both = format_record(&Record::Message {
            envelope: envelope(&["#g"], &["@b"]),
        });
        assert!(both.contains("[#g]") && both.contains("@ @b"));
    }

    #[test]
    fn format_system_events() {
        let s = format_system(&SystemEvent::Registered {
            principal: "@a".into(),
            admin: true,
        });
        assert!(s.contains("@a") && s.contains("(admin)"));

        let s = format_system(&SystemEvent::Unsubscribed {
            principal: "@a".into(),
            channel: "#g".into(),
            by_admin: true,
        });
        assert!(s.contains("(by admin)"));

        let s = format_system(&SystemEvent::ChannelDeleted {
            channel_id: 1,
            name: "#gone".into(),
            record_count: 3,
            by: "@m".into(),
        });
        assert!(s.contains("PERMANENTLY DELETED") && s.contains("#gone") && s.contains("3"));
    }

    #[test]
    fn format_event_ack_line() {
        let s = format_event(&WatchEvent::Ack(AckTransition {
            kind: "ack".into(),
            message_id: 9,
            recipient: "@r".into(),
            state: AckState::Failed,
            timestamp: 0,
            reason: Some("disconnected".into()),
        }));
        assert!(s.contains("msg 9") && s.contains("@r") && s.contains("(disconnected)"));
    }

    #[test]
    fn push_caps_the_buffer_and_splits_lines() {
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let mut app = App::new(addr, "@m".into());
        app.push("one\ntwo".into());
        assert_eq!(app.lines, vec!["one".to_string(), "two".to_string()]);

        for i in 0..6000 {
            app.push(format!("l{i}"));
        }
        assert_eq!(app.lines.len(), 5000, "buffer must cap at 5000 lines");
        assert_eq!(app.lines.last().unwrap(), "l5999", "newest lines are kept");
    }

    fn dummy_client() -> Arc<Client> {
        Arc::new(Client {
            out: mpsc::unbounded_channel().0,
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
        })
    }

    #[tokio::test]
    async fn scroll_keys_saturate() {
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let (ui_tx, _ui_rx) = mpsc::unbounded_channel();
        let client = dummy_client();
        let mut app = App::new(addr, "@m".into());

        let key = |code| KeyEvent::new(code, KeyModifiers::NONE);
        handle_key(&mut app, &client, &ui_tx, key(KeyCode::PageDown));
        assert_eq!(
            app.scroll_from_bottom, 0,
            "PageDown at the tail saturates at 0"
        );
        handle_key(&mut app, &client, &ui_tx, key(KeyCode::PageUp));
        assert_eq!(app.scroll_from_bottom, 10);
        handle_key(&mut app, &client, &ui_tx, key(KeyCode::PageDown));
        assert_eq!(app.scroll_from_bottom, 0);
    }

    #[test]
    fn scroll_offset_rows_math() {
        // Content shorter than the viewport: pinned to top, no scroll-back.
        assert_eq!(scroll_offset_rows(10, 20, 0), (0, 0));
        assert_eq!(scroll_offset_rows(10, 20, 7), (0, 0));
        // Exact fit.
        assert_eq!(scroll_offset_rows(20, 20, 5), (0, 0));
        // Longer content, at the tail: offset shows the newest rows.
        assert_eq!(scroll_offset_rows(50, 20, 0), (30, 0));
        // Scrolled back within range.
        assert_eq!(scroll_offset_rows(50, 20, 12), (18, 12));
        // Over-scroll clamps at the oldest row.
        assert_eq!(scroll_offset_rows(50, 20, 999), (0, 30));
    }

    #[test]
    fn command_completion() {
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        // Unique prefix completes with a trailing space.
        let mut app = App::new(addr, "@m".into());
        app.input = "/wh".into();
        complete_command(&mut app);
        assert_eq!(app.input, "/who ");

        // Ambiguous prefix lists candidates without changing the input.
        let mut app = App::new(addr, "@m".into());
        app.input = "/un".into();
        complete_command(&mut app);
        assert_eq!(app.input, "/un");
        assert!(app.lines.last().unwrap().contains("/unsub"));
        assert!(app.lines.last().unwrap().contains("/unarchive"));

        // Non-command input is untouched.
        let mut app = App::new(addr, "@m".into());
        app.input = "plain text".into();
        complete_command(&mut app);
        assert_eq!(app.input, "plain text");
    }
}
