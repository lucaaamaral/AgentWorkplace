//! Broker core: session registry, method dispatch, delivery, watch streaming.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use protocol::methods as m;
use protocol::*;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};

use crate::store::{now_ms, Store, StoreError};
use crate::BrokerConfig;

const DELIVER_TIMEOUT: Duration = Duration::from_secs(30);
const DELETE_TOKEN_TTL: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
pub enum WatchFilter {
    #[default]
    Off,
    All,
    Channels(HashSet<String>),
}

#[derive(Default)]
pub struct SessionState {
    pub client_info: Option<ClientInfo>,
    pub principal: Option<String>,
    pub admin: bool,
    pub watch: WatchFilter,
}

pub struct Session {
    pub id: u64,
    pub out: mpsc::UnboundedSender<Message>,
    pub state: Mutex<SessionState>,
    next_req_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>,
}

impl Session {
    pub fn principal(&self) -> Option<String> {
        self.state.lock().unwrap().principal.clone()
    }

    pub fn is_admin(&self) -> bool {
        self.state.lock().unwrap().admin
    }

    fn send(&self, msg: Message) {
        let _ = self.out.send(msg);
    }

    /// Issue a broker-originated request on this session's connection and
    /// await the client's response.
    async fn call(&self, method: &str, params: impl serde::Serialize) -> Result<Value, RpcError> {
        let id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        self.send(Message::Request(request(id, method, params)));
        let out = tokio::time::timeout(DELIVER_TIMEOUT, rx).await;
        match out {
            Ok(Ok(res)) => res,
            _ => {
                self.pending.lock().unwrap().remove(&id);
                Err(RpcError { code: -32000, message: "delivery timeout".into(), data: None })
            }
        }
    }

    pub fn resolve_response(&self, resp: Response) {
        let key = match &resp.id {
            Id::Num(n) => *n,
            Id::Str(_) => return,
        };
        if let Some(tx) = self.pending.lock().unwrap().remove(&key) {
            let _ = tx.send(match resp.error {
                Some(e) => Err(e),
                None => Ok(resp.result.unwrap_or(Value::Null)),
            });
        }
    }
}

struct DeleteToken {
    channel_id: i64,
    channel_name: String,
    session_id: u64,
    expires: Instant,
}

pub struct Inner {
    pub cfg: BrokerConfig,
    pub store: Store,
    started: Instant,
    sessions: Mutex<HashMap<u64, Arc<Session>>>,
    next_session_id: AtomicU64,
    delete_tokens: Mutex<HashMap<String, DeleteToken>>,
    token_counter: AtomicU64,
    shutdown_tx: watch::Sender<bool>,
}

#[derive(Clone)]
pub struct Broker(pub(crate) Arc<Inner>);

impl Broker {
    pub fn new(cfg: BrokerConfig) -> anyhow::Result<Self> {
        let store = match &cfg.db_path {
            Some(p) => Store::open(p)?,
            None => Store::open_in_memory()?,
        };
        let (shutdown_tx, _) = watch::channel(false);
        let broker = Broker(Arc::new(Inner {
            cfg,
            store,
            started: Instant::now(),
            sessions: Mutex::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
            delete_tokens: Mutex::new(HashMap::new()),
            token_counter: AtomicU64::new(1),
            shutdown_tx,
        }));
        broker.spawn_grace_window();
        Ok(broker)
    }

    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.0.shutdown_tx.subscribe()
    }

    pub fn store(&self) -> &Store {
        &self.0.store
    }

    /// Restart re-evaluation: held deliveries whose recipients have not
    /// re-attached when the grace window closes fail per-recipient.
    fn spawn_grace_window(&self) {
        let broker = self.clone();
        let grace = broker.0.cfg.grace_window;
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            for (record_id, recipient) in broker.0.store.held_all() {
                if broker.session_of(&recipient).is_none() {
                    broker.ack_update(record_id, &recipient, AckState::Failed, Some("disconnected"));
                }
            }
        });
    }

    // -- session registry ---------------------------------------------------

    pub fn attach(&self, out: mpsc::UnboundedSender<Message>) -> Arc<Session> {
        let id = self.0.next_session_id.fetch_add(1, Ordering::Relaxed);
        let session = Arc::new(Session {
            id,
            out,
            state: Mutex::new(SessionState::default()),
            next_req_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
        });
        self.0.sessions.lock().unwrap().insert(id, session.clone());
        session
    }

    pub fn detach(&self, session: &Arc<Session>) {
        self.0.sessions.lock().unwrap().remove(&session.id);
        if let Some(principal) = session.principal() {
            self.system_record(&SystemEvent::Disconnected { principal: principal.clone() }, None);
            // No store-and-forward across sessions: held deliveries for a
            // departed recipient fail.
            for record_id in self.0.store.held_for(&principal) {
                self.ack_update(record_id, &principal, AckState::Failed, Some("disconnected"));
            }
        }
    }

    fn sessions(&self) -> Vec<Arc<Session>> {
        self.0.sessions.lock().unwrap().values().cloned().collect()
    }

    fn session_of(&self, principal: &str) -> Option<Arc<Session>> {
        self.sessions().into_iter().find(|s| s.principal().as_deref() == Some(principal))
    }

    fn principal_active(&self, principal: &str) -> bool {
        self.session_of(principal).is_some()
    }

    // -- log + watch --------------------------------------------------------

    fn system_record(&self, event: &SystemEvent, channel: Option<(i64, &str)>) {
        let record = self.0.store.append_system(event, channel.map(|(id, _)| id));
        self.broadcast(WatchEvent::Record(record), channel.map(|(_, n)| vec![n.to_string()]), false);
    }

    /// scope: Some(channels) for channel-scoped events, None for global
    /// (global events and DMs reach only unfiltered watchers).
    fn broadcast(&self, event: WatchEvent, scope: Option<Vec<String>>, is_dm: bool) {
        let notif = Message::Notification(notification(m::WATCH_EVENT, &event));
        for s in self.sessions() {
            let wants = {
                let st = s.state.lock().unwrap();
                match &st.watch {
                    WatchFilter::Off => false,
                    WatchFilter::All => true,
                    WatchFilter::Channels(set) => {
                        !is_dm
                            && scope
                                .as_ref()
                                .is_some_and(|chs| chs.iter().any(|c| set.contains(c)))
                    }
                }
            };
            if wants {
                s.send(notif.clone());
            }
        }
    }

    fn ack_update(&self, record_id: u64, recipient: &str, state: AckState, reason: Option<&str>) {
        // Stale stragglers (a late `relayed` after `processed`) record their
        // timestamp but neither change state nor produce a transition.
        if let Some(ts) = self.0.store.ack_set(record_id, recipient, state, reason) {
            self.broadcast_ack(record_id, recipient, state, ts, reason);
        }
    }

    fn broadcast_ack(&self, record_id: u64, recipient: &str, state: AckState, ts: u64, reason: Option<&str>) {
        let scope = self
            .0
            .store
            .envelope_of(record_id)
            .map(|e| e.recipients.channels.clone());
        let is_dm = scope.as_ref().is_some_and(|c| c.is_empty());
        self.broadcast(
            WatchEvent::Ack(AckTransition {
                kind: "ack".into(),
                message_id: record_id,
                recipient: recipient.to_string(),
                state,
                timestamp: ts,
                reason: reason.map(String::from),
            }),
            scope,
            is_dm,
        );
    }

    // -- delivery -----------------------------------------------------------

    /// Deliver one envelope to one recipient's session. `tracked` deliveries
    /// update ack state; untracked ones are the admin observability tap.
    fn spawn_deliver(&self, recipient: String, envelope: Envelope, tracked: bool) {
        let broker = self.clone();
        tokio::spawn(async move {
            let Some(session) = broker.session_of(&recipient) else {
                if tracked {
                    broker.ack_update(envelope.message_id, &recipient, AckState::Failed, Some("disconnected"));
                }
                return;
            };
            let params = DeliverParams { recipient: recipient.clone(), envelope: envelope.clone() };
            let result = session.call(m::MESSAGE_DELIVER, &params).await;
            if !tracked {
                return;
            }
            match result {
                Ok(_) => broker.ack_update(envelope.message_id, &recipient, AckState::Relayed, None),
                Err(e) => broker.ack_update(
                    envelope.message_id,
                    &recipient,
                    AckState::Failed,
                    Some(&e.message),
                ),
            }
        });
    }

    fn drain_held(&self, principal: &str) {
        for record_id in self.0.store.held_for(principal) {
            if let Some(envelope) = self.0.store.envelope_of(record_id) {
                self.spawn_deliver(principal.to_string(), envelope, true);
            }
        }
    }

    // -- dispatch -----------------------------------------------------------

    pub async fn handle_request(&self, session: &Arc<Session>, req: Request) -> Response {
        let id = req.id.clone();
        let params = req.params.unwrap_or(Value::Null);
        let result = self.dispatch(session, &req.method, params).await;
        match result {
            Ok(v) => ok_response(id, v),
            Err(e) => err_response(id, e),
        }
    }

    pub fn handle_notification(&self, session: &Arc<Session>, notif: Notification) {
        if notif.method == m::MESSAGE_PROCESSED {
            let Ok(p) = serde_json::from_value::<ProcessedParams>(notif.params.unwrap_or(Value::Null)) else {
                return;
            };
            // Only the session bound to the recipient may report processing.
            if session.principal().as_deref() == Some(p.recipient.as_str()) {
                self.ack_update(p.message_id, &p.recipient, AckState::Processed, None);
            }
        }
    }

    async fn dispatch(&self, session: &Arc<Session>, method: &str, params: Value) -> Result<Value, RpcError> {
        if *self.0.shutdown_tx.borrow() {
            return Err(ErrorCode::ShuttingDown.to_error("broker is shutting down"));
        }
        if method != m::SESSION_HELLO && session.state.lock().unwrap().client_info.is_none() {
            return Err(RpcError {
                code: -32600,
                message: "session/hello must be the first call".into(),
                data: None,
            });
        }
        match method {
            m::SESSION_HELLO => self.hello(session, parse(params)?),
            m::PRINCIPAL_REGISTER => self.register(session, parse(params)?, false),
            m::ADMIN_REGISTER => self.register(session, parse(params)?, true),
            m::PRINCIPAL_DEREGISTER => self.deregister(session),
            m::MESSAGE_SEND => self.send_message(session, parse(params)?),
            m::CHANNEL_SUBSCRIBE => self.subscribe(session, parse(params)?),
            m::CHANNEL_UNSUBSCRIBE => self.unsubscribe(session, parse(params)?),
            m::CHANNEL_CREATE => self.create_channel(session, parse(params)?),
            m::HISTORY_GET => self.history(session, parse(params)?),
            m::DIRECTORY_WHO => self.who(),
            m::ADMIN_SUBSCRIBE => self.admin_subscription(session, parse(params)?, true),
            m::ADMIN_UNSUBSCRIBE => self.admin_subscription(session, parse(params)?, false),
            m::CHANNEL_RENAME => self.rename(session, parse(params)?),
            m::CHANNEL_ARCHIVE => self.archive(session, parse(params)?, true),
            m::CHANNEL_UNARCHIVE => self.archive(session, parse(params)?, false),
            m::CHANNEL_DELETE => self.delete_request(session, parse(params)?),
            m::CHANNEL_DELETE_CONFIRM => self.delete_confirm(session, parse(params)?),
            m::MESSAGE_STATUS => self.message_status(session, parse(params)?),
            m::DAEMON_STATUS => self.daemon_status(session),
            m::DAEMON_SHUTDOWN => self.daemon_shutdown(session),
            m::WATCH_START => self.watch_start(session, parse(params)?),
            m::WATCH_STOP => {
                session.state.lock().unwrap().watch = WatchFilter::Off;
                Ok(Value::Null)
            }
            other => Err(RpcError {
                code: -32601,
                message: format!("method not found: {other}"),
                data: None,
            }),
        }
    }

    fn hello(&self, session: &Arc<Session>, p: HelloParams) -> Result<Value, RpcError> {
        session.state.lock().unwrap().client_info = Some(p.client_info);
        to_value(HelloResult {
            broker_version: self.0.cfg.version.clone(),
            session_id: session.id,
        })
    }

    fn register(&self, session: &Arc<Session>, p: RegisterParams, admin: bool) -> Result<Value, RpcError> {
        if !valid_principal_name(&p.name) {
            return Err(ErrorCode::InvalidName.to_error(format!(
                "principal names are '@' + lowercase alphanumeric/'-': {}",
                p.name
            )));
        }
        if session.principal().is_some() {
            return Err(ErrorCode::AlreadyRegistered.to_error("session already bound to a principal"));
        }
        if self.principal_active(&p.name) {
            self.system_record(
                &SystemEvent::RegistrationDenied { name: p.name.clone(), reason: "actively claimed".into() },
                None,
            );
            return Err(ErrorCode::NameTaken.to_error(format!("{} is actively claimed", p.name)));
        }
        self.0.store.ensure_principal(&p.name);
        {
            let mut st = session.state.lock().unwrap();
            st.principal = Some(p.name.clone());
            st.admin = admin;
        }
        self.system_record(&SystemEvent::Registered { principal: p.name.clone(), admin }, None);
        self.drain_held(&p.name);
        to_value(RegisterResult { principal: p.name })
    }

    fn deregister(&self, session: &Arc<Session>) -> Result<Value, RpcError> {
        let Some(principal) = session.principal() else {
            return Err(ErrorCode::NotRegistered.to_error("session has no principal"));
        };
        {
            let mut st = session.state.lock().unwrap();
            st.principal = None;
            st.admin = false;
        }
        self.system_record(&SystemEvent::Deregistered { principal }, None);
        Ok(Value::Null)
    }

    fn send_message(&self, session: &Arc<Session>, p: SendParams) -> Result<Value, RpcError> {
        let Some(sender) = session.principal() else {
            return Err(ErrorCode::NotRegistered.to_error("register before sending"));
        };
        if p.channels.is_empty() && p.principals.is_empty() {
            return Err(RpcError {
                code: -32602,
                message: "send requires channels, principals, or both".into(),
                data: None,
            });
        }
        // Unknown names are errors: nothing stored.
        let mut channel_ids = Vec::new();
        for name in &p.channels {
            match self.0.store.channel_by_name(name) {
                Some(c) if !c.archived => channel_ids.push(c.id),
                Some(_) => {
                    return Err(ErrorCode::UnknownName.to_error(format!("{name} is archived")));
                }
                None => return Err(ErrorCode::UnknownName.to_error(format!("no such channel: {name}"))),
            }
        }
        for name in &p.principals {
            if self.0.store.principal_id(name).is_none() {
                return Err(ErrorCode::UnknownName.to_error(format!("no such principal: {name}")));
            }
        }
        if let Some(thread) = p.thread_id {
            if !self.0.store.message_exists(thread) {
                return Err(ErrorCode::UnknownName.to_error(format!("no such thread: {thread}")));
            }
        }

        // Resolve the audience (intersection semantics, ADR-0009).
        let mut audience: Vec<String> = Vec::new();
        let mut empty_audience = None;
        if !p.channels.is_empty() && !p.principals.is_empty() {
            for principal in &p.principals {
                let pid = self.0.store.principal_id(principal).unwrap();
                if channel_ids.iter().any(|cid| self.0.store.is_subscribed(pid, *cid)) {
                    audience.push(principal.clone());
                }
            }
            if audience.is_empty() {
                empty_audience = Some(EmptyAudience::EmptyIntersection);
            }
        } else if !p.channels.is_empty() {
            let mut set = HashSet::new();
            for cid in &channel_ids {
                set.extend(self.0.store.subscribers_of(*cid));
            }
            audience = set.into_iter().collect();
            audience.sort();
            if audience.is_empty() {
                empty_audience = Some(EmptyAudience::NoSubscribers);
            }
        } else {
            audience = p.principals.clone();
            audience.dedup();
        }
        audience.retain(|r| r != &sender);
        if !audience.is_empty() {
            empty_audience = None;
        }

        // Size limit: truncate, never reject.
        let limit = self.0.cfg.message_size_limit;
        let (body, truncated) = if p.body.len() > limit {
            let mut cut = limit;
            while !p.body.is_char_boundary(cut) {
                cut -= 1;
            }
            (p.body[..cut].to_string(), true)
        } else {
            (p.body.clone(), false)
        };

        let recipients = Recipients { channels: p.channels.clone(), principals: p.principals.clone() };
        let envelope = self.0.store.append_message(
            &sender,
            &recipients,
            &body,
            truncated,
            p.thread_id,
            &channel_ids,
        );

        let mut report = DeliveryReport { empty_audience, ..Default::default() };
        for recipient in &audience {
            if self.principal_active(recipient) {
                self.0.store.ack_init(envelope.message_id, recipient, AckState::Held, None);
                report.delivered.push(recipient.clone());
                self.spawn_deliver(recipient.clone(), envelope.clone(), true);
            } else {
                self.0.store.ack_init(envelope.message_id, recipient, AckState::Failed, Some("disconnected"));
                report
                    .failed
                    .push(FailedDelivery { principal: recipient.clone(), reason: "disconnected".into() });
                self.broadcast_ack(envelope.message_id, recipient, AckState::Failed, now_ms(), Some("disconnected"));
            }
        }

        // Admin observability tap: every message, never counted (rpc-surface).
        for s in self.sessions() {
            let (is_admin, principal) = {
                let st = s.state.lock().unwrap();
                (st.admin, st.principal.clone())
            };
            if is_admin {
                if let Some(admin_principal) = principal {
                    if admin_principal != sender && !audience.contains(&admin_principal) {
                        self.spawn_deliver(admin_principal, envelope.clone(), false);
                    }
                }
            }
        }

        let is_dm = envelope.recipients.channels.is_empty();
        self.broadcast(
            WatchEvent::Record(Record::Message { envelope: envelope.clone() }),
            Some(envelope.recipients.channels.clone()),
            is_dm,
        );

        to_value(SendResult { message_id: envelope.message_id, thread_id: envelope.thread_id, delivery: report })
    }

    fn subscribe(&self, session: &Arc<Session>, p: ChannelParams) -> Result<Value, RpcError> {
        let Some(principal) = session.principal() else {
            return Err(ErrorCode::NotRegistered.to_error("register before subscribing"));
        };
        let ch = self.live_channel(&p.channel)?;
        let pid = self.0.store.principal_id(&principal).unwrap();
        if self.0.store.subscribe(pid, ch.id) {
            self.system_record(
                &SystemEvent::Subscribed { principal, channel: ch.name.clone(), by_admin: false },
                Some((ch.id, &ch.name)),
            );
        }
        Ok(Value::Null)
    }

    fn unsubscribe(&self, session: &Arc<Session>, p: ChannelParams) -> Result<Value, RpcError> {
        let Some(principal) = session.principal() else {
            return Err(ErrorCode::NotRegistered.to_error("register before unsubscribing"));
        };
        let ch = self.live_channel(&p.channel)?;
        let pid = self.0.store.principal_id(&principal).unwrap();
        if self.0.store.unsubscribe(pid, ch.id) {
            self.system_record(
                &SystemEvent::Unsubscribed { principal, channel: ch.name.clone(), by_admin: false },
                Some((ch.id, &ch.name)),
            );
        }
        Ok(Value::Null)
    }

    fn create_channel(&self, session: &Arc<Session>, p: RegisterParams) -> Result<Value, RpcError> {
        let Some(principal) = session.principal() else {
            return Err(ErrorCode::NotRegistered.to_error("register before creating channels"));
        };
        if !valid_channel_name(&p.name) {
            return Err(ErrorCode::InvalidName.to_error(format!(
                "channel names are '#' + lowercase alphanumeric/'-': {}",
                p.name
            )));
        }
        match self.0.store.create_channel(&p.name) {
            Ok(id) => {
                self.system_record(
                    &SystemEvent::ChannelCreated { channel: p.name.clone(), by: principal },
                    Some((id, &p.name)),
                );
                to_value(ChannelCreateResult { channel: p.name })
            }
            Err(StoreError::NameTaken) => {
                Err(ErrorCode::NameTaken.to_error(format!("{} already exists (live or archived)", p.name)))
            }
            Err(e) => Err(internal(e)),
        }
    }

    fn history(&self, session: &Arc<Session>, p: HistoryParams) -> Result<Value, RpcError> {
        let admin = session.is_admin();
        let limit = p.limit.clamp(1, 1000);
        let records = match &p.scope {
            HistoryScope::Channel { channel } => {
                let Some(ch) = self.0.store.channel_by_name(channel) else {
                    return Err(ErrorCode::UnknownName.to_error(format!("no such channel: {channel}")));
                };
                if ch.archived && !admin {
                    return Err(ErrorCode::ScopeDenied.to_error("archived channel history is admin-only"));
                }
                self.0.store.history_channel(ch.id, p.before_message_id, limit)
            }
            HistoryScope::DmWith { dm_with } => {
                let Some(me) = session.principal() else {
                    return Err(ErrorCode::NotRegistered.to_error("register before reading DM history"));
                };
                if self.0.store.principal_id(dm_with).is_none() {
                    return Err(ErrorCode::UnknownName.to_error(format!("no such principal: {dm_with}")));
                }
                self.0.store.history_dm(&me, dm_with, p.before_message_id, limit)
            }
            HistoryScope::DmBetween { dm_between } => {
                if !admin {
                    return Err(ErrorCode::ScopeDenied.to_error("dm_between is admin-only"));
                }
                for principal in dm_between {
                    if self.0.store.principal_id(principal).is_none() {
                        return Err(ErrorCode::UnknownName.to_error(format!("no such principal: {principal}")));
                    }
                }
                self.0.store.history_dm(&dm_between[0], &dm_between[1], p.before_message_id, limit)
            }
        };
        let next_cursor = if records.len() as u32 == limit { records.first().map(|r| r.id()) } else { None };
        to_value(HistoryResult { records, next_cursor })
    }

    fn who(&self) -> Result<Value, RpcError> {
        let channels = self
            .0
            .store
            .live_channels()
            .into_iter()
            .map(|c| ChannelDirectoryEntry {
                subscribers: self.0.store.subscribers_of(c.id),
                channel: c.name,
            })
            .collect();
        let principals = self
            .0
            .store
            .all_principals()
            .into_iter()
            .map(|p| PrincipalDirectoryEntry { active: self.principal_active(&p), principal: p })
            .collect();
        to_value(WhoResult { channels, principals })
    }

    fn require_admin(&self, session: &Arc<Session>) -> Result<String, RpcError> {
        let st = session.state.lock().unwrap();
        if st.admin {
            Ok(st.principal.clone().expect("admin sessions are registered"))
        } else {
            Err(ErrorCode::NotAdmin.to_error("admin verb on a non-admin session"))
        }
    }

    fn admin_subscription(&self, session: &Arc<Session>, p: AdminSubscriptionParams, subscribe: bool) -> Result<Value, RpcError> {
        self.require_admin(session)?;
        let Some(pid) = self.0.store.principal_id(&p.principal) else {
            return Err(ErrorCode::UnknownName.to_error(format!("no such principal: {}", p.principal)));
        };
        let ch = self.live_channel(&p.channel)?;
        let changed = if subscribe {
            self.0.store.subscribe(pid, ch.id)
        } else {
            self.0.store.unsubscribe(pid, ch.id)
        };
        if changed {
            let event = if subscribe {
                SystemEvent::Subscribed { principal: p.principal, channel: ch.name.clone(), by_admin: true }
            } else {
                SystemEvent::Unsubscribed { principal: p.principal, channel: ch.name.clone(), by_admin: true }
            };
            self.system_record(&event, Some((ch.id, &ch.name)));
        }
        Ok(Value::Null)
    }

    fn rename(&self, session: &Arc<Session>, p: ChannelRenameParams) -> Result<Value, RpcError> {
        let by = self.require_admin(session)?;
        // Rename works on archived channels too (ADR-0018 escape hatch).
        let Some(ch) = self.0.store.channel_by_name(&p.channel) else {
            return Err(ErrorCode::UnknownName.to_error(format!("no such channel: {}", p.channel)));
        };
        if !valid_channel_name(&p.new_name) {
            return Err(ErrorCode::InvalidName.to_error(format!("invalid channel name: {}", p.new_name)));
        }
        match self.0.store.rename_channel(ch.id, &p.new_name) {
            Ok(()) => {
                self.system_record(
                    &SystemEvent::ChannelRenamed { old_name: p.channel, new_name: p.new_name.clone(), by },
                    Some((ch.id, &p.new_name)),
                );
                Ok(Value::Null)
            }
            Err(StoreError::NameTaken) => {
                Err(ErrorCode::NameTaken.to_error(format!("{} already exists", p.new_name)))
            }
            Err(e) => Err(internal(e)),
        }
    }

    fn archive(&self, session: &Arc<Session>, p: ChannelParams, archive: bool) -> Result<Value, RpcError> {
        let by = self.require_admin(session)?;
        let Some(ch) = self.0.store.channel_by_name(&p.channel) else {
            return Err(ErrorCode::UnknownName.to_error(format!("no such channel: {}", p.channel)));
        };
        if ch.archived == archive {
            return Ok(Value::Null); // idempotent
        }
        self.0.store.set_archived(ch.id, archive).map_err(internal)?;
        if archive {
            for principal in self.0.store.clear_subscriptions(ch.id).map_err(internal)? {
                self.system_record(
                    &SystemEvent::Unsubscribed { principal, channel: ch.name.clone(), by_admin: true },
                    Some((ch.id, &ch.name)),
                );
            }
            self.system_record(
                &SystemEvent::ChannelArchived { channel: ch.name.clone(), by },
                Some((ch.id, &ch.name)),
            );
        } else {
            self.system_record(
                &SystemEvent::ChannelUnarchived { channel: ch.name.clone(), by },
                Some((ch.id, &ch.name)),
            );
        }
        Ok(Value::Null)
    }

    fn delete_request(&self, session: &Arc<Session>, p: ChannelParams) -> Result<Value, RpcError> {
        self.require_admin(session)?;
        let Some(ch) = self.0.store.channel_by_name(&p.channel) else {
            return Err(ErrorCode::UnknownName.to_error(format!("no such channel: {}", p.channel)));
        };
        let token = format!(
            "del-{}-{}-{}",
            session.id,
            self.0.token_counter.fetch_add(1, Ordering::Relaxed),
            now_ms()
        );
        self.0.delete_tokens.lock().unwrap().insert(
            token.clone(),
            DeleteToken {
                channel_id: ch.id,
                channel_name: ch.name,
                session_id: session.id,
                expires: Instant::now() + DELETE_TOKEN_TTL,
            },
        );
        to_value(ChannelDeleteResult { confirmation_token: token })
    }

    fn delete_confirm(&self, session: &Arc<Session>, p: ChannelDeleteConfirmParams) -> Result<Value, RpcError> {
        let by = self.require_admin(session)?;
        let token = self.0.delete_tokens.lock().unwrap().remove(&p.confirmation_token);
        let Some(token) = token else {
            return Err(ErrorCode::BadConfirmation.to_error("unknown or already-used confirmation token"));
        };
        if token.session_id != session.id
            || token.expires < Instant::now()
            || self.0.store.channel_by_name(&token.channel_name).map(|c| c.id) != Some(token.channel_id)
            || p.channel != token.channel_name
        {
            return Err(ErrorCode::BadConfirmation.to_error("confirmation token expired or mismatched"));
        }
        let record_count = self.0.store.delete_channel(token.channel_id).map_err(internal)?;
        self.system_record(
            &SystemEvent::ChannelDeleted {
                channel_id: token.channel_id as u64,
                name: token.channel_name,
                record_count,
                by,
            },
            None,
        );
        Ok(Value::Null)
    }

    fn message_status(&self, session: &Arc<Session>, p: MessageStatusParams) -> Result<Value, RpcError> {
        self.require_admin(session)?;
        if !self.0.store.message_exists(p.message_id) {
            return Err(ErrorCode::UnknownName.to_error(format!("no such message: {}", p.message_id)));
        }
        to_value(MessageStatusResult { acks: self.0.store.acks_for(p.message_id) })
    }

    fn daemon_status(&self, session: &Arc<Session>) -> Result<Value, RpcError> {
        self.require_admin(session)?;
        let sessions = self
            .sessions()
            .into_iter()
            .filter_map(|s| {
                let st = s.state.lock().unwrap();
                st.client_info.clone().map(|ci| SessionInfo {
                    session_id: s.id,
                    principal: st.principal.clone(),
                    admin: st.admin,
                    client_info: ci,
                })
            })
            .collect();
        to_value(DaemonStatusResult {
            broker_version: self.0.cfg.version.clone(),
            uptime_seconds: self.0.started.elapsed().as_secs(),
            sessions,
            channel_count: self.0.store.channel_count(),
            principal_count: self.0.store.principal_count(),
        })
    }

    fn daemon_shutdown(&self, session: &Arc<Session>) -> Result<Value, RpcError> {
        self.require_admin(session)?;
        let tx = self.0.shutdown_tx.clone();
        tokio::spawn(async move {
            // Let the response flush before the listener dies.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx.send(true);
        });
        Ok(Value::Null)
    }

    fn watch_start(&self, session: &Arc<Session>, p: WatchStartParams) -> Result<Value, RpcError> {
        let filter = match p.channels {
            None => WatchFilter::All,
            Some(names) => {
                for name in &names {
                    if self.0.store.channel_by_name(name).is_none() {
                        return Err(ErrorCode::UnknownName.to_error(format!("no such channel: {name}")));
                    }
                }
                WatchFilter::Channels(names.into_iter().collect())
            }
        };
        session.state.lock().unwrap().watch = filter;
        Ok(Value::Null)
    }

    fn live_channel(&self, name: &str) -> Result<crate::store::ChannelRow, RpcError> {
        match self.0.store.channel_by_name(name) {
            Some(c) if !c.archived => Ok(c),
            Some(_) => Err(ErrorCode::UnknownName.to_error(format!("{name} is archived"))),
            None => Err(ErrorCode::UnknownName.to_error(format!("no such channel: {name}"))),
        }
    }
}

fn parse<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, RpcError> {
    serde_json::from_value(params).map_err(|e| RpcError {
        code: -32602,
        message: format!("invalid params: {e}"),
        data: None,
    })
}

fn to_value(v: impl serde::Serialize) -> Result<Value, RpcError> {
    serde_json::to_value(v).map_err(internal)
}

fn internal(e: impl std::fmt::Display) -> RpcError {
    RpcError { code: -32603, message: format!("internal error: {e}"), data: None }
}
