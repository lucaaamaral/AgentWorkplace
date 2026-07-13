//! Broker core: session registry, method dispatch, delivery, watch streaming.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use protocol::methods as m;
use protocol::*;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};

use crate::BrokerConfig;
use crate::store::{OverrideMode, Store, StoreError, SubOutcome, now_ms};

mod rpc;

const DELIVER_TIMEOUT: Duration = Duration::from_secs(30);
const DELETE_TOKEN_TTL: Duration = Duration::from_secs(30);
/// Backoff cap for held Codex deliveries whose app-server is unreachable
/// while the recipient's session is still present (CX-5).
const CODEX_RETRY_MAX: Duration = Duration::from_secs(30);

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
    /// Present when this session registered as a Codex agent: deliveries are
    /// injected via the app-server (attach engine), not this connection.
    pub codex: Option<CodexCoordinates>,
}

pub struct Session {
    pub id: u64,
    pub out: mpsc::UnboundedSender<Message>,
    pub state: Mutex<SessionState>,
    next_req_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>,
    /// Messages queued for the writer but not yet written; the writer task
    /// decrements. Bounds per-session outbound memory (slow-reader defense).
    pub(crate) queued: Arc<std::sync::atomic::AtomicUsize>,
    /// Queue bound (BrokerConfig::max_out_queue, captured at attach).
    max_out_queue: usize,
    overflow: std::sync::atomic::AtomicBool,
    /// Fired once when the outbound queue overflows, so the connection loop
    /// wakes even while blocked on an idle reader.
    pub(crate) overflow_notify: tokio::sync::Notify,
}

impl Session {
    pub fn principal(&self) -> Option<String> {
        self.state.lock().unwrap().principal.clone()
    }

    pub fn is_admin(&self) -> bool {
        self.state.lock().unwrap().admin
    }

    /// True once the outbound queue overflowed: the connection loop tears
    /// the session down on its next iteration.
    pub fn overflowed(&self) -> bool {
        self.overflow.load(Ordering::Relaxed)
    }

    pub(crate) fn send(&self, msg: Message) {
        if self.overflow.load(Ordering::Relaxed) {
            return;
        }
        // Reserve the slot atomically: concurrent senders must not overshoot
        // the bound.
        let limit = self.max_out_queue;
        let reserved = self
            .queued
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |q| {
                (q < limit).then_some(q + 1)
            })
            .is_ok();
        if !reserved {
            if !self.overflow.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    session = self.id,
                    "outbound queue overflow ({limit} unread messages); dropping session"
                );
                // notify_one stores a permit, so the connection loop wakes
                // even if it enters its select after this fires.
                self.overflow_notify.notify_one();
            }
            return;
        }
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
                Err(RpcError {
                    code: -32000,
                    message: "delivery timeout".into(),
                    data: None,
                })
            }
        }
    }

    pub fn resolve_response(&self, resp: Response) {
        let key = match &resp.id {
            Id::Num(n) => *n,
            Id::Str(_) | Id::Null => return,
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
    /// Serializes principal claims: register is check-then-bind, and two
    /// sessions racing for one name must not both win.
    claim_lock: Mutex<()>,
    /// In-process Codex attach clients, one per app-server endpoint
    /// (rpc-surface: in-process adapters implement the deliver contract
    /// natively), with per-endpoint dial backoff so an outage with many held
    /// messages does not hammer the endpoint with serialized failed dials.
    codex_attach: tokio::sync::Mutex<AttachCache>,
}

#[derive(Default)]
struct AttachCache {
    clients: HashMap<String, Arc<adapter_codex::CodexAttach>>,
    /// endpoint → (current backoff, do-not-dial-before deadline)
    dial_backoff: HashMap<String, (Duration, tokio::time::Instant)>,
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
            claim_lock: Mutex::new(()),
            codex_attach: tokio::sync::Mutex::new(AttachCache::default()),
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
            let held = broker.0.store.held_all().unwrap_or_else(|e| {
                tracing::error!("grace-window sweep failed: {e}");
                Vec::new()
            });
            for (record_id, recipient) in held {
                if broker.session_of(&recipient).is_none() {
                    broker.ack_update(
                        record_id,
                        &recipient,
                        AckState::Failed,
                        Some("disconnected"),
                    );
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
            queued: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            max_out_queue: self.0.cfg.max_out_queue,
            overflow: std::sync::atomic::AtomicBool::new(false),
            overflow_notify: tokio::sync::Notify::new(),
        });
        self.0.sessions.lock().unwrap().insert(id, session.clone());
        session
    }

    pub fn detach(&self, session: &Arc<Session>) {
        self.0.sessions.lock().unwrap().remove(&session.id);
        if let Some(principal) = session.principal() {
            self.system_record(
                &SystemEvent::Disconnected {
                    principal: principal.clone(),
                },
                None,
            );
            // No store-and-forward across sessions: held deliveries for a
            // departed recipient fail.
            let held = self.0.store.held_for(&principal).unwrap_or_else(|e| {
                tracing::error!("held sweep for {principal} failed: {e}");
                Vec::new()
            });
            for record_id in held {
                self.ack_update(
                    record_id,
                    &principal,
                    AckState::Failed,
                    Some("disconnected"),
                );
            }
        }
    }

    fn sessions(&self) -> Vec<Arc<Session>> {
        self.0.sessions.lock().unwrap().values().cloned().collect()
    }

    fn session_of(&self, principal: &str) -> Option<Arc<Session>> {
        self.sessions()
            .into_iter()
            .find(|s| s.principal().as_deref() == Some(principal))
    }

    fn principal_active(&self, principal: &str) -> bool {
        self.session_of(principal).is_some()
    }

    // -- log + watch --------------------------------------------------------

    fn system_record(&self, event: &SystemEvent, channel: Option<(i64, &str)>) {
        match self.0.store.append_system(event, channel.map(|(id, _)| id)) {
            Ok(record) => self.broadcast(
                WatchEvent::Record(record),
                channel.map(|(_, n)| vec![n.to_string()]),
                false,
            ),
            Err(e) => tracing::error!("system record append failed: {e}"),
        }
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
        match self.0.store.ack_set(record_id, recipient, state, reason) {
            Ok(Some(ts)) => self.broadcast_ack(record_id, recipient, state, ts, reason),
            Ok(None) => {}
            Err(e) => tracing::error!("ack update for message {record_id} failed: {e}"),
        }
    }

    fn broadcast_ack(
        &self,
        record_id: u64,
        recipient: &str,
        state: AckState,
        ts: u64,
        reason: Option<&str>,
    ) {
        let scope = self
            .0
            .store
            .envelope_of(record_id)
            .ok()
            .flatten()
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
    /// Codex-registered recipients are delivered via the attach engine
    /// (turn/start on their thread); everyone else gets a message/deliver
    /// request on their own connection.
    fn spawn_deliver(&self, recipient: String, envelope: Envelope, tracked: bool) {
        let broker = self.clone();
        tokio::spawn(async move {
            let Some(session) = broker.session_of(&recipient) else {
                if tracked {
                    broker.ack_update(
                        envelope.message_id,
                        &recipient,
                        AckState::Failed,
                        Some("disconnected"),
                    );
                }
                return;
            };
            let codex = session.state.lock().unwrap().codex.clone();
            if let Some(coords) = codex {
                broker
                    .deliver_via_codex(&recipient, &envelope, &coords, tracked, session.id)
                    .await;
                return;
            }
            let params = DeliverParams {
                recipient: recipient.clone(),
                envelope: envelope.clone(),
            };
            let result = session.call(m::MESSAGE_DELIVER, &params).await;
            if !tracked {
                return;
            }
            match result {
                Ok(_) => {
                    broker.ack_update(envelope.message_id, &recipient, AckState::Relayed, None)
                }
                Err(e) => broker.ack_update(
                    envelope.message_id,
                    &recipient,
                    AckState::Failed,
                    Some(&e.message),
                ),
            }
        });
    }

    /// Fetch (or dial) the cached attach client for one app-server endpoint,
    /// honoring the per-endpoint dial backoff (outage amplification guard:
    /// many held messages must not translate into continuous failed dials).
    async fn codex_attach_client(
        &self,
        endpoint: &str,
    ) -> anyhow::Result<Arc<adapter_codex::CodexAttach>> {
        let mut cache = self.0.codex_attach.lock().await;
        if let Some(a) = cache.clients.get(endpoint) {
            return Ok(a.clone());
        }
        if let Some((_, not_before)) = cache.dial_backoff.get(endpoint)
            && tokio::time::Instant::now() < *not_before
        {
            anyhow::bail!("endpoint in dial backoff");
        }
        let token = match &self.0.cfg.codex_token_file {
            Some(path) => Some(
                std::fs::read_to_string(path)
                    .map(|t| t.trim().to_string())
                    .map_err(|e| anyhow::anyhow!("cannot read codex token file: {e}"))?,
            ),
            None => None,
        };
        match adapter_codex::CodexAttach::connect(endpoint, token.as_deref()).await {
            Ok(a) => {
                let a = Arc::new(a);
                cache.clients.insert(endpoint.to_string(), a.clone());
                cache.dial_backoff.remove(endpoint);
                Ok(a)
            }
            Err(e) => {
                let next = cache
                    .dial_backoff
                    .get(endpoint)
                    .map(|(d, _)| (*d * 2).min(CODEX_RETRY_MAX))
                    .unwrap_or(Duration::from_secs(1));
                cache.dial_backoff.insert(
                    endpoint.to_string(),
                    (next, tokio::time::Instant::now() + next),
                );
                Err(e)
            }
        }
    }

    /// Inject a delivery into a Codex agent's (human-owned) thread. Blocks
    /// through turn completion: Processed collapses relayed+processed — the
    /// attach engine only observes the turn as a whole (findings.md).
    ///
    /// CX-5: while the recipient's session is still present, an unreachable
    /// app-server keeps the delivery `held` and retries with capped backoff
    /// (reconnect → resume → drain). Once the session is gone, the delivery
    /// fails — no store-and-forward. Retries are keyed to the *session* that
    /// registered these coordinates: if the name changes hands, this task
    /// exits silently and the new session's drain owns the record.
    async fn deliver_via_codex(
        &self,
        recipient: &str,
        envelope: &Envelope,
        coords: &CodexCoordinates,
        tracked: bool,
        session_id: u64,
    ) {
        let text = render_delivery(envelope);
        let mut retry = Duration::from_secs(1);
        loop {
            match self.session_of(recipient) {
                None => {
                    if tracked {
                        self.ack_update(
                            envelope.message_id,
                            recipient,
                            AckState::Failed,
                            Some("disconnected"),
                        );
                    }
                    return;
                }
                Some(s) if s.id != session_id => {
                    // Stale task: the principal was re-claimed by another
                    // session with (possibly) different coordinates.
                    return;
                }
                Some(_) => {}
            }
            let outcome = match self.codex_attach_client(&coords.app_server).await {
                Ok(attach) => attach.deliver(&coords.thread_id, &text).await,
                Err(e) => {
                    adapter_codex::Delivered::Unreachable(format!("app-server unreachable: {e}"))
                }
            };
            match outcome {
                adapter_codex::Delivered::Processed => {
                    if tracked {
                        self.ack_update(envelope.message_id, recipient, AckState::Relayed, None);
                        self.ack_update(envelope.message_id, recipient, AckState::Processed, None);
                    }
                    return;
                }
                adapter_codex::Delivered::Failed(reason) => {
                    if tracked {
                        self.ack_update(
                            envelope.message_id,
                            recipient,
                            AckState::Failed,
                            Some(&reason),
                        );
                    }
                    return;
                }
                adapter_codex::Delivered::Unreachable(reason) => {
                    // A dead attach client must not poison the cache.
                    self.0
                        .codex_attach
                        .lock()
                        .await
                        .clients
                        .remove(&coords.app_server);
                    if !tracked {
                        return;
                    }
                    tracing::warn!(
                        recipient,
                        app_server = coords.app_server,
                        "codex delivery held, app-server unreachable ({reason}); retrying in {retry:?}"
                    );
                    tokio::time::sleep(retry).await;
                    retry = (retry * 2).min(CODEX_RETRY_MAX);
                }
            }
        }
    }

    fn drain_held(&self, principal: &str) {
        let held = self.0.store.held_for(principal).unwrap_or_else(|e| {
            tracing::error!("held drain for {principal} failed: {e}");
            Vec::new()
        });
        for record_id in held {
            if let Ok(Some(envelope)) = self.0.store.envelope_of(record_id) {
                self.spawn_deliver(principal.to_string(), envelope, true);
            }
        }
    }
}
