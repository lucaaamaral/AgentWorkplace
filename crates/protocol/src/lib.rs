//! Wire protocol for the AgentWorkplace bus.
//!
//! JSON-RPC 2.0, newline-delimited, on every broker connection (ADR-0014,
//! ADR-0016). This crate defines the JSON-RPC framing types, the message
//! model types (envelope, records, acks), and the parameter/result shapes of
//! every method in the RPC surface (docs/architecture/rpc-surface.md).

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const DEFAULT_PORT: u16 = 9675;

// ---------------------------------------------------------------------------
// JSON-RPC 2.0 framing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Id {
    Num(u64),
    Str(String),
    /// JSON-RPC parse/invalid-request errors carry `"id": null`.
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    pub id: Id,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: String,
    pub id: Id,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone)]
pub enum Message {
    Request(Request),
    Notification(Notification),
    Response(Response),
}

impl Message {
    /// Classify one NDJSON line. JSON-RPC distinguishes the three shapes by
    /// the presence of `method` and `id`.
    pub fn parse(line: &str) -> Result<Message, serde_json::Error> {
        Message::from_value(serde_json::from_str(line)?)
    }

    /// Classify an already-parsed JSON value. Lets transports distinguish a
    /// syntax error (-32700) from valid JSON with an invalid shape (-32600).
    pub fn from_value(v: Value) -> Result<Message, serde_json::Error> {
        let has_method = v.get("method").is_some();
        let has_id = v.get("id").map(|i| !i.is_null()).unwrap_or(false);
        if has_method && has_id {
            Ok(Message::Request(serde_json::from_value(v)?))
        } else if has_method {
            Ok(Message::Notification(serde_json::from_value(v)?))
        } else {
            Ok(Message::Response(serde_json::from_value(v)?))
        }
    }

    pub fn to_line(&self) -> String {
        let s = match self {
            Message::Request(r) => serde_json::to_string(r),
            Message::Notification(n) => serde_json::to_string(n),
            Message::Response(r) => serde_json::to_string(r),
        };
        s.expect("protocol types always serialize")
    }
}

pub fn request(id: u64, method: &str, params: impl Serialize) -> Request {
    Request {
        jsonrpc: "2.0".into(),
        id: Id::Num(id),
        method: method.into(),
        params: Some(serde_json::to_value(params).expect("params serialize")),
    }
}

pub fn notification(method: &str, params: impl Serialize) -> Notification {
    Notification {
        jsonrpc: "2.0".into(),
        method: method.into(),
        params: Some(serde_json::to_value(params).expect("params serialize")),
    }
}

pub fn ok_response(id: Id, result: impl Serialize) -> Response {
    Response {
        jsonrpc: "2.0".into(),
        id,
        result: Some(serde_json::to_value(result).expect("result serialize")),
        error: None,
    }
}

pub fn err_response(id: Id, error: RpcError) -> Response {
    Response {
        jsonrpc: "2.0".into(),
        id,
        result: None,
        error: Some(error),
    }
}

// ---------------------------------------------------------------------------
// Errors (rpc-surface.md, stable symbolic names in error.data.code)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    UnknownName,
    NameTaken,
    NotRegistered,
    AlreadyRegistered,
    InvalidName,
    NotAdmin,
    ShuttingDown,
    ScopeDenied,
    BadConfirmation,
    Unauthorized,
    OverrideDenied,
}

impl ErrorCode {
    pub fn code(self) -> i64 {
        match self {
            ErrorCode::UnknownName => -32000,
            ErrorCode::NameTaken => -32001,
            ErrorCode::NotRegistered => -32002,
            ErrorCode::AlreadyRegistered => -32003,
            ErrorCode::InvalidName => -32004,
            ErrorCode::NotAdmin => -32005,
            ErrorCode::ShuttingDown => -32006,
            ErrorCode::ScopeDenied => -32007,
            ErrorCode::BadConfirmation => -32008,
            ErrorCode::Unauthorized => -32009,
            ErrorCode::OverrideDenied => -32010,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            ErrorCode::UnknownName => "UNKNOWN_NAME",
            ErrorCode::NameTaken => "NAME_TAKEN",
            ErrorCode::NotRegistered => "NOT_REGISTERED",
            ErrorCode::AlreadyRegistered => "ALREADY_REGISTERED",
            ErrorCode::InvalidName => "INVALID_NAME",
            ErrorCode::NotAdmin => "NOT_ADMIN",
            ErrorCode::ShuttingDown => "SHUTTING_DOWN",
            ErrorCode::ScopeDenied => "SCOPE_DENIED",
            ErrorCode::BadConfirmation => "BAD_CONFIRMATION",
            ErrorCode::Unauthorized => "UNAUTHORIZED",
            ErrorCode::OverrideDenied => "OVERRIDE_DENIED",
        }
    }

    pub fn to_error(self, detail: impl Into<String>) -> RpcError {
        RpcError {
            code: self.code(),
            message: detail.into(),
            data: Some(serde_json::json!({ "code": self.name() })),
        }
    }
}

// ---------------------------------------------------------------------------
// Method names
// ---------------------------------------------------------------------------

pub mod methods {
    // Handshake
    pub const SESSION_HELLO: &str = "session/hello";
    // Agent surface
    pub const PRINCIPAL_REGISTER: &str = "principal/register";
    pub const PRINCIPAL_DEREGISTER: &str = "principal/deregister";
    pub const MESSAGE_SEND: &str = "message/send";
    pub const CHANNEL_SUBSCRIBE: &str = "channel/subscribe";
    pub const CHANNEL_UNSUBSCRIBE: &str = "channel/unsubscribe";
    pub const CHANNEL_CREATE: &str = "channel/create";
    pub const HISTORY_GET: &str = "history/get";
    pub const DIRECTORY_WHO: &str = "directory/who";
    // Delivery (broker -> adapter request; adapter -> broker notification)
    pub const MESSAGE_DELIVER: &str = "message/deliver";
    pub const MESSAGE_PROCESSED: &str = "message/processed";
    // Admin surface
    pub const ADMIN_REGISTER: &str = "admin/register";
    pub const ADMIN_SUBSCRIBE: &str = "admin/subscribe";
    pub const ADMIN_UNSUBSCRIBE: &str = "admin/unsubscribe";
    pub const CHANNEL_RENAME: &str = "channel/rename";
    pub const CHANNEL_ARCHIVE: &str = "channel/archive";
    pub const CHANNEL_UNARCHIVE: &str = "channel/unarchive";
    pub const CHANNEL_DELETE: &str = "channel/delete";
    pub const CHANNEL_DELETE_CONFIRM: &str = "channel/delete_confirm";
    pub const MESSAGE_STATUS: &str = "message/status";
    pub const DAEMON_STATUS: &str = "daemon/status";
    pub const DAEMON_SHUTDOWN: &str = "daemon/shutdown";
    // Watch surface
    pub const WATCH_START: &str = "watch/start";
    pub const WATCH_STOP: &str = "watch/stop";
    pub const WATCH_EVENT: &str = "watch/event";
}

// ---------------------------------------------------------------------------
// Message model (docs/architecture/message-model.md)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Recipients {
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub principals: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub message_id: u64,
    pub thread_id: u64,
    /// Unix timestamp in milliseconds, UTC, broker-assigned.
    pub timestamp: u64,
    pub sender: String,
    pub recipients: Recipients,
    pub body: String,
    pub truncated: bool,
}

/// One record of the unified log. `kind` discriminates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Record {
    Message {
        #[serde(flatten)]
        envelope: Envelope,
    },
    System {
        id: u64,
        timestamp: u64,
        event: SystemEvent,
    },
}

impl Record {
    pub fn id(&self) -> u64 {
        match self {
            Record::Message { envelope } => envelope.message_id,
            Record::System { id, .. } => *id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SystemEvent {
    Registered {
        principal: String,
        admin: bool,
    },
    RegistrationDenied {
        name: String,
        reason: String,
    },
    Deregistered {
        principal: String,
    },
    Disconnected {
        principal: String,
    },
    Subscribed {
        principal: String,
        channel: String,
        by_admin: bool,
    },
    Unsubscribed {
        principal: String,
        channel: String,
        by_admin: bool,
    },
    ChannelCreated {
        channel: String,
        by: String,
    },
    ChannelRenamed {
        old_name: String,
        new_name: String,
        by: String,
    },
    ChannelArchived {
        channel: String,
        by: String,
    },
    ChannelUnarchived {
        channel: String,
        by: String,
    },
    /// Deletion tombstone (ADR-0018): permanent record of what was redacted.
    ChannelDeleted {
        channel_id: u64,
        name: String,
        record_count: u64,
        by: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckState {
    Held,
    Relayed,
    Processed,
    Failed,
}

/// Current acknowledgment state of one recipient of one message, with a
/// timestamp per state reached. State is stored; transitions are not.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckStatus {
    pub recipient: String,
    pub state: AckState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relayed_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processed_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<u64>,
}

/// Live ack transition, streamed on watch/event only — never a log record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckTransition {
    /// Always the literal "ack".
    pub kind: String,
    pub message_id: u64,
    pub recipient: String,
    pub state: AckState,
    pub timestamp: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Everything watch/event can carry: log records plus wire-only ack
/// transitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WatchEvent {
    Record(Record),
    Ack(AckTransition),
}

// ---------------------------------------------------------------------------
// Method params and results
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClientInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    pub version: String,
    pub pid: u32,
    pub cwd: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloParams {
    pub client_info: ClientInfo,
    /// Shared-secret token; required when the broker is configured with one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloResult {
    pub broker_version: String,
    pub session_id: u64,
}

/// Delivery coordinates for a Codex-harness principal (CX-7): the shared
/// app-server the broker dials and the agent's thread. Self-reported at
/// registration (the agent reads its own $CODEX_THREAD_ID).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexCoordinates {
    /// WebSocket endpoint of the shared `codex app-server --listen`.
    pub app_server: String,
    pub thread_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterParams {
    pub name: String,
    /// Present when the registering session is a Codex agent: deliveries are
    /// then injected via the app-server instead of the registering connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex: Option<CodexCoordinates>,
    /// admin/register only: the admin credential (ADR-0019). The broker
    /// compares and discards it — it never appears in records or errors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResult {
    pub principal: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendParams {
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub principals: Vec<String>,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedDelivery {
    pub principal: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmptyAudience {
    NoSubscribers,
    EmptyIntersection,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeliveryReport {
    /// Recipients accepted for delivery (held or relayed).
    pub delivered: Vec<String>,
    pub failed: Vec<FailedDelivery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty_audience: Option<EmptyAudience>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendResult {
    pub message_id: u64,
    pub thread_id: u64,
    pub delivery: DeliveryReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelParams {
    pub channel: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelCreateResult {
    pub channel: String,
}

/// history/get scope: exactly one of the three forms.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HistoryScope {
    Channel { channel: String },
    DmWith { dm_with: String },
    DmBetween { dm_between: [String; 2] },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryParams {
    pub scope: HistoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_message_id: Option<u64>,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryResult {
    pub records: Vec<Record>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelDirectoryEntry {
    pub channel: String,
    pub subscribers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalDirectoryEntry {
    pub principal: String,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhoResult {
    pub channels: Vec<ChannelDirectoryEntry>,
    pub principals: Vec<PrincipalDirectoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminSubscriptionParams {
    pub principal: String,
    pub channel: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelRenameParams {
    pub channel: String,
    pub new_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelDeleteResult {
    pub confirmation_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelDeleteConfirmParams {
    pub channel: String,
    pub confirmation_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageStatusParams {
    pub message_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageStatusResult {
    pub acks: Vec<AckStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    pub admin: bool,
    pub client_info: ClientInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatusResult {
    pub broker_version: String,
    pub uptime_seconds: u64,
    pub sessions: Vec<SessionInfo>,
    pub channel_count: u64,
    pub principal_count: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WatchStartParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliverParams {
    pub recipient: String,
    pub envelope: Envelope,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliverResult {
    /// Always the literal "relayed" on success.
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedParams {
    pub message_id: u64,
    pub recipient: String,
}

// ---------------------------------------------------------------------------
// Delivery rendering (message-model.md, "Delivery rendering")
// ---------------------------------------------------------------------------

/// Render an envelope as the structured block adapters hand to a harness:
/// identifies the bus channel(s), the sender, the thread, the body, and how
/// to reply through the bus tools.
pub fn render_delivery(e: &Envelope) -> String {
    let via = if e.recipients.channels.is_empty() {
        "(direct message)".to_string()
    } else {
        e.recipients.channels.join(" ")
    };
    format!(
        "Bus message on {via} from {} (thread {}):\n\n{}\n\nTo reply, call the send tool with \
         thread_id {} addressed to the originating channel and/or sender.",
        e.sender, e.thread_id, e.body, e.thread_id
    )
}

// ---------------------------------------------------------------------------
// Names (message-model.md, Identifiers and naming)
// ---------------------------------------------------------------------------

/// Channel display names: `#` + lowercase alphanumeric and `-`.
pub fn valid_channel_name(name: &str) -> bool {
    name.strip_prefix('#').is_some_and(|rest| {
        !rest.is_empty()
            && rest
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    })
}

/// Principal names: `@` + lowercase alphanumeric and `-`.
pub fn valid_principal_name(name: &str) -> bool {
    name.strip_prefix('@').is_some_and(|rest| {
        !rest.is_empty()
            && rest
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_messages() {
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"session/hello","params":{}}"#;
        assert!(matches!(Message::parse(req).unwrap(), Message::Request(_)));
        let notif = r#"{"jsonrpc":"2.0","method":"watch/event","params":{}}"#;
        assert!(matches!(
            Message::parse(notif).unwrap(),
            Message::Notification(_)
        ));
        let resp = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
        assert!(matches!(
            Message::parse(resp).unwrap(),
            Message::Response(_)
        ));
        let err = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"x"}}"#;
        assert!(matches!(Message::parse(err).unwrap(), Message::Response(_)));
    }

    #[test]
    fn record_wire_shape() {
        let rec = Record::Message {
            envelope: Envelope {
                message_id: 7,
                thread_id: 7,
                timestamp: 123,
                sender: "@a".into(),
                recipients: Recipients {
                    channels: vec!["#g".into()],
                    principals: vec![],
                },
                body: "hi".into(),
                truncated: false,
            },
        };
        let v = serde_json::to_value(&rec).unwrap();
        assert_eq!(v["kind"], "message");
        assert_eq!(v["message_id"], 7);
        let back: Record = serde_json::from_value(v).unwrap();
        assert_eq!(back.id(), 7);

        let sys = Record::System {
            id: 8,
            timestamp: 124,
            event: SystemEvent::Registered {
                principal: "@a".into(),
                admin: false,
            },
        };
        let v = serde_json::to_value(&sys).unwrap();
        assert_eq!(v["kind"], "system");
        assert_eq!(v["event"]["type"], "registered");
    }

    #[test]
    fn history_scope_forms() {
        let s: HistoryScope = serde_json::from_str(r##"{"channel":"#g"}"##).unwrap();
        assert!(matches!(s, HistoryScope::Channel { .. }));
        let s: HistoryScope = serde_json::from_str(r#"{"dm_with":"@b"}"#).unwrap();
        assert!(matches!(s, HistoryScope::DmWith { .. }));
        let s: HistoryScope = serde_json::from_str(r#"{"dm_between":["@a","@b"]}"#).unwrap();
        assert!(matches!(s, HistoryScope::DmBetween { .. }));
    }

    #[test]
    fn watch_event_forms() {
        let ack = WatchEvent::Ack(AckTransition {
            kind: "ack".into(),
            message_id: 1,
            recipient: "@a".into(),
            state: AckState::Relayed,
            timestamp: 5,
            reason: None,
        });
        let v = serde_json::to_value(&ack).unwrap();
        assert_eq!(v["kind"], "ack");
        let back: WatchEvent = serde_json::from_value(v).unwrap();
        assert!(matches!(back, WatchEvent::Ack(_)));
    }

    #[test]
    fn name_validation() {
        assert!(valid_channel_name("#general"));
        assert!(valid_channel_name("#sec-review-2"));
        assert!(!valid_channel_name("general"));
        assert!(!valid_channel_name("#General"));
        assert!(!valid_channel_name("#"));
        assert!(valid_principal_name("@sec-reviewer"));
        assert!(!valid_principal_name("sec"));
        assert!(!valid_principal_name("@UP"));
    }

    #[test]
    fn error_shape() {
        let e = ErrorCode::UnknownName.to_error("no such channel: #nope");
        assert_eq!(e.code, -32000);
        assert_eq!(e.data.unwrap()["code"], "UNKNOWN_NAME");
    }
}
