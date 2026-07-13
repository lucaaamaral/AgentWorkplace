//! JSON-RPC dispatch and broker method handlers.

use super::*;

impl Broker {
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
            let Ok(p) =
                serde_json::from_value::<ProcessedParams>(notif.params.unwrap_or(Value::Null))
            else {
                return;
            };
            // Only the session bound to the recipient may report processing.
            if session.principal().as_deref() == Some(p.recipient.as_str()) {
                self.ack_update(p.message_id, &p.recipient, AckState::Processed, None);
            }
        }
    }

    async fn dispatch(
        &self,
        session: &Arc<Session>,
        method: &str,
        params: Value,
    ) -> Result<Value, RpcError> {
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
        if let Some(required) = &self.0.cfg.auth_token
            && p.auth_token.as_deref() != Some(required.as_str())
        {
            return Err(ErrorCode::Unauthorized.to_error("missing or invalid auth token"));
        }
        session.state.lock().unwrap().client_info = Some(p.client_info);
        to_value(HelloResult {
            broker_version: self.0.cfg.version.clone(),
            session_id: session.id,
        })
    }

    fn register(
        &self,
        session: &Arc<Session>,
        p: RegisterParams,
        admin: bool,
    ) -> Result<Value, RpcError> {
        if !valid_principal_name(&p.name) {
            return Err(ErrorCode::InvalidName.to_error(format!(
                "principal names are '@' + lowercase alphanumeric/'-': {}",
                p.name
            )));
        }
        if let Some(codex) = &p.codex {
            // The app-server endpoint is self-reported over the wire: the
            // broker only ever dials loopback WebSocket endpoints — anything
            // else is a request to exfiltrate bus traffic (SSRF).
            if !loopback_ws_url(&codex.app_server) {
                return Err(ErrorCode::InvalidName.to_error(format!(
                    "codex app_server must be a loopback ws:// endpoint: {}",
                    codex.app_server
                )));
            }
        }
        if admin {
            // ADR-0019: admin rights require the admin credential. The
            // supplied value is compared and discarded — never logged,
            // never echoed, never recorded.
            let authorized = match (&self.0.cfg.admin_token, &p.admin_token) {
                (Some(required), Some(supplied)) => required == supplied,
                _ => false,
            };
            if !authorized {
                self.system_record(
                    &SystemEvent::RegistrationDenied {
                        name: p.name.clone(),
                        reason: "invalid admin credential".into(),
                    },
                    None,
                );
                return Err(ErrorCode::Unauthorized.to_error(
                    "admin registration requires the admin token (see the daemon's admin-token \
                     file, ADR-0019)",
                ));
            }
        }
        if session.principal().is_some() {
            return Err(
                ErrorCode::AlreadyRegistered.to_error("session already bound to a principal")
            );
        }
        // Claim atomically: check-then-bind under the claim lock, so two
        // sessions racing for one name cannot both win.
        {
            let _claim = self.0.claim_lock.lock().unwrap();
            if self.principal_active(&p.name) {
                drop(_claim);
                self.system_record(
                    &SystemEvent::RegistrationDenied {
                        name: p.name.clone(),
                        reason: "actively claimed".into(),
                    },
                    None,
                );
                return Err(
                    ErrorCode::NameTaken.to_error(format!("{} is actively claimed", p.name))
                );
            }
            let mut st = session.state.lock().unwrap();
            st.principal = Some(p.name.clone());
            st.admin = admin;
            st.codex = p.codex;
        }
        // Persist the principal row and its Registered record in ONE store
        // transaction before the bind is considered done: a failure must not
        // leave a phantom DM-addressable principal, nor an unaudited
        // registration.
        let record = match self.0.store.register_principal(&p.name, admin) {
            Ok(record) => record,
            Err(e) => {
                let mut st = session.state.lock().unwrap();
                st.principal = None;
                st.admin = false;
                st.codex = None;
                return Err(internal(e));
            }
        };
        self.broadcast(WatchEvent::Record(record), None, false);
        self.drain_held(&p.name);
        to_value(RegisterResult { principal: p.name })
    }

    fn deregister(&self, session: &Arc<Session>) -> Result<Value, RpcError> {
        let Some(principal) = session.principal() else {
            return Err(ErrorCode::NotRegistered.to_error("session has no principal"));
        };
        // Audit first: a deregistration that cannot be recorded fails with
        // the binding intact (registration symmetry — every registration has
        // its deregistration record).
        let record = self
            .0
            .store
            .append_system(
                &SystemEvent::Deregistered {
                    principal: principal.clone(),
                },
                None,
            )
            .map_err(internal)?;
        {
            let mut st = session.state.lock().unwrap();
            st.principal = None;
            st.admin = false;
            st.codex = None;
        }
        self.broadcast(WatchEvent::Record(record), None, false);
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
            match self.0.store.channel_by_name(name).map_err(internal)? {
                Some(c) if !c.archived => channel_ids.push(c.id),
                Some(_) => {
                    return Err(ErrorCode::UnknownName.to_error(format!("{name} is archived")));
                }
                None => {
                    return Err(ErrorCode::UnknownName.to_error(format!("no such channel: {name}")));
                }
            }
        }
        for name in &p.principals {
            if self.0.store.principal_id(name).map_err(internal)?.is_none() {
                return Err(ErrorCode::UnknownName.to_error(format!("no such principal: {name}")));
            }
        }
        if let Some(thread) = p.thread_id
            && !self.0.store.message_exists(thread).map_err(internal)?
        {
            return Err(ErrorCode::UnknownName.to_error(format!("no such thread: {thread}")));
        }

        // Resolve the audience (intersection semantics, ADR-0009).
        let mut audience: Vec<String> = Vec::new();
        let mut empty_audience = None;
        if !p.channels.is_empty() && !p.principals.is_empty() {
            for principal in &p.principals {
                let pid = self
                    .0
                    .store
                    .principal_id(principal)
                    .map_err(internal)?
                    .ok_or_else(|| internal("principal disappeared mid-send"))?;
                let mut subscribed = false;
                for cid in &channel_ids {
                    if self.0.store.is_subscribed(pid, *cid).map_err(internal)? {
                        subscribed = true;
                        break;
                    }
                }
                if subscribed {
                    audience.push(principal.clone());
                }
            }
            if audience.is_empty() {
                empty_audience = Some(EmptyAudience::EmptyIntersection);
            }
        } else if !p.channels.is_empty() {
            let mut set = HashSet::new();
            for cid in &channel_ids {
                set.extend(self.0.store.subscribers_of(*cid).map_err(internal)?);
            }
            audience = set.into_iter().collect();
            if audience.is_empty() {
                empty_audience = Some(EmptyAudience::NoSubscribers);
            }
        } else {
            audience = p.principals.clone();
        }
        // One delivery per recipient, however many times they were addressed.
        audience.sort();
        audience.dedup();
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

        let recipients = Recipients {
            channels: p.channels.clone(),
            principals: p.principals.clone(),
        };
        // Partition by presence up front so the message and every initial
        // ack row commit in one transaction: a stored message always has its
        // inspectable per-recipient ack state.
        let (held, absent): (Vec<String>, Vec<String>) = audience
            .iter()
            .cloned()
            .partition(|r| self.principal_active(r));
        let envelope = self
            .0
            .store
            .append_message_with_acks(
                &sender,
                &recipients,
                &body,
                truncated,
                p.thread_id,
                &channel_ids,
                &held,
                &absent,
            )
            .map_err(internal)?;

        let mut report = DeliveryReport {
            empty_audience,
            ..Default::default()
        };
        report.delivered = held.clone();
        for recipient in &held {
            self.spawn_deliver(recipient.clone(), envelope.clone(), true);
        }
        for recipient in &absent {
            report.failed.push(FailedDelivery {
                principal: recipient.clone(),
                reason: "disconnected".into(),
            });
            self.broadcast_ack(
                envelope.message_id,
                recipient,
                AckState::Failed,
                now_ms(),
                Some("disconnected"),
            );
        }

        // Admin observability tap: every message, never counted (rpc-surface).
        for s in self.sessions() {
            let (is_admin, principal) = {
                let st = s.state.lock().unwrap();
                (st.admin, st.principal.clone())
            };
            if is_admin
                && let Some(admin_principal) = principal
                && admin_principal != sender
                && !audience.contains(&admin_principal)
            {
                self.spawn_deliver(admin_principal, envelope.clone(), false);
            }
        }

        let is_dm = envelope.recipients.channels.is_empty();
        self.broadcast(
            WatchEvent::Record(Record::Message {
                envelope: envelope.clone(),
            }),
            Some(envelope.recipients.channels.clone()),
            is_dm,
        );

        to_value(SendResult {
            message_id: envelope.message_id,
            thread_id: envelope.thread_id,
            delivery: report,
        })
    }

    fn subscribe(&self, session: &Arc<Session>, p: ChannelParams) -> Result<Value, RpcError> {
        self.self_subscription(session, p, true)
    }

    fn unsubscribe(&self, session: &Arc<Session>, p: ChannelParams) -> Result<Value, RpcError> {
        self.self_subscription(session, p, false)
    }

    /// Self-service membership change: override check, mutation, and audit
    /// record are one store transaction (ADR-0009 precedence cannot be raced
    /// past, and no change goes unlogged).
    fn self_subscription(
        &self,
        session: &Arc<Session>,
        p: ChannelParams,
        subscribe: bool,
    ) -> Result<Value, RpcError> {
        let Some(principal) = session.principal() else {
            return Err(ErrorCode::NotRegistered.to_error("register before (un)subscribing"));
        };
        let ch = self.live_channel(&p.channel)?;
        let pid = self.principal_row(&principal)?;
        match self
            .0
            .store
            .self_subscription(pid, ch.id, subscribe, &principal, &ch.name)
            .map_err(internal)?
        {
            SubOutcome::Applied(record) => {
                self.broadcast(
                    WatchEvent::Record(record),
                    Some(vec![ch.name.clone()]),
                    false,
                );
                Ok(Value::Null)
            }
            SubOutcome::NoChange => Ok(Value::Null),
            SubOutcome::Denied(OverrideMode::Cancelled) => {
                Err(ErrorCode::OverrideDenied.to_error(format!(
                    "{} was cancelled from {} by the manager",
                    principal, ch.name
                )))
            }
            SubOutcome::Denied(OverrideMode::Forced) => Err(ErrorCode::OverrideDenied.to_error(
                format!("{} is forced onto {} by the manager", principal, ch.name),
            )),
        }
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
        match self.0.store.create_channel_logged(&p.name, &principal) {
            Ok((_id, record)) => {
                self.broadcast(
                    WatchEvent::Record(record),
                    Some(vec![p.name.clone()]),
                    false,
                );
                to_value(ChannelCreateResult { channel: p.name })
            }
            Err(StoreError::NameTaken) => Err(ErrorCode::NameTaken
                .to_error(format!("{} already exists (live or archived)", p.name))),
            Err(e) => Err(internal(e)),
        }
    }

    fn history(&self, session: &Arc<Session>, p: HistoryParams) -> Result<Value, RpcError> {
        let admin = session.is_admin();
        let limit = p.limit.clamp(1, 1000);
        let records =
            match &p.scope {
                HistoryScope::Channel { channel } => {
                    let Some(ch) = self.0.store.channel_by_name(channel).map_err(internal)? else {
                        return Err(
                            ErrorCode::UnknownName.to_error(format!("no such channel: {channel}"))
                        );
                    };
                    if ch.archived && !admin {
                        return Err(ErrorCode::ScopeDenied
                            .to_error("archived channel history is admin-only"));
                    }
                    self.0
                        .store
                        .history_channel(ch.id, p.before_message_id, limit)
                        .map_err(internal)?
                }
                HistoryScope::DmWith { dm_with } => {
                    let Some(me) = session.principal() else {
                        return Err(
                            ErrorCode::NotRegistered.to_error("register before reading DM history")
                        );
                    };
                    if self
                        .0
                        .store
                        .principal_id(dm_with)
                        .map_err(internal)?
                        .is_none()
                    {
                        return Err(ErrorCode::UnknownName
                            .to_error(format!("no such principal: {dm_with}")));
                    }
                    self.0
                        .store
                        .history_dm(&me, dm_with, p.before_message_id, limit)
                        .map_err(internal)?
                }
                HistoryScope::DmBetween { dm_between } => {
                    if !admin {
                        return Err(ErrorCode::ScopeDenied.to_error("dm_between is admin-only"));
                    }
                    for principal in dm_between {
                        if self
                            .0
                            .store
                            .principal_id(principal)
                            .map_err(internal)?
                            .is_none()
                        {
                            return Err(ErrorCode::UnknownName
                                .to_error(format!("no such principal: {principal}")));
                        }
                    }
                    self.0
                        .store
                        .history_dm(&dm_between[0], &dm_between[1], p.before_message_id, limit)
                        .map_err(internal)?
                }
            };
        let next_cursor = if records.len() as u32 == limit {
            records.first().map(|r| r.id())
        } else {
            None
        };
        to_value(HistoryResult {
            records,
            next_cursor,
        })
    }

    fn who(&self) -> Result<Value, RpcError> {
        let mut channels = Vec::new();
        for c in self.0.store.live_channels().map_err(internal)? {
            channels.push(ChannelDirectoryEntry {
                subscribers: self.0.store.subscribers_of(c.id).map_err(internal)?,
                channel: c.name,
            });
        }
        let principals = self
            .0
            .store
            .all_principals()
            .map_err(internal)?
            .into_iter()
            .map(|p| PrincipalDirectoryEntry {
                active: self.principal_active(&p),
                principal: p,
            })
            .collect();
        to_value(WhoResult {
            channels,
            principals,
        })
    }

    fn require_admin(&self, session: &Arc<Session>) -> Result<String, RpcError> {
        let st = session.state.lock().unwrap();
        match (st.admin, st.principal.clone()) {
            (true, Some(p)) => Ok(p),
            (true, None) => Err(internal("admin session without principal")),
            _ => Err(ErrorCode::NotAdmin.to_error("admin verb on a non-admin session")),
        }
    }

    fn principal_row(&self, principal: &str) -> Result<i64, RpcError> {
        self.0
            .store
            .principal_id(principal)
            .map_err(internal)?
            .ok_or_else(|| internal(format!("principal {principal} not persisted")))
    }

    fn admin_subscription(
        &self,
        session: &Arc<Session>,
        p: AdminSubscriptionParams,
        subscribe: bool,
    ) -> Result<Value, RpcError> {
        self.require_admin(session)?;
        let Some(pid) = self.0.store.principal_id(&p.principal).map_err(internal)? else {
            return Err(
                ErrorCode::UnknownName.to_error(format!("no such principal: {}", p.principal))
            );
        };
        let ch = self.live_channel(&p.channel)?;
        // Human-set state wins (ADR-0009): policy + membership + audit record
        // commit in one transaction; a policy transition is logged even when
        // effective membership did not change.
        if let Some(record) = self
            .0
            .store
            .admin_subscription(pid, ch.id, subscribe, &p.principal, &ch.name)
            .map_err(internal)?
        {
            self.broadcast(
                WatchEvent::Record(record),
                Some(vec![ch.name.clone()]),
                false,
            );
        }
        Ok(Value::Null)
    }

    fn rename(&self, session: &Arc<Session>, p: ChannelRenameParams) -> Result<Value, RpcError> {
        let by = self.require_admin(session)?;
        // Rename works on archived channels too (ADR-0018 escape hatch).
        let Some(ch) = self.0.store.channel_by_name(&p.channel).map_err(internal)? else {
            return Err(ErrorCode::UnknownName.to_error(format!("no such channel: {}", p.channel)));
        };
        if !valid_channel_name(&p.new_name) {
            return Err(
                ErrorCode::InvalidName.to_error(format!("invalid channel name: {}", p.new_name))
            );
        }
        match self
            .0
            .store
            .rename_channel_logged(ch.id, &p.channel, &p.new_name, &by)
        {
            Ok(record) => {
                self.broadcast(
                    WatchEvent::Record(record),
                    Some(vec![p.new_name.clone()]),
                    false,
                );
                Ok(Value::Null)
            }
            Err(StoreError::NameTaken) => {
                Err(ErrorCode::NameTaken.to_error(format!("{} already exists", p.new_name)))
            }
            Err(e) => Err(internal(e)),
        }
    }

    fn archive(
        &self,
        session: &Arc<Session>,
        p: ChannelParams,
        archive: bool,
    ) -> Result<Value, RpcError> {
        let by = self.require_admin(session)?;
        let Some(ch) = self.0.store.channel_by_name(&p.channel).map_err(internal)? else {
            return Err(ErrorCode::UnknownName.to_error(format!("no such channel: {}", p.channel)));
        };
        if ch.archived == archive {
            return Ok(Value::Null); // idempotent
        }
        let records = self
            .0
            .store
            .archive_channel(ch.id, &ch.name, &by, archive)
            .map_err(internal)?;
        for record in records {
            self.broadcast(
                WatchEvent::Record(record),
                Some(vec![ch.name.clone()]),
                false,
            );
        }
        Ok(Value::Null)
    }

    fn delete_request(&self, session: &Arc<Session>, p: ChannelParams) -> Result<Value, RpcError> {
        self.require_admin(session)?;
        let Some(ch) = self.0.store.channel_by_name(&p.channel).map_err(internal)? else {
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
        to_value(ChannelDeleteResult {
            confirmation_token: token,
        })
    }

    fn delete_confirm(
        &self,
        session: &Arc<Session>,
        p: ChannelDeleteConfirmParams,
    ) -> Result<Value, RpcError> {
        let by = self.require_admin(session)?;
        let token = self
            .0
            .delete_tokens
            .lock()
            .unwrap()
            .remove(&p.confirmation_token);
        let Some(token) = token else {
            return Err(
                ErrorCode::BadConfirmation.to_error("unknown or already-used confirmation token")
            );
        };
        let current_id = self
            .0
            .store
            .channel_by_name(&token.channel_name)
            .map_err(internal)?
            .map(|c| c.id);
        if token.session_id != session.id
            || token.expires < Instant::now()
            || current_id != Some(token.channel_id)
            || p.channel != token.channel_name
        {
            return Err(
                ErrorCode::BadConfirmation.to_error("confirmation token expired or mismatched")
            );
        }
        let (_record_count, record) = self
            .0
            .store
            .delete_channel_logged(token.channel_id, &token.channel_name, &by)
            .map_err(internal)?;
        self.broadcast(WatchEvent::Record(record), None, false);
        Ok(Value::Null)
    }

    fn message_status(
        &self,
        session: &Arc<Session>,
        p: MessageStatusParams,
    ) -> Result<Value, RpcError> {
        self.require_admin(session)?;
        if !self
            .0
            .store
            .message_exists(p.message_id)
            .map_err(internal)?
        {
            return Err(
                ErrorCode::UnknownName.to_error(format!("no such message: {}", p.message_id))
            );
        }
        to_value(MessageStatusResult {
            acks: self.0.store.acks_for(p.message_id).map_err(internal)?,
        })
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
            channel_count: self.0.store.channel_count().map_err(internal)?,
            principal_count: self.0.store.principal_count().map_err(internal)?,
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
                    if self
                        .0
                        .store
                        .channel_by_name(name)
                        .map_err(internal)?
                        .is_none()
                    {
                        return Err(
                            ErrorCode::UnknownName.to_error(format!("no such channel: {name}"))
                        );
                    }
                }
                WatchFilter::Channels(names.into_iter().collect())
            }
        };
        session.state.lock().unwrap().watch = filter;
        Ok(Value::Null)
    }

    fn live_channel(&self, name: &str) -> Result<crate::store::ChannelRow, RpcError> {
        match self.0.store.channel_by_name(name).map_err(internal)? {
            Some(c) if !c.archived => Ok(c),
            Some(_) => Err(ErrorCode::UnknownName.to_error(format!("{name} is archived"))),
            None => Err(ErrorCode::UnknownName.to_error(format!("no such channel: {name}"))),
        }
    }
}

/// Only loopback WebSocket endpoints are dialable as codex app-servers: the
/// value arrives self-reported over the wire.
fn loopback_ws_url(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("ws://") else {
        return false;
    };
    let authority = rest.split('/').next().unwrap_or("");
    let host = if let Some(v6) = authority.strip_prefix('[') {
        v6.split(']').next().unwrap_or("")
    } else {
        authority
            .rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or(authority)
    };
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
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
    RpcError {
        code: -32603,
        message: format!("internal error: {e}"),
        data: None,
    }
}

#[cfg(test)]
mod tests {
    use super::loopback_ws_url;

    #[test]
    fn app_server_url_validation() {
        assert!(loopback_ws_url("ws://127.0.0.1:9701"));
        assert!(loopback_ws_url("ws://localhost:9701"));
        assert!(loopback_ws_url("ws://[::1]:9701"));
        assert!(loopback_ws_url("ws://127.0.0.1:9701/path"));
        assert!(!loopback_ws_url("ws://10.0.0.8:9701"));
        assert!(!loopback_ws_url("ws://evil.example:9701"));
        assert!(!loopback_ws_url("wss://127.0.0.1:9701")); // shared local server is plain ws
        assert!(!loopback_ws_url("http://127.0.0.1:9701"));
        assert!(!loopback_ws_url("127.0.0.1:9701"));
    }
}
