//! TCP listener and per-connection loop. NDJSON JSON-RPC (ADR-0016).

use protocol::{Id, Message, RpcError, err_response};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::core::Broker;

/// Slack on top of the body size limit for envelope/JSON overhead when
/// bounding a single inbound line.
const LINE_OVERHEAD: usize = 64 * 1024;

/// Bind every configured address and serve until shutdown is signalled.
pub async fn run(broker: Broker) -> anyhow::Result<()> {
    let mut listeners = Vec::new();
    for addr in &broker.0.cfg.listens {
        listeners.push(
            TcpListener::bind(addr)
                .await
                .map_err(|e| anyhow::anyhow!("cannot bind {addr}: {e}"))?,
        );
        tracing::info!("listening on {addr}");
    }
    let mut shutdown = broker.shutdown_signal();
    let mut accept_tasks = Vec::new();
    for listener in listeners {
        let broker = broker.clone();
        accept_tasks.push(tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        tracing::debug!("connection from {peer}");
                        let broker = broker.clone();
                        tokio::spawn(async move { handle_connection(broker, stream).await });
                    }
                    Err(e) => {
                        tracing::warn!("accept error: {e}");
                    }
                }
            }
        }));
    }
    let _ = shutdown.wait_for(|stop| *stop).await;
    for task in accept_tasks {
        task.abort();
    }
    tracing::info!("broker shut down");
    Ok(())
}

/// Serve one broker on one pre-bound listener (integration tests).
pub async fn serve_listener(broker: Broker, listener: TcpListener) {
    let mut shutdown = broker.shutdown_signal();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { break };
                let broker = broker.clone();
                tokio::spawn(async move { handle_connection(broker, stream).await });
            }
            _ = shutdown.wait_for(|stop| *stop) => break,
        }
    }
}

/// One inbound NDJSON line, or `Oversized` when it exceeded the cap (the
/// excess is consumed so the connection can be torn down cleanly).
enum InboundLine {
    Line(String),
    Oversized,
    Eof,
}

/// Read one newline-delimited line with a hard byte cap: the transport
/// memory bound per connection. `BufRead::lines` would buffer an arbitrarily
/// long line before returning it.
async fn read_line_bounded<R>(reader: &mut R, max: usize) -> std::io::Result<InboundLine>
where
    R: AsyncBufReadExt + Unpin,
{
    let mut buf: Vec<u8> = Vec::new();
    let mut oversized = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            // EOF: a final unterminated line still parses.
            if oversized {
                return Ok(InboundLine::Oversized);
            }
            if buf.is_empty() {
                return Ok(InboundLine::Eof);
            }
            break;
        }
        if let Some(pos) = available.iter().position(|b| *b == b'\n') {
            if !oversized {
                buf.extend_from_slice(&available[..pos]);
            }
            reader.consume(pos + 1);
            if buf.len() > max {
                oversized = true;
            }
            break;
        }
        if !oversized {
            buf.extend_from_slice(available);
        }
        let n = available.len();
        reader.consume(n);
        if buf.len() > max {
            oversized = true;
            buf.clear();
        }
    }
    if oversized {
        return Ok(InboundLine::Oversized);
    }
    match String::from_utf8(buf) {
        Ok(s) => Ok(InboundLine::Line(s)),
        Err(_) => Ok(InboundLine::Line(String::new())), // non-UTF8: parse error downstream
    }
}

async fn handle_connection(broker: Broker, stream: TcpStream) {
    let (read_half, mut write_half) = stream.into_split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();
    let session = broker.attach(out_tx);

    // The writer drains queued messages (e.g. a final protocol-violation
    // response) and exits when the last sender drops — aborted only on
    // outbound overflow, where the peer has stopped reading and draining is
    // pointless. It decrements the session's outbound counter per message
    // written (the slow-reader bound lives in Session::send).
    let queued = session.queued.clone();
    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            queued.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            let mut line = msg.to_line();
            line.push('\n');
            if write_half.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    let max_line = broker.0.cfg.message_size_limit + LINE_OVERHEAD;
    let mut reader = BufReader::new(read_half);
    loop {
        if session.overflowed() {
            // The peer stopped reading while the broker kept producing:
            // tear the session down instead of queuing without bound.
            break;
        }
        // Select the overflow signal against the read: an idle peer that
        // never sends another byte must still be torn down when its
        // outbound queue overflows.
        let inbound = tokio::select! {
            r = read_line_bounded(&mut reader, max_line) => r,
            _ = session.overflow_notify.notified() => break,
        };
        let line = match inbound {
            Ok(InboundLine::Line(line)) => line,
            Ok(InboundLine::Oversized) => {
                // Protocol violation: respond, then drop the connection —
                // an unframed peer cannot be resynchronized reliably.
                session.send(Message::Response(err_response(
                    Id::Null,
                    RpcError {
                        code: -32600,
                        message: format!("line exceeds {max_line} bytes"),
                        data: None,
                    },
                )));
                break;
            }
            Ok(InboundLine::Eof) | Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        // Two-stage classification: invalid JSON is a -32700 parse error;
        // valid JSON that is not a request/notification/response shape is a
        // -32600 invalid request.
        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("unparseable line from session {}: {e}", session.id);
                session.send(Message::Response(err_response(
                    Id::Null,
                    RpcError {
                        code: -32700,
                        message: format!("parse error: {e}"),
                        data: None,
                    },
                )));
                continue;
            }
        };
        match Message::from_value(value) {
            Ok(Message::Request(req)) => {
                if req.jsonrpc != "2.0" {
                    session.send(Message::Response(err_response(
                        req.id,
                        RpcError {
                            code: -32600,
                            message: "jsonrpc must be \"2.0\"".into(),
                            data: None,
                        },
                    )));
                    continue;
                }
                // Sequential per connection: a client's requests are handled
                // in the order sent (hello-then-register must not race).
                // Handlers never block — deliveries run on their own tasks.
                let resp = broker.handle_request(&session, req).await;
                session.send(Message::Response(resp));
            }
            Ok(Message::Response(resp)) => {
                if resp.jsonrpc == "2.0" {
                    session.resolve_response(resp);
                }
            }
            Ok(Message::Notification(notif)) => {
                if notif.jsonrpc == "2.0" {
                    broker.handle_notification(&session, notif);
                } else {
                    tracing::warn!(
                        "notification with jsonrpc != 2.0 from session {} ignored",
                        session.id
                    );
                }
            }
            Err(e) => {
                session.send(Message::Response(err_response(
                    Id::Null,
                    RpcError {
                        code: -32600,
                        message: format!("invalid request shape: {e}"),
                        data: None,
                    },
                )));
            }
        }
    }

    broker.detach(&session);
    if session.overflowed() {
        // Close the socket now — nothing queued is deliverable to a peer
        // that stopped reading.
        writer.abort();
    }
}
