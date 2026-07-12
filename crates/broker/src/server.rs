//! TCP listener and per-connection loop. NDJSON JSON-RPC (ADR-0016).

use std::sync::Arc;

use protocol::Message;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::core::Broker;

/// Bind every configured address and serve until shutdown is signalled.
pub async fn run(broker: Broker) -> anyhow::Result<()> {
    let mut listeners = Vec::new();
    for addr in &broker.0.cfg.listens {
        listeners.push(TcpListener::bind(addr).await.map_err(|e| {
            anyhow::anyhow!("cannot bind {addr}: {e}")
        })?);
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

async fn handle_connection(broker: Broker, stream: TcpStream) {
    let (read_half, mut write_half) = stream.into_split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();
    let session = broker.attach(out_tx);

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let mut line = msg.to_line();
            line.push('\n');
            if write_half.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    let mut lines = BufReader::new(read_half).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        match Message::parse(&line) {
            Ok(Message::Request(req)) => {
                let broker = broker.clone();
                let session = Arc::clone(&session);
                tokio::spawn(async move {
                    let resp = broker.handle_request(&session, req).await;
                    let _ = session.out.send(Message::Response(resp));
                });
            }
            Ok(Message::Response(resp)) => session.resolve_response(resp),
            Ok(Message::Notification(notif)) => broker.handle_notification(&session, notif),
            Err(e) => {
                tracing::warn!("unparseable line from session {}: {e}", session.id);
            }
        }
    }

    broker.detach(&session);
    writer.abort();
}
