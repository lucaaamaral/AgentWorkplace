//! Shared TCP harness primitives for broker integration tests.
//!
//! Keep protocol-specific assertions in each test crate; this module only
//! owns connection setup, request/response correlation, and broker startup.

use std::collections::VecDeque;
use std::time::Duration;

use broker::{Broker, BrokerConfig, server};
use protocol::methods as m;
use protocol::{Id, Message, Request, RpcError};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

const RPC_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn start_broker(cfg: BrokerConfig) -> (std::net::SocketAddr, Broker) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let broker = Broker::new(cfg).unwrap();
    tokio::spawn(server::serve_listener(broker.clone(), listener));
    (addr, broker)
}

pub struct Client {
    pub reader: tokio::io::Lines<BufReader<OwnedReadHalf>>,
    pub writer: OwnedWriteHalf,
    next_id: u64,
    pub(crate) queue: VecDeque<Message>,
}

impl Client {
    pub async fn connect(addr: std::net::SocketAddr) -> Client {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (reader, writer) = stream.into_split();
        Client {
            reader: BufReader::new(reader).lines(),
            writer,
            next_id: 1,
            queue: VecDeque::new(),
        }
    }

    pub async fn connect_hello(addr: std::net::SocketAddr) -> Client {
        let mut client = Client::connect(addr).await;
        client
            .call(
                m::SESSION_HELLO,
                json!({ "client_info": { "version": "t", "pid": 1, "cwd": "/" } }),
            )
            .await
            .unwrap();
        client
    }

    pub async fn write(&mut self, msg: Message) {
        let mut line = msg.to_line();
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.unwrap();
    }

    /// Read from the socket without consulting the parked-message queue.
    pub async fn read_socket(&mut self) -> Message {
        loop {
            let line = tokio::time::timeout(RPC_TIMEOUT, self.reader.next_line())
                .await
                .expect("read timeout")
                .unwrap()
                .expect("connection closed");
            if line.trim().is_empty() {
                continue;
            }
            return Message::parse(&line).unwrap();
        }
    }

    pub async fn call(&mut self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id;
        self.next_id += 1;
        self.write(Message::Request(Request {
            jsonrpc: "2.0".into(),
            id: Id::Num(id),
            method: method.into(),
            params: Some(params),
        }))
        .await;
        loop {
            match self.read_socket().await {
                Message::Response(resp) if resp.id == Id::Num(id) => {
                    return match resp.error {
                        Some(error) => Err(error),
                        None => Ok(resp.result.unwrap_or(Value::Null)),
                    };
                }
                other => self.queue.push_back(other),
            }
        }
    }
}
