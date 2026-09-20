//! Minimal vLLM `/v1/realtime` WebSocket protocol client.
//!
//! Client -> Server:
//!   {"type":"session.update","model":"..."}
//!   {"type":"input_audio_buffer.append","audio":"<base64 PCM16@16k mono>"}
//!   {"type":"input_audio_buffer.commit","final":bool}
//! Server -> Client:
//!   {"type":"session.created"}
//!   {"type":"transcription.delta","delta":"..."}
//!   {"type":"transcription.done","text":"...","usage":{...}}
//!   {"type":"error",...}
//!
//! Simple and reliable: one connection, continuous append, concurrent read.

use base64::Engine as _;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::audio::AudioChunker;

/// Events received from the server.
#[derive(Debug, Clone)]
pub enum ServerEvent {
    Created { id: String },
    Delta { text: String },
    Done { text: String, usage: Option<Value> },
    Error { message: String, code: Option<String> },
}

/// Client -> Server messages.
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum ClientMessage<'a> {
    #[serde(rename = "session.update")]
    SessionUpdate { model: &'a str },
    #[serde(rename = "input_audio_buffer.append")]
    AppendAudio { audio: &'a str },
    #[serde(rename = "input_audio_buffer.commit")]
    Commit {
        #[serde(rename = "final")]
        is_final: bool,
    },
}

/// Server error payload accepts two shapes: plain string or object.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum WireError {
    Text(String),
    Object {
        message: Option<String>,
        code: Option<String>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
enum WireEvent {
    #[serde(rename = "session.created")]
    Created {
        #[serde(default)]
        id: String,
    },
    #[serde(rename = "transcription.delta")]
    Delta {
        #[serde(default, rename = "delta")]
        text: String,
    },
    #[serde(rename = "transcription.done")]
    Done {
        #[serde(default)]
        text: String,
        #[serde(default)]
        usage: Option<Value>,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(default)]
        error: Option<WireError>,
        #[serde(default)]
        message: Option<String>,
        #[serde(default)]
        code: Option<String>,
    },
    #[serde(other)]
    Unknown,
}

fn wire_to_event(ev: WireEvent) -> Option<ServerEvent> {
    Some(match ev {
        WireEvent::Unknown => return None,
        WireEvent::Created { id } => ServerEvent::Created { id },
        WireEvent::Delta { text } => ServerEvent::Delta { text },
        WireEvent::Done { text, usage } => ServerEvent::Done { text, usage },
        WireEvent::Error { error, message, code } => {
            let (message, code) = match error {
                Some(WireError::Text(text)) => (text, None),
                Some(WireError::Object { message, code }) => (message.unwrap_or_default(), code),
                None => (message.unwrap_or_default(), code),
            };
            ServerEvent::Error { message, code }
        }
    })
}

fn parse_wire_event(txt: &str) -> Option<ServerEvent> {
    let v: Value = serde_json::from_str(txt).ok()?;
    serde_json::from_value::<WireEvent>(v).ok().and_then(wire_to_event)
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Sender half (write side).
pub struct RealtimeSender {
    tx: SplitSink<WsStream, Message>,
    chunker: AudioChunker,
}

/// Receiver half (read side).
pub struct RealtimeReceiver {
    rx: SplitStream<WsStream>,
}

/// Connect to `ws://host/v1/realtime`, split into independent sender/receiver.
pub async fn connect(
    url: &str,
    sample_rate: u32,
    chunk_ms: u64,
) -> Result<(RealtimeSender, RealtimeReceiver), Box<dyn std::error::Error + Send + Sync>> {
    let (ws, _) = connect_async(url).await?;
    let (tx, rx) = ws.split();
    Ok((
        RealtimeSender {
            tx,
            chunker: AudioChunker::new(sample_rate, chunk_ms),
        },
        RealtimeReceiver { rx },
    ))
}

impl RealtimeSender {
    /// Validate the model.
    pub async fn update_model(&mut self, model: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.send_json(ClientMessage::SessionUpdate { model }).await
    }

    /// Start generation with a non-final commit (required).
    pub async fn start_generation(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.send_json(ClientMessage::Commit { is_final: false }).await
    }

    /// Append raw PCM16 mono bytes (chunked internally).
    pub async fn append_bytes(&mut self, pcm: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for chunk in self.chunker.chunks(pcm) {
            let b64 = base64::engine::general_purpose::STANDARD.encode(chunk);
            self.send_json(ClientMessage::AppendAudio { audio: &b64 }).await?;
        }
        Ok(())
    }

    /// Final commit: signals end of audio.
    pub async fn finish(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.send_json(ClientMessage::Commit { is_final: true }).await
    }

    /// Close write half.
    pub async fn close(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.tx.close().await?;
        Ok(())
    }

    async fn send_json(&mut self, msg: ClientMessage<'_>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.tx.send(Message::Text(serde_json::to_string(&msg)?.into())).await?;
        Ok(())
    }
}

impl RealtimeReceiver {
    /// Read next server event, skipping unknown messages.
    pub async fn read_event(&mut self) -> Result<ServerEvent, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let msg = self.rx.next().await.ok_or_else(|| "websocket closed".to_string())?;
            match msg? {
                Message::Text(txt) => {
                    if let Some(ev) = parse_wire_event(&txt) {
                        return Ok(ev);
                    }
                }
                Message::Binary(_) => {}
                Message::Close(_) => return Err("websocket closed by server".into()),
                _ => {}
            }
        }
    }
}
