//! vinput streaming provider for vLLM `/v1/realtime` (Qwen3-ASR).
//!
//! Protocol (stdin/stdout JSON-lines):
//!   stdin:  {"type":"audio","audio_base64":"...","commit":bool}
//!           {"type":"finish"}
//!           {"type":"cancel"}
//!   stdout: {"type":"session_started"}
//!           {"type":"partial","text":"<full cleaned text>"}
//!           {"type":"final","text":"<full cleaned text>"}
//!           {"type":"error","message":"..."}
//!           {"type":"closed"}
//!
//! Design: ONE WebSocket connection for the whole session, continuous
//! append of incoming PCM, concurrent delta reading. No segmentation or
//! reconnect. Model prefix (`language {lang}<asr_text>`) is stripped from
//! every segment and the full cleaned text is emitted as replace-style
//! `partial`/`final` (matching command_streaming_backend semantics).

use base64::Engine as _;
use vinput_ws_asr::asr::{self, ServerEvent};

use std::io::{BufRead, Write};
use std::time::Duration;

const DEFAULT_URL: &str = "ws://192.168.102.10:7000/v1/realtime";
const DEFAULT_MODEL: &str = "qwen3-asr";
const ASR_TEXT_TAG: &str = "<asr_text>";
/// Timeout for waiting `transcription.done` after final commit.
const DONE_TIMEOUT: Duration = Duration::from_secs(9);
/// WS chunk size for outgoing PCM.
const CHUNK_MS: u64 = 250;

/// Strip every `language {lang}<asr_text>` prefix and join segments.
fn strip_prefix(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pos = 0usize;
    while let Some(rel) = text[pos..].find(ASR_TEXT_TAG) {
        let tag = pos + rel;
        let seg = &text[pos..tag];
        out.push_str(strip_trailing_lang_prefix(seg));
        pos = tag + ASR_TEXT_TAG.len();
    }
    out.push_str(strip_trailing_lang_prefix(&text[pos..]));
    out.replace('\n', "")
}

/// If `seg` ends with an unclosed `language <lang>` prefix, drop it.
fn strip_trailing_lang_prefix(seg: &str) -> &str {
    const LP: &str = "language ";
    match seg.rfind(LP) {
        None => seg,
        Some(p) => {
            let after = &seg[p + LP.len()..];
            if !after.is_empty() && after.chars().all(|c| !c.is_whitespace()) {
                &seg[..p]
            } else {
                seg
            }
        }
    }
}

/// Cleaner that returns full cleaned text so far (replace semantics).
struct StreamCleaner {
    raw: String,
}

impl StreamCleaner {
    fn new() -> Self {
        Self { raw: String::new() }
    }
    fn push_delta(&mut self, delta: &str) -> String {
        self.raw.push_str(delta);
        strip_prefix(&self.raw)
    }
    fn current(&self) -> String {
        strip_prefix(&self.raw)
    }
}

fn write_event(v: serde_json::Value) {
    let mut out = std::io::stdout();
    let _ = writeln!(out, "{}", v);
    let _ = out.flush();
}

fn log(msg: impl AsRef<str>) {
    if std::env::var("VINPUT_WS_DEBUG").map(|v| v == "1").unwrap_or(false) {
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/vinput-ws-provider.log")
        {
            use std::io::Write as _;
            let _ = writeln!(f, "[provider] {}", msg.as_ref());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    log("=== provider start (single-connection streaming) ===");

    // Prefer vinput-registry-compliant VINPUT_ASR_* names; also accept the
    // older VINPUT_WS_* names used by earlier local setups.
    let url = std::env::var("VINPUT_ASR_URL")
        .or_else(|_| std::env::var("VINPUT_WS_URL"))
        .unwrap_or_else(|_| DEFAULT_URL.to_string());
    let model = std::env::var("VINPUT_ASR_MODEL")
        .or_else(|_| std::env::var("VINPUT_WS_MODEL"))
        .unwrap_or_else(|_| DEFAULT_MODEL.to_string());

    // Connect once for the whole session.
    let (mut tx, rx) = match asr::connect(&url, 16000, CHUNK_MS).await {
        Ok(p) => p,
        Err(e) => {
            write_event(serde_json::json!({ "type": "error", "message": e.to_string() }));
            write_event(serde_json::json!({ "type": "closed" }));
            return Ok(());
        }
    };

    // WS event reader -> channel.
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<ServerEvent>();
    let mut reader_rx = rx;
    let reader_handle = tokio::spawn(async move {
        loop {
            match reader_rx.read_event().await {
                Ok(ev) => {
                    let stop = matches!(ev, ServerEvent::Error { .. });
                    let _ = event_tx.send(ev);
                    if stop {
                        break;
                    }
                }
                Err(e) => {
                    let _ = event_tx.send(ServerEvent::Error {
                        message: e.to_string(),
                        code: Some("connection_closed".into()),
                    });
                    break;
                }
            }
        }
    });

    write_event(serde_json::json!({ "type": "session_started" }));

    // Wait for session.created.
    match event_rx.recv().await {
        Some(ServerEvent::Created { .. }) => log("got session.created"),
        Some(ServerEvent::Error { message, .. }) => {
            write_event(serde_json::json!({ "type": "error", "message": message }));
            write_event(serde_json::json!({ "type": "closed" }));
            let _ = tx.close().await;
            return Ok(());
        }
        _ => {
            log("unexpected first event");
        }
    }
    if let Err(e) = tx.update_model(&model).await {
        log(&format!("update_model err: {e}"));
    }
    if let Err(e) = tx.start_generation().await {
        log(&format!("start_generation err: {e}"));
        write_event(serde_json::json!({ "type": "error", "message": e.to_string() }));
        write_event(serde_json::json!({ "type": "closed" }));
        return Ok(());
    }
    log("start_generation sent");

    // stdin lines -> channel.
    let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let stdin_handle = tokio::task::spawn_blocking(move || {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let _ = stdin_tx.send(line.clone());
                }
                Err(_) => break,
            }
        }
        let _ = stdin_tx.send("__STDIN_EOF__".to_string());
    });

    let mut cleaner = StreamCleaner::new();
    let mut main_loop_done = false;
    let mut saw_final = false;
    let mut final_sent = false;
    let mut pending_commit = false;

    'outer: loop {
        // Event-first: drain all pending WS events before reading stdin,
        // so transcription.done/delta are never starved by a busy stdin.
        loop {
            match event_rx.try_recv() {
                Ok(ev) => match ev {
                    ServerEvent::Delta { text } => {
                        let clean = cleaner.push_delta(&text);
                        if !clean.is_empty() {
                            write_event(serde_json::json!({ "type": "partial", "text": clean }));
                        }
                    }
                    ServerEvent::Done { text, .. } => {
                        saw_final = true;
                        let t = if text.is_empty() {
                            cleaner.current()
                        } else {
                            strip_prefix(&text)
                        };
                        if !t.is_empty() && !final_sent {
                            final_sent = true;
                            write_event(serde_json::json!({ "type": "final", "text": t }));
                        }
                        break 'outer;
                    }
                    ServerEvent::Error { message, .. } => {
                        write_event(serde_json::json!({ "type": "error", "message": message }));
                        break 'outer;
                    }
                    _ => {}
                },
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    break 'outer;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            }
        }

        if main_loop_done {
            // We already sent final commit; keep draining events until Done,
            // bounded by DONE_TIMEOUT.
            let drain_deadline = tokio::time::Instant::now() + DONE_TIMEOUT;
            loop {
                let remaining = drain_deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, event_rx.recv()).await {
                    Ok(Some(ServerEvent::Delta { text })) => {
                        let clean = cleaner.push_delta(&text);
                        if !clean.is_empty() {
                            write_event(serde_json::json!({ "type": "partial", "text": clean }));
                        }
                    }
                    Ok(Some(ServerEvent::Done { text, .. })) => {
                        saw_final = true;
                        let t = if text.is_empty() {
                            cleaner.current()
                        } else {
                            strip_prefix(&text)
                        };
                        if !t.is_empty() && !final_sent {
                            final_sent = true;
                            write_event(serde_json::json!({ "type": "final", "text": t }));
                        }
                        break 'outer;
                    }
                    Ok(Some(ServerEvent::Error { message, .. })) => {
                        write_event(serde_json::json!({ "type": "error", "message": message }));
                        break 'outer;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            break 'outer;
        }

        // Read one stdin line.
        match stdin_rx.recv().await {
            Some(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if trimmed == "__STDIN_EOF__" {
                    // Abnormal EOF: still flush tail.
                    if let Err(e) = tx.finish().await {
                        log(&format!("finish err: {e}"));
                    }
                    main_loop_done = true;
                    continue;
                }
                let event: serde_json::Value = match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let etype = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
                match etype {
                    "audio" => {
                        let b64 = event.get("audio_base64").and_then(|v| v.as_str()).unwrap_or("");
                        let commit_flag = event.get("commit").and_then(|v| v.as_bool()).unwrap_or(false);
                        if let Ok(pcm) = base64::engine::general_purpose::STANDARD.decode(b64) {
                            if !pcm.is_empty() {
                                if let Err(e) = tx.append_bytes(&pcm).await {
                                    log(&format!("append err: {e}"));
                                    write_event(serde_json::json!({ "type": "error", "message": e.to_string() }));
                                    break 'outer;
                                }
                                pending_commit = true;
                            }
                        }
                        if commit_flag {
                            // This audio block is the final chunk; commit now.
                            if pending_commit {
                                if let Err(e) = tx.finish().await {
                                    log(&format!("audio commit err: {e}"));
                                }
                                pending_commit = false;
                            }
                        }
                    }
                    "finish" => {
                        if pending_commit {
                            if let Err(e) = tx.finish().await {
                                log(&format!("finish err: {e}"));
                            }
                            pending_commit = false;
                        }
                        main_loop_done = true;
                    }
                    "cancel" => break 'outer,
                    _ => {}
                }
            }
            None => break 'outer,
        }
    }

    if !final_sent && saw_final {
        // Fallback: emit accumulated cleaned text if we have any.
        let t = cleaner.current().trim().to_string();
        if !t.is_empty() {
            final_sent = true;
            write_event(serde_json::json!({ "type": "final", "text": t }));
        }
    }

    write_event(serde_json::json!({ "type": "closed" }));
    log("sent closed");
    let _ = tx.close().await;
    let _ = stdin_handle.await;
    let _ = reader_handle.await;
    log("=== provider end ===");
    Ok(())
}
