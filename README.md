# vinput-ws-asr

Rust realtime WebSocket ASR streaming provider for vLLM `/v1/realtime` (Qwen3-ASR).

## 功能
- 一条 WebSocket 连接，持续推流（不做分段/重连）。
- 边发边收：实时收到 `transcription.delta` 即输出 `partial`。
- 自动剥离模型前缀 `language {lang}<asr_text>`，多段换行合并。
- vinput `finish` 后输出最终文本 `final`，然后 `closed`。

## vinput 协议
stdin:
```json
{"type":"audio","audio_base64":"...","commit":false}
{"type":"finish"}
{"type":"cancel"}
```
stdout:
```json
{"type":"session_started"}
{"type":"partial","text":"..."}
{"type":"final","text":"..."}
{"type":"error","message":"..."}
{"type":"closed"}
```

## 构建
```bash
nix develop --offline   # 或使用仓库内 nix 工具链
cargo build --bin vinput-ws-provider
```

## 运行
```bash
VINPUT_WS_URL=ws://192.168.102.10:7000/v1/realtime \
VINPUT_WS_MODEL=qwen3-asr \
target/debug/vinput-ws-provider
```

## 依赖
- tokio
- tokio-tungstenite
- futures-util
- serde / serde_json
- base64

## Environment Variables

- `VINPUT_ASR_URL` (fallback `VINPUT_WS_URL`): WebSocket endpoint, e.g. `ws://192.168.102.10:7000/v1/realtime`
- `VINPUT_ASR_MODEL` (fallback `VINPUT_WS_MODEL`): served model name, e.g. `qwen3-asr`
- `VINPUT_ASR_DEBUG` (fallback `VINPUT_WS_DEBUG`): set to `1` to enable debug logging to `/tmp/vinput-ws-provider.log`

The `VINPUT_ASR_*` names follow the vinput-registry provider env convention.
