# vinput-ws-asr

Rust realtime WebSocket ASR streaming provider for vinput.

It connects to any service exposing an OpenAI-Realtime-style ASR WebSocket API
(such as vLLM `/v1/realtime`), streams PCM incrementally, and forwards cleaned
transcription results back to vinput.

## Features

- Single WebSocket connection, continuous streaming (no segmentation/reconnect).
- Concurrent send/receive; `transcription.delta` is emitted as `partial` live.
- Strips Qwen3-ASR `language {lang}<asr_text>` model prefixes and joins segments.
- `final` is emitted exactly once; `session_started` is emitted exactly once.
- Pure Rust, no Python/runtime dependencies beyond the compiled binary.
- Env-configurable endpoint/model/debug via `VINPUT_ASR_*`.

## Build

```bash
# Reproducible Nix build (pinned flake toolchain + Cargo.lock):
nix build .#
# The package version is read from Cargo.toml, so rebuilding after a version
# bump produces a correctly-versioned derivation.

# 或者直接 checkout 最新提交构建当前 main（需要能访问 GitHub）：
git pull --ff-only
nix build --refresh github:yjymosheng/rust-vinput-ws-asr

# Output: ./result/bin/vinput-ws-provider

# Or use the dev shell and cargo directly:
nix develop --offline
cargo build --release
```

## Test

```bash
cargo test --release
```

## Run (vinput provider)

The binary speaks vinput's command-streaming JSONL protocol over stdin/stdout:

```bash
VINPUT_ASR_URL=ws://127.0.0.1:7000/v1/realtime \
VINPUT_ASR_MODEL=qwen3-asr \
target/release/vinput-ws-provider
```

## vinput Protocol

stdin:

```json
{"type":"audio","audio_base64":"...","commit":false}
{"type":"audio","audio_base64":"...","commit":true}
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

`audio_base64` is mono `S16_LE` PCM at 16 kHz.

## Environment Variables

- `VINPUT_ASR_URL` (fallback `VINPUT_WS_URL`): WebSocket endpoint, e.g. `ws://127.0.0.1:7000/v1/realtime`.
- `VINPUT_ASR_MODEL` (fallback `VINPUT_WS_MODEL`): served model name, e.g. `qwen3-asr`.
- `VINPUT_ASR_DEBUG` (fallback `VINPUT_WS_DEBUG`): enable debug log to `/tmp/vinput-ws-provider.log` when set to `1`/`true`.

`VINPUT_ASR_*` names follow the vinput-registry provider env convention.

## Protocol Notes

- A non-final `input_audio_buffer.commit` (`final:false`) is required to start
  generation on vLLM realtime.
- The provider accepts `commit:true` on the final audio block and will not send
  a second final commit on `finish`.
- `session.created` / `session.updated` are both treated as readiness signals.
- The provider is intentionally model-agnostic; it does not modify or patch
  the ASR server.

## Keeping up with the latest upstream

This repository is the provider itself. To use the newest `main` instead of
a previously locked revision, choose one of the options below:

### Option A: rebuild from the local checkout (after `git pull`)

```bash
git pull --ff-only
nix build .#
```

### Option B: update the flake input in your NixOS configuration

In a NixOS configuration, the input already points at
`github:yjymosheng/rust-vinput-ws-asr`. To bring it to the latest main:

```bash
cd /path/to/nixos
nix flake lock --update-input vinput-ws-asr
nixos-rebuild switch --flake .#
```

This updates `flake.lock` to the newest commit of `main` and then rebuilds/installs
the newest `vinput-ws-provider` binary into the system store.

### Version source

- Crate version: `Cargo.toml` `[package].version` (currently `0.1.0`).
- Nix derivation: reads the same value via `builtins.fromTOML (builtins.readFile ./Cargo.toml)`.

Bump `Cargo.toml` only; `flake.nix` follows automatically.
