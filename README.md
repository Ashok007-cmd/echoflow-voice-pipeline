# EchoFlow Voice Pipeline

> Real-time streaming voice assistant — Microphone → ASR → LLM → TTS → Speaker

[![CI](https://github.com/Ashok007-cmd/echoflow-voice-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Ashok007-cmd/echoflow-voice-pipeline/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%20|%203.12-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-blue.svg)](https://github.com/Ashok007-cmd/echoflow-voice-pipeline/pkgs/container/echoflow-voice-pipeline)
[![Security Policy](https://img.shields.io/badge/security-policy-brightgreen.svg)](SECURITY.md)

EchoFlow is a **production-grade, asyncio-based real-time voice assistant pipeline** that chains automatic speech recognition, large language models, and speech synthesis into a single low-latency streaming loop.

LLM tokens stream directly into TTS as they arrive — **audio begins playing before the full response is generated**, achieving sub-second perceived response times even with multi-sentence answers.

---

## Key Highlights

| Capability | Detail |
|---|---|
| **Streaming pipeline** | Three concurrent asyncio tasks share queues — LLM tokens flow directly into TTS |
| **Multi-backend** | 4 ASR, 3 LLM, 3 TTS backends — swap at runtime, no code changes needed |
| **Resilience** | Circuit breakers, per-stage timeouts, auto fallback chains, graceful degradation |
| **Observability** | Nanosecond-precision P50/P90/P95/P99 latency tracking, SQLite persistence, matplotlib charts |
| **Dynamic VAD** | RMS energy + Zero Crossing Rate speech detection — more robust than energy alone |
| **Interrupt support** | New speech cancels in-flight playback immediately (barge-in) |
| **Offline mode** | Full mock backends + local Whisper — runs completely without API keys |

---

## Architecture

```
Microphone ──► ASR Engine ──► LLM Client ──► TTS Engine ──► Speaker
                   │               │               │
                   ▼               ▼               ▼
              Transcription   Streaming Tokens  Audio Chunks
             (Dynamic VAD)   (sentence-split)  (queued play)
                   │               │               │
                   └───────────────┴───────────────┘
                                   │
                           ┌───────▼────────┐
                           │ Latency Tracker │
                           │ SQLite  · PNG   │
                           └────────────────┘
```

**Per conversational turn**, three concurrent asyncio tasks run as a pipeline:

1. **LLM worker** — streams tokens, splits on sentence boundaries, pushes text chunks to `_sentence_queue`
2. **TTS synthesizer** — pulls sentences from `_sentence_queue`, synthesizes audio, pushes to `_audio_queue`
3. **Playback worker** — pulls audio from `_audio_queue`, plays via sounddevice

No stage waits for the previous one to finish. Audio begins as soon as the first sentence is ready.

---

## Features

### Streaming Pipeline
- Concurrent ASR → LLM → TTS execution via `asyncio.Queue`
- Sentence-boundary splitting: TTS begins on the first complete sentence while LLM generates the rest
- **Dynamic VAD** using RMS energy + Zero Crossing Rate with adaptive noise floor tracking
- **Barge-in / interrupt**: new speech instantly cancels in-flight LLM and TTS tasks
- Backend pre-warming on startup to reduce first-turn latency

### Multiple Backends

| Stage | Backend | Notes |
|-------|---------|-------|
| ASR | Google Web Speech | Free, requires internet |
| ASR | OpenAI Whisper API | `whisper-1`, needs `OPENAI_API_KEY` |
| ASR | Local Whisper | HuggingFace `openai/whisper-tiny` (fully offline) |
| ASR | Mock | Instant, zero dependencies |
| LLM | OpenAI | `gpt-4o-mini` default, needs `OPENAI_API_KEY` |
| LLM | Anthropic Claude | `claude-3-5-haiku-latest`, needs `ANTHROPIC_API_KEY` |
| LLM | Mock | Instant, zero dependencies |
| TTS | Microsoft Edge TTS | Neural voices, `en-US-JennyNeural` default |
| TTS | pyttsx3 | Fully offline, no internet required |
| TTS | Mock | Silent, zero dependencies |

### Resilience
- **Circuit breakers** (CLOSED → OPEN → HALF_OPEN → CLOSED) per service with configurable thresholds
- **Automatic fallback chains**: ASR (primary → Google → Mock), LLM (primary → OpenAI → Anthropic → Mock), TTS (primary → pyttsx3 → Mock)
- **Per-stage timeouts**: ASR 10s, LLM 15s, TTS 10s, E2E 40s
- **Graceful degradation**: canned text response when all audio fails
- Client errors (auth failures, 404s, rate limits) are excluded from failure counts — they won't trip the circuit breaker

### Observability
- **Nanosecond-precision** latency measurement per stage (ASR, LLM TTFT, LLM total, TTS, playback, E2E)
- **Rolling window statistics**: mean, median, P50/P90/P95/P99, min, max, std dev
- **SQLite persistence** — metrics survive restarts; path configurable via `ECHOFLOW_DB_PATH`
- **matplotlib chart** — waterfall breakdown, per-turn timeline, percentile comparison
- **Health check** endpoint exposing circuit breaker states and degradation level

---

## Quickstart

### Prerequisites

- Python 3.11 or 3.12
- PortAudio (for microphone capture)

```bash
# Debian / Ubuntu
sudo apt install portaudio19-dev

# macOS
brew install portaudio
```

### Install

```bash
git clone https://github.com/Ashok007-cmd/echoflow-voice-pipeline.git
cd echoflow-voice-pipeline

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -e .
```

For local Whisper inference (downloads ~75 MB model on first run):

```bash
pip install -e ".[local]"
```

### Configure

```bash
cp .env.example .env
# Edit .env — add your API keys, choose backends
```

### Try it immediately (no API keys, no microphone)

```bash
voice-assistant benchmark --turns 10 --mock
```

Example output:
```
════════ Latency Benchmark ════════
Turns: 10  |  ASR: mock  |  LLM: mock  |  TTS: mock

 Turn    ASR(ms)   TTFT(ms)   LLM(ms)   TTS(ms)    E2E(ms)       Status
──────────────────────────────────────────────────────────────────────
    1      100.3      524.8     660.2     100.3      764.5            OK
    2      100.2      522.1     656.8     100.1      760.3            OK
   ...
```

### Run with real backends

```bash
voice-assistant interactive \
  --asr-backend google \
  --llm-backend openai \
  --tts-backend edge_tts \
  --report
```

---

## Configuration

All settings load from environment variables or `.env`. CLI flags override env vars for a single run.

| Variable | Default | Description |
|----------|---------|-------------|
| `ASR_BACKEND` | `google` | `google` · `openai` · `local_whisper` · `mock` |
| `LLM_BACKEND` | `openai` | `openai` · `anthropic` · `mock` |
| `TTS_BACKEND` | `edge_tts` | `edge_tts` · `pyttsx3` · `mock` |
| `OPENAI_API_KEY` | — | Required for OpenAI ASR and LLM backends |
| `ANTHROPIC_API_KEY` | — | Required for Anthropic LLM backend |
| `SYSTEM_PROMPT` | *(built-in)* | Override the assistant system prompt |
| `ECHOFLOW_DB_PATH` | `~/.local/share/echoflow/metrics.db` | Metrics database location |

---

## CLI Reference

### `interactive` — live voice assistant

```
voice-assistant interactive [OPTIONS]

  --asr-backend TEXT    ASR backend: google, openai, local_whisper, mock
  --llm-backend TEXT    LLM backend: openai, anthropic, mock
  --tts-backend TEXT    TTS backend: edge_tts, pyttsx3, mock
  --mock                Force all backends to mock mode (offline demo)
  --timeout FLOAT       Per-stage timeout in seconds
  --report              Write latency_report.png on exit
  -v, --verbose         Debug logging
```

Live output per turn:
```
User:      what's the weather like today?
Assistant: I don't have real-time access, but I can help with...
Latency:   ASR=312ms | LLM TTFT=187ms | TTS=94ms | E2E=593ms
```

### `benchmark` — synthetic latency measurement

```
voice-assistant benchmark [OPTIONS]

  -n, --turns INTEGER   Number of turns (default: 10)
  --mock                All-mock backends (offline, no keys needed)
  --mock-asr / --mock-llm / --mock-tts   Mix real and mock backends
  --output PATH         Chart output path (default: latency_report.png)
  -v, --verbose         Debug logging
```

### `report` — visualise saved metrics

```
voice-assistant report [METRICS_FILE] [OPTIONS]

  --output PATH         Chart output path (default: latency_report.png)
```

---

## Docker

### Pull from GitHub Container Registry

```bash
docker pull ghcr.io/ashok007-cmd/echoflow-voice-pipeline:latest

# Offline benchmark — no setup required
docker run --rm ghcr.io/ashok007-cmd/echoflow-voice-pipeline:latest \
  benchmark --turns 5 --mock

# With API keys
docker run --rm \
  -e OPENAI_API_KEY=sk-... \
  ghcr.io/ashok007-cmd/echoflow-voice-pipeline:latest \
  benchmark --turns 10
```

### Build locally

```bash
docker build -t echoflow .
docker run --rm echoflow benchmark --turns 5 --mock
```

The default `CMD` runs `benchmark --turns 5 --mock` — no environment setup required.

---

## Testing

```bash
pip install -e ".[dev]"

# Full suite
.venv/bin/python -m pytest

# With coverage
.venv/bin/python -m pytest --cov=src --cov-report=term-missing

# Fast smoke test (mock only)
.venv/bin/python -m pytest tests/test_pipeline.py -k "mock" -v
```

All tests use mock backends — **no API keys, microphone, or audio hardware required**.

Test coverage includes:
- All 3 ASR backends (Google, OpenAI Whisper, Local Whisper) with network calls patched
- All 3 LLM backends (OpenAI streaming, Anthropic streaming, Mock) with mock chunk iterators
- All 3 TTS backends (Edge TTS, pyttsx3, Mock) including subprocess playback and temp-file cleanup
- ASR streaming with VAD, timeout, and exception paths
- Circuit breaker state machine: CLOSED → OPEN → HALF_OPEN → CLOSED with recovery
- Fallback chain (primary succeeds, primary fails + fallback succeeds, all fail)
- Timeout decorator and `with_timeout` helper (success, raise, fallback-value paths)
- Degradation manager level tracking and alert callbacks
- Latency tracker (rolling window, percentiles, multi-stage, turn tracking)
- Pipeline orchestrator integration (start/stop, multi-turn, LLM circuit breaker trip)
- Metrics database write and query
- CLI commands via Click test runner

---

## Project Structure

```
src/
├── main.py                  # CLI entry point (interactive / benchmark / report)
├── config.py                # Typed dataclass config, env-var loading, validation
├── pipeline/
│   ├── orchestrator.py      # Async pipeline coordinator — concurrent queue tasks
│   ├── asr.py               # ASR engines + DynamicVAD (RMS + ZCR)
│   ├── llm_client.py        # LLM streaming clients (OpenAI / Anthropic / Mock)
│   ├── tts.py               # TTS engines (Edge TTS / pyttsx3 / Mock)
│   └── models.py            # Shared dataclasses (AudioChunk, TurnMetrics, …)
├── monitoring/
│   ├── latency_tracker.py   # Nanosecond-precision stage timers + rolling stats
│   ├── visualizer.py        # matplotlib waterfall, timeline, percentile charts
│   └── database.py          # Background-thread SQLite writer (WAL mode)
└── resilience/
    ├── circuit_breaker.py   # CLOSED / OPEN / HALF_OPEN state machine
    ├── timeout.py           # Per-stage asyncio.wait_for wrappers + decorator
    └── degradation.py       # Level tracking (NONE → FALLBACK → TEXT_ONLY → OFFLINE)
tests/
├── test_backends.py         # ASR / LLM / TTS backend unit tests
├── test_pipeline.py         # Orchestrator, resilience, monitoring integration tests
├── test_cli.py              # CLI command tests
└── conftest.py              # Shared fixtures
```

---

## How the Pipeline Works

1. **Audio capture** — PyAudio streams 16 kHz PCM chunks from the microphone (falls back to a synthetic generator if no hardware is found).
2. **Dynamic VAD** — `DynamicVAD` classifies each chunk using RMS energy (with adaptive noise floor) and Zero Crossing Rate. Once silence exceeds `silence_threshold` (0.5 s default) after speech, the utterance boundary is emitted.
3. **ASR** — The utterance is sent to the configured engine (Google / OpenAI Whisper / Local Whisper / Mock), wrapped in a circuit breaker and per-stage timeout. On failure, the fallback chain (→ Google → Mock) activates automatically.
4. **LLM streaming** — The transcript is sent to the LLM. Tokens stream in and are buffered into sentences using punctuation and word-count splitting. On failure, the fallback chain (→ OpenAI → Anthropic → Mock) activates.
5. **TTS** — Each sentence is synthesized as soon as it is ready. The TTS worker runs concurrently with the LLM worker via `asyncio.Queue`.
6. **Playback** — Audio chunks queue into a third worker that calls `engine.play()` as they arrive.
7. **Interrupt** — When new speech is detected, `orchestrator.interrupt()` cancels the active playback task and drains both queues before the next turn begins.

---

## Resilience Design

```
Service call ──► Circuit Breaker ──► Timeout guard ──► Engine
                      │
              CLOSED (normal)
                      │ N failures ≥ threshold
              OPEN (rejecting) ──► recovery_timeout elapsed ──► HALF_OPEN
                                                                    │
                                                            probe succeeds ──► CLOSED
                                                            probe fails   ──► OPEN
```

Circuit breakers are per-service (ASR, LLM, TTS) and independent. Client errors (auth failures, 404s, rate limits) do **not** count toward the failure threshold — only genuine service outages trip the breaker.

---

## Contributing

Contributions are welcome. Please open an issue before submitting large changes.

```bash
# Install dev tools
pip install -e ".[dev]"

# Run tests before submitting
.venv/bin/python -m pytest

# Lint
ruff check src/ tests/
```

---

## License

[MIT](LICENSE)
