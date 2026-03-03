# Local AI Server Improvements (GPU Host) — Progress Log

Branch: `local-ai-server-improvements`  
Primary host: `root@10.44.0.103`  
Scope: Local (no cloud) two-way telephony conversation quality/latency.

## Problem Statement

Whisper-family STT backends (`faster_whisper`, `whisper_cpp`) are batch models. The previous Local AI Server implementation was emitting “final” transcripts on a fixed ~1s buffer, which:

- Chopped utterances into unnatural fragments (poor coherence).
- Increased turn-taking errors (LLM responds to partial thoughts).
- Increased the chance of punctuation-only “finals” (e.g., `"?"`) reaching downstream logic.

## Changes Implemented

### 1) Admin UI: model guidance + correct “Installed” detection

- File: `admin_ui/frontend/src/pages/System/ModelsPage.tsx`
- Adds: description + notes + recommended badge (from models catalog).
- Fixes: “Installed” detection false positives (previously used substring match, which could match unrelated files like TTS model names).
- Commit (already pushed): `c119ba12 ui: improve STT model guidance and install detection`

### 2) Local AI Server: shared Whisper-family telephony segmenter (utterance endpointer)

Files:

- `local_ai_server/server.py`
  - Replaces “1s chunk => final transcript” for `faster_whisper` and `whisper_cpp`.
  - Adds energy-based utterance segmentation with preroll + silence endpointer.
  - Runs one-shot decode per utterance using backend `transcribe_pcm16()`.
  - Adds `_whisper_cpp_lock` to avoid concurrent Whisper.cpp access.
- `local_ai_server/stt_backends.py`
  - Adds `transcribe_pcm16(pcm16_16khz_mono: bytes) -> str` for both backends.
- `local_ai_server/config.py`
  - Adds telephony segmentation knobs (separate from `LOCAL_STT_IDLE_MS`).
- `local_ai_server/session.py`
  - Adds per-call segmentation state.

### New Env Knobs (Local AI Server)

- `LOCAL_STT_SEGMENT_ENERGY_THRESHOLD` (default `1200`)
- `LOCAL_STT_SEGMENT_PREROLL_MS` (default `200`)
- `LOCAL_STT_SEGMENT_MIN_MS` (default `250`)
- `LOCAL_STT_SEGMENT_SILENCE_MS` (default `500`)
- `LOCAL_STT_SEGMENT_MAX_MS` (default `12000`)

## Rationale / Expected Impact

- Fewer STT “finals”, but each “final” is a complete utterance (better coherence).
- Lower turn-taking jitter (LLM gets full thought, not 1s fragments).
- Better multilingual baseline because segmentation is language-agnostic and telephony-friendly.

## Deployment Notes (Git-tracked only)

On `10.44.0.103`:

- Pull tracked branch: `local-ai-server-improvements`
- Rebuild `local_ai_server` (image-baked code; not bind-mounted like `ai_engine`).
- Restart `local_ai_server` container.

## Test Calls / Observations (references)

- `1772235703.109` — whisper.cpp call where conversation was incoherent / agent “not hearing”.
- Future: after deployment, re-run this scenario with identical model combo to compare.

## Rollback

- On host, reset to previous known good commit, rebuild `local_ai_server`, restart.

## 2026-02-28 — UI/Config Alignment Pass

### Goals

- Align Admin UI env keys with Local AI Server runtime keys.
- Expose previously hidden runtime knobs for STT/TTS/LLM.
- Preserve backward compatibility for older `.env` keys.

### Changes

- `admin_ui/frontend/src/pages/System/EnvPage.tsx`
  - Fixed STT idle key mapping to canonical `LOCAL_STT_IDLE_MS` (with UI fallback read for legacy `LOCAL_STT_IDLE_TIMEOUT_MS`).
  - Fixed Whisper.cpp model path mapping to canonical `WHISPER_CPP_MODEL_PATH` (with legacy read fallback in UI).
  - Added Whisper segmentation controls for whisper-family STT:
    - `LOCAL_STT_SEGMENT_ENERGY_THRESHOLD`
    - `LOCAL_STT_SEGMENT_SILENCE_MS`
    - Advanced: `LOCAL_STT_SEGMENT_PREROLL_MS`, `LOCAL_STT_SEGMENT_MIN_MS`, `LOCAL_STT_SEGMENT_MAX_MS`
  - Added missing Whisper.cpp language control: `WHISPER_CPP_LANGUAGE`.
  - Added missing Kokoro controls: `KOKORO_LANG`, `KOKORO_API_MODEL`.
  - Added missing advanced LLM controls:
    - `LOCAL_LLM_GPU_LAYERS_AUTO_DEFAULT`
    - `LOCAL_TOOL_GATEWAY_ENABLED`
    - `LOCAL_LLM_SYSTEM_PROMPT`
    - `LOCAL_LLM_STOP_TOKENS`
  - Added tooltips for all newly exposed options.
- `local_ai_server/config.py`
  - Added compatibility alias for Whisper.cpp path:
    - `WHISPER_CPP_MODEL_PATH` OR legacy `LOCAL_WHISPER_CPP_MODEL_PATH`
  - Added compatibility alias for STT idle timeout:
    - `LOCAL_STT_IDLE_MS` OR legacy `LOCAL_STT_IDLE_TIMEOUT_MS`

### Validation

- `python3 -m py_compile local_ai_server/config.py` passed.
- `npm --prefix admin_ui/frontend run build` passed.

## 2026-02-28 — Local UI Tooltip Coverage Pass

### Goal

- Improve operator guidance by adding explicit tooltips for Local AI STT/TTS/LLM tuning controls exposed in Admin UI.

### Changes

- `admin_ui/frontend/src/pages/System/EnvPage.tsx`
  - Added tooltip text for:
    - STT advanced segmentation toggle (`stt-segment-advanced`)
    - Kroko controls (`Embedded Mode`, `Kroko Model Path`, `Kroko Port`, `Kroko URL`, `Kroko Language`)
    - Kokoro advanced mode toggle (`kokoro-advanced`)
    - LLM base controls (`LOCAL_LLM_MODEL_PATH`, `LOCAL_LLM_CONTEXT`, `LOCAL_LLM_BATCH`, `LOCAL_LLM_MAX_TOKENS`, `LOCAL_LLM_TEMPERATURE`, `LOCAL_LLM_THREADS`, `LOCAL_LLM_INFER_TIMEOUT_SEC`)
    - LLM advanced switches (`LOCAL_LLM_USE_MLOCK`, `LOCAL_TOOL_GATEWAY_ENABLED`)
  - Replaced plain labels with tooltip-enabled `FormLabel` for:
    - `LOCAL_LLM_SYSTEM_PROMPT`
    - `LOCAL_LLM_VOICE_PREAMBLE`
  - Added textarea IDs for accessible `htmlFor` bindings.

### Validation

- `npm --prefix admin_ui/frontend run build` passed.

## 2026-02-28 — Local AI WS Protocol Docs Refresh

### Goal

- Bring `docs/local-ai-server/PROTOCOL.md` in sync with latest Local AI Server behavior (barge-in, tool gateway, segmentation, status/switch payloads, env tuning).

### Changes

- `docs/local-ai-server/PROTOCOL.md`
  - Updated source-of-truth references to `ws_protocol.py`, `server.py`, `control_plane.py`, `status_builder.py`, and `protocol_contract.py`.
  - Added/expanded protocol coverage for:
    - `barge_in` / `barge_in_ack`
    - `llm_tool_request` / `llm_tool_response`
    - `backends` / `backend_schema` introspection
  - Updated `status_response` example to include modern fields (`gpu`, `config.runtime_mode`, LLM `prompt_fit`, `tool_capability`, etc.).
  - Expanded `switch_model` docs to include nested `stt_config` / `tts_config` / `llm_config` payloads and `chat_format` hot-reload support.
  - Corrected STT idle default to `LOCAL_STT_IDLE_MS=5000` and documented Whisper segmentation env knobs.
  - Replaced outdated generic `type=error` envelope section with current behavior-specific error contract.
  - Refreshed performance section using recent community GPU test references.
- `docs/local-ai-server/protocol.schema.json`
  - Regenerated from `local_ai_server/protocol_contract.py` to sync schema with current contract (including LLM tool gateway types).

### Validation

- `python3 local_ai_server/protocol_contract.py --write-schema docs/local-ai-server/protocol.schema.json` passed.
