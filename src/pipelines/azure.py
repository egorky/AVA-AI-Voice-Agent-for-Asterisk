"""
Microsoft Azure Speech Service component adapters for configurable pipelines.

This module provides REST-based STT and TTS adapters that integrate with
Azure Cognitive Services Speech API. All communication uses REST over HTTPS;
no Azure SDK dependency is required.

Adapters:
  - AzureSTTFastAdapter    (azure_stt_fast)   — Fast Transcription REST API
  - AzureSTTRealtimeAdapter (azure_stt_realtime) — Real-Time STT REST API
  - AzureTTSAdapter        (azure_tts)        — Text-to-Speech SSML REST API

Reference:
  STT Fast:    https://learn.microsoft.com/azure/ai-services/speech-service/fast-transcription-create
  STT RT:      https://learn.microsoft.com/azure/ai-services/speech-service/how-to-recognize-speech?pivots=programming-language-rest
  TTS:         https://learn.microsoft.com/azure/ai-services/speech-service/get-started-text-to-speech?pivots=programming-language-rest
"""

from __future__ import annotations

import json
import time
import uuid
import wave
from io import BytesIO
from typing import Any, AsyncIterator, Callable, Dict, Iterable, Optional

import aiohttp

from ..audio import convert_pcm16le_to_target_format, resample_audio
from ..config import AppConfig, AzureSTTProviderConfig, AzureTTSProviderConfig
from ..logging_config import get_logger
from .base import STTComponent, TTSComponent

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _merge_dicts(base: Dict[str, Any], override: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = dict(base or {})
    if override:
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = _merge_dicts(merged[key], value)
            elif value is not None:
                merged[key] = value
    return merged


def _bytes_per_sample(encoding: str) -> int:
    fmt = (encoding or "").lower()
    if fmt in ("ulaw", "mulaw", "mu-law", "alaw"):
        return 1
    return 2  # pcm16, slin, slin16


def _chunk_audio(audio_bytes: bytes, encoding: str, sample_rate: int, chunk_ms: int) -> Iterable[bytes]:
    if not audio_bytes:
        return
    bytes_per_sample = _bytes_per_sample(encoding)
    frame_size = max(bytes_per_sample, int(sample_rate * (chunk_ms / 1000.0) * bytes_per_sample))
    for idx in range(0, len(audio_bytes), frame_size):
        yield audio_bytes[idx: idx + frame_size]


def _pcm16le_to_wav(audio_pcm16: bytes, sample_rate_hz: int) -> bytes:
    """Wrap raw PCM16-LE bytes in a proper WAV container."""
    buf = BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(int(sample_rate_hz))
        wf.writeframes(audio_pcm16)
    return buf.getvalue()


def _wav_to_pcm16le(wav_bytes: bytes) -> tuple[bytes, int]:
    """Extract raw PCM16-LE frames and sample rate from a WAV container."""
    try:
        with wave.open(BytesIO(wav_bytes), "rb") as wf:
            frames = wf.readframes(wf.getnframes())
            rate = wf.getframerate()
        return frames, int(rate)
    except Exception as exc:
        raise RuntimeError(f"Azure: failed to decode WAV response: {exc}") from exc


def _build_azure_stt_fast_url(region: str) -> str:
    return f"https://{region}.api.cognitive.microsoft.com/speechtotext/transcriptions:transcribe?api-version=2025-10-01"


def _build_azure_stt_realtime_url(region: str, language: str) -> str:
    return (
        f"https://{region}.stt.speech.microsoft.com"
        f"/speech/recognition/conversation/cognitiveservices/v1"
        f"?language={language}"
    )


def _build_azure_tts_url(region: str) -> str:
    return f"https://{region}.tts.speech.microsoft.com/cognitiveservices/v1"


def _make_stt_headers(api_key: str) -> Dict[str, str]:
    return {
        "Ocp-Apim-Subscription-Key": api_key,
        "User-Agent": "AVA-AI-Voice-Agent/1.0",
    }


def _make_tts_headers(api_key: str, output_format: str) -> Dict[str, str]:
    return {
        "Ocp-Apim-Subscription-Key": api_key,
        "Content-Type": "application/ssml+xml",
        "X-Microsoft-OutputFormat": output_format,
        "User-Agent": "AVA-AI-Voice-Agent/1.0",
    }


def _build_ssml(
    text: str,
    voice_name: str,
    language: str,
    prosody_pitch: Optional[str] = None,
    prosody_rate: Optional[str] = None,
) -> str:
    """Build a minimal SSML document for Azure TTS, with optional prosody controls."""
    # Derive xml:lang from voice_name locale prefix (e.g. "en-US-JennyNeural" -> "en-US")
    lang = language or "-".join(voice_name.split("-")[:2]) if "-" in voice_name else "en-US"
    safe_text = (
        text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )
    # Build optional <prosody> wrapper
    prosody_attrs = []
    if prosody_pitch:
        prosody_attrs.append(f"pitch='{prosody_pitch}'")
    if prosody_rate:
        prosody_attrs.append(f"rate='{prosody_rate}'")
    if prosody_attrs:
        inner = f"<prosody {' '.join(prosody_attrs)}>{safe_text}</prosody>"
    else:
        inner = safe_text
    return (
        f"<speak version='1.0' xml:lang='{lang}'>"
        f"<voice name='{voice_name}'>{inner}</voice>"
        f"</speak>"
    )


# ---------------------------------------------------------------------------
# Mapping from Azure output_format -> (pcm_native, riff_wrapped)
# This tells us whether the response needs WAV decoding or is raw PCM/mulaw.
# ---------------------------------------------------------------------------
_AZURE_FORMAT_INFO: Dict[str, tuple[str, bool]] = {
    # raw-* formats: no WAV header, native encoding
    "raw-8khz-8bit-mono-mulaw": ("mulaw", False),
    "raw-8khz-8bit-mono-alaw": ("alaw", False),
    "raw-8khz-16bit-mono-pcm": ("pcm16", False),
    "raw-16khz-16bit-mono-pcm": ("pcm16", False),
    "raw-24khz-16bit-mono-pcm": ("pcm16", False),
    # riff-* formats: RIFF/WAV container wrapping PCM
    "riff-8khz-16bit-mono-pcm": ("pcm16", True),
    "riff-16khz-16bit-mono-pcm": ("pcm16", True),
    "riff-24khz-16bit-mono-pcm": ("pcm16", True),
}


def _decode_tts_audio(raw_bytes: bytes, output_format: str) -> tuple[bytes, int, str]:
    """
    Decode Azure TTS response bytes to PCM16-LE (or mulaw) + sample_rate.

    Returns (pcm16_bytes, sample_rate_hz, native_encoding).
    For raw mulaw formats, returns the mulaw bytes directly with encoding='mulaw'.
    For riff/raw PCM formats, returns PCM16 LE bytes with encoding='pcm16'.
    """
    fmt_lower = (output_format or "riff-8khz-16bit-mono-pcm").lower()
    info = _AZURE_FORMAT_INFO.get(fmt_lower)

    if info is None:
        # Unknown/MP3 format — try WAV decode, fall back to raw bytes
        logger.warning("Azure TTS: unknown output_format; attempting WAV decode", output_format=output_format)
        try:
            pcm, rate = _wav_to_pcm16le(raw_bytes)
            return pcm, rate, "pcm16"
        except Exception:
            return raw_bytes, 8000, "unknown"

    native_encoding, is_riff = info

    if native_encoding == "mulaw":
        # raw 8 kHz mulaw — return as-is
        return raw_bytes, 8000, "mulaw"

    if native_encoding == "alaw":
        # We don't handle alaw conversions; return raw bytes at 8 kHz
        return raw_bytes, 8000, "alaw"

    # pcm16 — may be RIFF-wrapped or raw
    if is_riff:
        try:
            pcm, rate = _wav_to_pcm16le(raw_bytes)
            return pcm, rate, "pcm16"
        except Exception as exc:
            raise RuntimeError(f"Azure TTS WAV decode failed for format '{output_format}': {exc}") from exc

    # raw PCM16 — derive sample rate from format name
    if "8khz" in fmt_lower:
        rate = 8000
    elif "16khz" in fmt_lower:
        rate = 16000
    elif "24khz" in fmt_lower:
        rate = 24000
    else:
        rate = 16000
    return raw_bytes, rate, "pcm16"


# ---------------------------------------------------------------------------
# Azure STT — Fast Transcription Adapter
# ---------------------------------------------------------------------------

class AzureSTTFastAdapter(STTComponent):
    """Azure Fast Transcription REST adapter.

    Endpoint: POST {region}.api.cognitive.microsoft.com/speechtotext/transcriptions:transcribe
    Auth:     Ocp-Apim-Subscription-Key header
    Input:    multipart/form-data with 'audio' (WAV) + 'definition' (JSON)
    Output:   JSON { combinedPhrases: [{ text: "..." }], ... }
    """

    def __init__(
        self,
        component_key: str,
        app_config: AppConfig,
        provider_config: AzureSTTProviderConfig,
        options: Optional[Dict[str, Any]] = None,
        *,
        session_factory: Optional[Callable[[], aiohttp.ClientSession]] = None,
    ):
        self.component_key = component_key
        self._app_config = app_config
        self._provider_defaults = provider_config
        self._pipeline_defaults = options or {}
        self._session_factory = session_factory
        self._session: Optional[aiohttp.ClientSession] = None
        self._default_timeout = float(
            self._pipeline_defaults.get("request_timeout_sec", provider_config.request_timeout_sec)
        )

    async def start(self) -> None:
        logger.debug(
            "Azure STT Fast adapter initialized",
            component=self.component_key,
            region=self._provider_defaults.region,
            language=self._provider_defaults.language,
        )

    async def stop(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def open_call(self, call_id: str, options: Dict[str, Any]) -> None:
        await self._ensure_session()

    async def close_call(self, call_id: str) -> None:
        return

    async def validate_connectivity(self, options: Dict[str, Any]) -> Dict[str, Any]:
        merged = self._compose_options(options or {})
        # Inject the computed Azure URL so the base-class URL check passes.
        # The base class only does a lightweight URL-format check for non-primary providers.
        if not merged.get("base_url"):
            region = merged.get("region") or self._provider_defaults.region
            merged["base_url"] = merged.get("fast_stt_base_url") or _build_azure_stt_fast_url(region)
        return await super().validate_connectivity(merged)

    async def transcribe(
        self,
        call_id: str,
        audio_pcm16: bytes,
        sample_rate_hz: int,
        options: Dict[str, Any],
    ) -> str:
        if not audio_pcm16:
            return ""

        await self._ensure_session()
        assert self._session

        merged = self._compose_options(options)
        api_key = merged.get("api_key") or ""
        if not api_key:
            raise RuntimeError("Azure STT Fast requires AZURE_SPEECH_KEY / api_key")

        wav_bytes = _pcm16le_to_wav(audio_pcm16, sample_rate_hz)
        language = str(merged.get("language") or self._provider_defaults.language)
        url = str(merged.get("fast_stt_base_url") or _build_azure_stt_fast_url(merged["region"]))
        timeout_sec = float(merged.get("request_timeout_sec", self._default_timeout))
        request_id = f"azure-stt-fast-{uuid.uuid4().hex[:12]}"

        definition = json.dumps({"locales": [language]})

        form = aiohttp.FormData()
        form.add_field("audio", wav_bytes, filename="audio.wav", content_type="audio/wav")
        form.add_field("definition", definition)

        headers = _make_stt_headers(api_key)
        started_at = time.perf_counter()

        async with self._session.post(
            url,
            data=form,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout_sec),
        ) as resp:
            raw = await resp.read()
            latency_ms = (time.perf_counter() - started_at) * 1000.0
            body_text = raw.decode("utf-8", errors="ignore")

            if resp.status >= 400:
                logger.error(
                    "Azure STT Fast request failed",
                    call_id=call_id,
                    request_id=request_id,
                    status=resp.status,
                    body_preview=body_text[:200],
                )
                raise RuntimeError(
                    f"Azure STT Fast request failed (status {resp.status}): {body_text[:256]}"
                )

        transcript = self._parse_transcript(raw)
        logger.info(
            "Azure STT Fast transcript received",
            call_id=call_id,
            request_id=request_id,
            latency_ms=round(latency_ms, 2),
            transcript_preview=(transcript or "")[:80],
        )
        return transcript or ""

    async def _ensure_session(self) -> None:
        if self._session and not self._session.closed:
            return
        factory = self._session_factory or aiohttp.ClientSession
        self._session = factory()

    @staticmethod
    def _parse_transcript(payload: bytes) -> str:
        try:
            data = json.loads(payload.decode("utf-8"))
        except Exception:
            return payload.decode("utf-8", errors="ignore").strip()

        # Fast transcription response: { combinedPhrases: [{ text: "..." }] }
        combined = data.get("combinedPhrases")
        if combined and isinstance(combined, list) and combined[0].get("text"):
            return str(combined[0]["text"]).strip()

        # Fallback: join all phrase texts
        phrases = data.get("phrases") or []
        texts = [p.get("text", "") for p in phrases if p.get("text")]
        if texts:
            return " ".join(texts).strip()

        return ""

    def _compose_options(self, runtime_options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        runtime_options = runtime_options or {}
        return {
            "api_key": runtime_options.get(
                "api_key",
                self._pipeline_defaults.get("api_key", self._provider_defaults.api_key),
            ),
            "region": runtime_options.get(
                "region",
                self._pipeline_defaults.get("region", self._provider_defaults.region),
            ),
            "fast_stt_base_url": runtime_options.get(
                "fast_stt_base_url",
                self._pipeline_defaults.get("fast_stt_base_url", self._provider_defaults.fast_stt_base_url),
            ),
            "language": runtime_options.get(
                "language",
                self._pipeline_defaults.get("language", self._provider_defaults.language),
            ),
            "request_timeout_sec": float(
                runtime_options.get(
                    "request_timeout_sec",
                    self._pipeline_defaults.get("request_timeout_sec", self._default_timeout),
                )
            ),
        }


# ---------------------------------------------------------------------------
# Azure STT — Real-Time Adapter
# ---------------------------------------------------------------------------

class AzureSTTRealtimeAdapter(STTComponent):
    """Azure Real-Time Speech-to-Text REST adapter.

    Endpoint: POST {region}.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1?language={lang}
    Auth:     Ocp-Apim-Subscription-Key header
    Input:    audio/wav (raw binary body)
    Output:   JSON { RecognitionStatus, DisplayText, ... }
    """

    def __init__(
        self,
        component_key: str,
        app_config: AppConfig,
        provider_config: AzureSTTProviderConfig,
        options: Optional[Dict[str, Any]] = None,
        *,
        session_factory: Optional[Callable[[], aiohttp.ClientSession]] = None,
    ):
        self.component_key = component_key
        self._app_config = app_config
        self._provider_defaults = provider_config
        self._pipeline_defaults = options or {}
        self._session_factory = session_factory
        self._session: Optional[aiohttp.ClientSession] = None
        self._default_timeout = float(
            self._pipeline_defaults.get("request_timeout_sec", provider_config.request_timeout_sec)
        )

    async def start(self) -> None:
        logger.debug(
            "Azure STT Realtime adapter initialized",
            component=self.component_key,
            region=self._provider_defaults.region,
            language=self._provider_defaults.language,
        )

    async def stop(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def open_call(self, call_id: str, options: Dict[str, Any]) -> None:
        await self._ensure_session()

    async def close_call(self, call_id: str) -> None:
        return

    async def validate_connectivity(self, options: Dict[str, Any]) -> Dict[str, Any]:
        merged = self._compose_options(options or {})
        # Inject the computed Azure URL so the base-class URL check passes.
        if not merged.get("base_url"):
            region = merged.get("region") or self._provider_defaults.region
            language = merged.get("language") or self._provider_defaults.language
            merged["base_url"] = merged.get("realtime_stt_base_url") or _build_azure_stt_realtime_url(region, language)
        return await super().validate_connectivity(merged)

    async def transcribe(
        self,
        call_id: str,
        audio_pcm16: bytes,
        sample_rate_hz: int,
        options: Dict[str, Any],
    ) -> str:
        if not audio_pcm16:
            return ""

        await self._ensure_session()
        assert self._session

        merged = self._compose_options(options)
        api_key = merged.get("api_key") or ""
        if not api_key:
            raise RuntimeError("Azure STT Realtime requires AZURE_SPEECH_KEY / api_key")

        language = str(merged.get("language") or self._provider_defaults.language)
        region = str(merged.get("region") or self._provider_defaults.region)
        url = str(
            merged.get("realtime_stt_base_url")
            or _build_azure_stt_realtime_url(region, language)
        )
        timeout_sec = float(merged.get("request_timeout_sec", self._default_timeout))
        request_id = f"azure-stt-rt-{uuid.uuid4().hex[:12]}"

        wav_bytes = _pcm16le_to_wav(audio_pcm16, sample_rate_hz)
        headers = {**_make_stt_headers(api_key), "Content-Type": "audio/wav"}

        started_at = time.perf_counter()
        async with self._session.post(
            url,
            data=wav_bytes,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout_sec),
        ) as resp:
            raw = await resp.read()
            latency_ms = (time.perf_counter() - started_at) * 1000.0
            body_text = raw.decode("utf-8", errors="ignore")

            if resp.status >= 400:
                logger.error(
                    "Azure STT Realtime request failed",
                    call_id=call_id,
                    request_id=request_id,
                    status=resp.status,
                    body_preview=body_text[:200],
                )
                raise RuntimeError(
                    f"Azure STT Realtime request failed (status {resp.status}): {body_text[:256]}"
                )

        transcript = self._parse_transcript(raw)
        logger.info(
            "Azure STT Realtime transcript received",
            call_id=call_id,
            request_id=request_id,
            latency_ms=round(latency_ms, 2),
            transcript_preview=(transcript or "")[:80],
        )
        return transcript or ""

    async def _ensure_session(self) -> None:
        if self._session and not self._session.closed:
            return
        factory = self._session_factory or aiohttp.ClientSession
        self._session = factory()

    @staticmethod
    def _parse_transcript(payload: bytes) -> str:
        try:
            data = json.loads(payload.decode("utf-8"))
        except Exception:
            return payload.decode("utf-8", errors="ignore").strip()

        # Real-time STT response: { RecognitionStatus: "Success", DisplayText: "..." }
        status = data.get("RecognitionStatus", "")
        if status not in ("Success", ""):
            logger.debug("Azure STT Realtime: non-success status", recognition_status=status)
        display_text = data.get("DisplayText", "")
        return str(display_text).strip() if display_text else ""

    def _compose_options(self, runtime_options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        runtime_options = runtime_options or {}
        return {
            "api_key": runtime_options.get(
                "api_key",
                self._pipeline_defaults.get("api_key", self._provider_defaults.api_key),
            ),
            "region": runtime_options.get(
                "region",
                self._pipeline_defaults.get("region", self._provider_defaults.region),
            ),
            "realtime_stt_base_url": runtime_options.get(
                "realtime_stt_base_url",
                self._pipeline_defaults.get("realtime_stt_base_url", self._provider_defaults.realtime_stt_base_url),
            ),
            "language": runtime_options.get(
                "language",
                self._pipeline_defaults.get("language", self._provider_defaults.language),
            ),
            "request_timeout_sec": float(
                runtime_options.get(
                    "request_timeout_sec",
                    self._pipeline_defaults.get("request_timeout_sec", self._default_timeout),
                )
            ),
        }


# ---------------------------------------------------------------------------
# Azure TTS Adapter
# ---------------------------------------------------------------------------

class AzureTTSAdapter(TTSComponent):
    """Azure Text-to-Speech REST adapter.

    Endpoint: POST {region}.tts.speech.microsoft.com/cognitiveservices/v1
    Auth:     Ocp-Apim-Subscription-Key header
    Input:    SSML XML body
    Output:   Audio bytes in the format specified by X-Microsoft-OutputFormat header
    """

    def __init__(
        self,
        component_key: str,
        app_config: AppConfig,
        provider_config: AzureTTSProviderConfig,
        options: Optional[Dict[str, Any]] = None,
        *,
        session_factory: Optional[Callable[[], aiohttp.ClientSession]] = None,
    ):
        self.component_key = component_key
        self._app_config = app_config
        self._provider_defaults = provider_config
        self._pipeline_defaults = options or {}
        self._session_factory = session_factory
        self._session: Optional[aiohttp.ClientSession] = None
        self._chunk_size_ms = int(self._pipeline_defaults.get("chunk_size_ms", provider_config.chunk_size_ms))

    async def start(self) -> None:
        logger.debug(
            "Azure TTS adapter initialized",
            component=self.component_key,
            region=self._provider_defaults.region,
            voice=self._provider_defaults.voice_name,
        )

    async def stop(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def open_call(self, call_id: str, options: Dict[str, Any]) -> None:
        await self._ensure_session()

    async def close_call(self, call_id: str) -> None:
        return

    async def validate_connectivity(self, options: Dict[str, Any]) -> Dict[str, Any]:
        merged = self._compose_options(options or {})
        # Inject the computed Azure URL so the base-class URL check passes.
        if not merged.get("base_url"):
            region = merged.get("region") or self._provider_defaults.region
            merged["base_url"] = merged.get("tts_base_url") or _build_azure_tts_url(region)
        return await super().validate_connectivity(merged)

    async def synthesize(
        self,
        call_id: str,
        text: str,
        options: Dict[str, Any],
    ) -> AsyncIterator[bytes]:
        if not text:
            return
            yield  # make this an async generator

        await self._ensure_session()
        assert self._session

        merged = self._compose_options(options)
        api_key = merged.get("api_key") or ""
        if not api_key:
            raise RuntimeError("Azure TTS requires AZURE_SPEECH_KEY / api_key")

        region = str(merged.get("region") or self._provider_defaults.region)
        url = str(merged.get("tts_base_url") or _build_azure_tts_url(region))
        voice_name = str(merged.get("voice_name") or self._provider_defaults.voice_name)
        language = str(merged.get("language") or "")
        output_format = str(merged.get("output_format") or self._provider_defaults.output_format)
        timeout_sec = float(merged.get("request_timeout_sec", self._provider_defaults.request_timeout_sec))

        ssml = _build_ssml(
            text,
            voice_name,
            language,
            prosody_pitch=merged.get("prosody_pitch") or None,
            prosody_rate=merged.get("prosody_rate") or None,
        )
        headers = _make_tts_headers(api_key, output_format)

        logger.info(
            "Azure TTS synthesis started",
            call_id=call_id,
            voice=voice_name,
            output_format=output_format,
            text_preview=text[:64],
        )

        started_at = time.perf_counter()
        use_streaming = bool(merged.get("streaming", self._provider_defaults.streaming))

        async with self._session.post(
            url,
            data=ssml.encode("utf-8"),
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout_sec),
        ) as resp:
            if resp.status >= 400:
                raw = await resp.read()
                body_text = raw.decode("utf-8", errors="ignore")
                latency_ms = (time.perf_counter() - started_at) * 1000.0
                logger.error(
                    "Azure TTS synthesis failed",
                    call_id=call_id,
                    status=resp.status,
                    body_preview=body_text[:200],
                )
                raise RuntimeError(
                    f"Azure TTS request failed (status {resp.status}): {body_text[:256]}"
                )

            latency_ms = (time.perf_counter() - started_at) * 1000.0

            if use_streaming:
                # Stream chunks as they arrive from Azure — minimises time-to-first-audio
                chunk_ms = int(merged.get("chunk_size_ms", self._chunk_size_ms))
                first_chunk = True
                async for raw_chunk in resp.content.iter_chunked(4096):
                    if not raw_chunk:
                        continue
                    if first_chunk:
                        logger.info(
                            "Azure TTS first chunk received (streaming)",
                            call_id=call_id,
                            latency_ms=round(latency_ms, 2),
                            voice=voice_name,
                        )
                        first_chunk = False
                    # Decode the incoming chunk (may be partial WAV/PCM)
                    audio_bytes, source_rate, native_encoding = _decode_tts_audio(raw_chunk, output_format)
                    target_encoding = str(merged.get("target_encoding") or self._provider_defaults.target_encoding)
                    target_rate = int(merged.get("target_sample_rate_hz") or self._provider_defaults.target_sample_rate_hz)
                    if native_encoding == "mulaw" and target_encoding == "mulaw":
                        converted = audio_bytes
                    elif native_encoding in ("pcm16", "pcm"):
                        if source_rate != target_rate:
                            audio_bytes, _ = resample_audio(audio_bytes, source_rate, target_rate)
                        converted = convert_pcm16le_to_target_format(audio_bytes, target_encoding)
                    else:
                        converted = audio_bytes
                    for chunk in _chunk_audio(converted, target_encoding, target_rate, chunk_ms):
                        if chunk:
                            yield chunk
                return

            # Non-streaming: read full response then yield
            raw = await resp.read()

        target_encoding = str(merged.get("target_encoding") or self._provider_defaults.target_encoding)
        target_rate = int(merged.get("target_sample_rate_hz") or self._provider_defaults.target_sample_rate_hz)

        if native_encoding == "mulaw" and target_encoding == "mulaw":
            # Already in mulaw at 8 kHz — yield directly
            converted = audio_bytes
        elif native_encoding in ("pcm16", "pcm"):
            # Resample if needed, then convert to target encoding
            if source_rate != target_rate:
                audio_bytes, _ = resample_audio(audio_bytes, source_rate, target_rate)
            converted = convert_pcm16le_to_target_format(audio_bytes, target_encoding)
        else:
            # Unknown encoding — pass through as-is
            converted = audio_bytes

        logger.info(
            "Azure TTS synthesis completed",
            call_id=call_id,
            output_bytes=len(converted),
            latency_ms=round(latency_ms, 2),
            target_encoding=target_encoding,
            target_sample_rate=target_rate,
        )

        chunk_ms = int(merged.get("chunk_size_ms", self._chunk_size_ms))
        for chunk in _chunk_audio(converted, target_encoding, target_rate, chunk_ms):
            if chunk:
                yield chunk

    async def _ensure_session(self) -> None:
        if self._session and not self._session.closed:
            return
        factory = self._session_factory or aiohttp.ClientSession
        self._session = factory()

    def _compose_options(self, runtime_options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        runtime_options = runtime_options or {}
        return {
            "api_key": runtime_options.get(
                "api_key",
                self._pipeline_defaults.get("api_key", self._provider_defaults.api_key),
            ),
            "region": runtime_options.get(
                "region",
                self._pipeline_defaults.get("region", self._provider_defaults.region),
            ),
            "tts_base_url": runtime_options.get(
                "tts_base_url",
                self._pipeline_defaults.get("tts_base_url", self._provider_defaults.tts_base_url),
            ),
            "voice_name": runtime_options.get(
                "voice_name",
                self._pipeline_defaults.get("voice_name", self._provider_defaults.voice_name),
            ),
            "language": runtime_options.get(
                "language",
                self._pipeline_defaults.get("language", ""),
            ),
            "output_format": runtime_options.get(
                "output_format",
                self._pipeline_defaults.get("output_format", self._provider_defaults.output_format),
            ),
            "target_encoding": runtime_options.get(
                "target_encoding",
                self._pipeline_defaults.get("target_encoding", self._provider_defaults.target_encoding),
            ),
            "target_sample_rate_hz": int(
                runtime_options.get(
                    "target_sample_rate_hz",
                    self._pipeline_defaults.get("target_sample_rate_hz", self._provider_defaults.target_sample_rate_hz),
                )
            ),
            "chunk_size_ms": int(
                runtime_options.get(
                    "chunk_size_ms",
                    self._pipeline_defaults.get("chunk_size_ms", self._chunk_size_ms),
                )
            ),
            "request_timeout_sec": float(
                runtime_options.get(
                    "request_timeout_sec",
                    self._pipeline_defaults.get("request_timeout_sec", self._provider_defaults.request_timeout_sec),
                )
            ),
            "streaming": bool(
                runtime_options.get(
                    "streaming",
                    self._pipeline_defaults.get("streaming", self._provider_defaults.streaming),
                )
            ),
            "prosody_pitch": (
                runtime_options.get(
                    "prosody_pitch",
                    self._pipeline_defaults.get("prosody_pitch", self._provider_defaults.prosody_pitch),
                ) or None
            ),
            "prosody_rate": (
                runtime_options.get(
                    "prosody_rate",
                    self._pipeline_defaults.get("prosody_rate", self._provider_defaults.prosody_rate),
                ) or None
            ),
        }


__all__ = [
    "AzureSTTFastAdapter",
    "AzureSTTRealtimeAdapter",
    "AzureTTSAdapter",
]
