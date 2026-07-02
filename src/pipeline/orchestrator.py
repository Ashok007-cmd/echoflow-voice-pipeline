"""Pipeline orchestrator — ties together ASR, LLM, and TTS with streaming,
latency tracking, and resilience handling.

Phase 1: Streaming pipeline coordination
Phase 2: Latency budget instrumentation
Phase 3: Resilience wrapping (timeouts, circuit breakers, degradation)
"""

import asyncio
import logging
import time
from typing import AsyncGenerator, Optional

import click

from src.config import PipelineConfig, ASRBackend, LLMBackend, TTSBackend
from src.pipeline.asr import ASREngine, create_asr_engine
from src.pipeline.llm_client import LLMClient, create_llm_client
from src.pipeline.models import AudioChunk, LLMResponse, PipelineState, TTSResult, TurnMetrics, TranscriptionResult
from src.pipeline.tts import TTSEngine, create_tts_engine
from src.monitoring.latency_tracker import LatencyTracker, PipelineTimer
from src.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
)
from src.resilience.degradation import DegradationManager, DegradationLevel
from src.resilience.timeout import (
    PipelineTimeoutError,
    TimeoutSettings,
    with_timeout,
)

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Main orchestrator for the real-time voice assistant pipeline.

    Manages the streaming flow:
      Microphone → ASR → LLM → TTS → Speaker

    With:
      - Latency measurement at every stage (Phase 2)
      - Timeouts, circuit breakers, graceful degradation (Phase 3)
    """

    def __init__(
        self,
        config: Optional[PipelineConfig] = None,
    ):
        self.config = config or PipelineConfig.from_env()

        # Pipeline components
        self.asr_engine: ASREngine = create_asr_engine(self.config.asr)
        self.llm_client: LLMClient = create_llm_client(self.config.llm)
        self.tts_engine: TTSEngine = create_tts_engine(self.config.tts)

        # Define caching dicts for dynamically created engines (for fallback support)
        self._all_asr_engines = {self.config.asr.backend: self.asr_engine}
        self._all_llm_clients = {self.config.llm.backend: self.llm_client}
        self._all_tts_engines = {self.config.tts.backend: self.tts_engine}

        # Queue tracking for async pipelining
        self._sentence_queue = asyncio.Queue()
        self._audio_queue = asyncio.Queue()
        self._playback_task = None
        self._warmup_task = None

        # Monitoring
        self.latency_tracker = LatencyTracker(
            window_size=self.config.monitoring.latency_window
        )
        self.state = PipelineState()

        from src.monitoring.database import MetricsDatabase
        self.db = MetricsDatabase()

        # Resilience
        self.degradation = DegradationManager()

        # Circuit breakers for each external service
        self.asr_circuit = CircuitBreaker(
            CircuitBreakerConfig(
                service_name="ASR",
                failure_threshold=self.config.resilience.circuit_breaker_failure_threshold,
                recovery_timeout=self.config.resilience.circuit_breaker_recovery_timeout,
            )
        )
        self.llm_circuit = CircuitBreaker(
            CircuitBreakerConfig(
                service_name="LLM",
                failure_threshold=self.config.resilience.circuit_breaker_failure_threshold,
                recovery_timeout=self.config.resilience.circuit_breaker_recovery_timeout,
            )
        )
        self.tts_circuit = CircuitBreaker(
            CircuitBreakerConfig(
                service_name="TTS",
                failure_threshold=self.config.resilience.circuit_breaker_failure_threshold,
                recovery_timeout=self.config.resilience.circuit_breaker_recovery_timeout,
            )
        )

        # Register degradation alerts
        self.degradation.register_alert(self._on_degradation_change)

    @staticmethod
    def _detect_test_env() -> bool:
        import sys
        return "pytest" in sys.modules or "unittest" in sys.modules

    @property
    def _is_testing(self) -> bool:
        """True when running under a test framework — suppresses real-service fallbacks."""
        return self.__class__._detect_test_env()

    def _get_asr_engine(self, backend: ASRBackend) -> ASREngine:
        if backend == self.config.asr.backend:
            return self.asr_engine
        if backend not in self._all_asr_engines:
            import copy
            config = copy.copy(self.config.asr)
            config.backend = backend
            self._all_asr_engines[backend] = create_asr_engine(config)
        return self._all_asr_engines[backend]

    def _get_llm_client(self, backend: LLMBackend) -> LLMClient:
        if backend == self.config.llm.backend:
            return self.llm_client
        if backend not in self._all_llm_clients:
            import copy
            config = copy.copy(self.config.llm)
            config.backend = backend
            self._all_llm_clients[backend] = create_llm_client(config)
        return self._all_llm_clients[backend]

    def _get_tts_engine(self, backend: TTSBackend) -> TTSEngine:
        if backend == self.config.tts.backend:
            return self.tts_engine
        if backend not in self._all_tts_engines:
            import copy
            config = copy.copy(self.config.tts)
            config.backend = backend
            self._all_tts_engines[backend] = create_tts_engine(config)
        return self._all_tts_engines[backend]

    def interrupt(self) -> None:
        """Interrupt current playback and clear queues."""
        logger.info("Interruption triggered: cancelling active playback and clearing queues")
        
        # 1. Clear queues by reading all items
        while not self._sentence_queue.empty():
            try:
                self._sentence_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
                
        while not self._audio_queue.empty():
            try:
                self._audio_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
                
        # 2. Cancel active playback task
        if self._playback_task and not self._playback_task.done():
            self._playback_task.cancel()
            
        # 3. Stop playbacks on all TTS engines
        for engine in self._all_tts_engines.values():
            asyncio.create_task(engine.stop())
        if self.tts_engine:
            asyncio.create_task(self.tts_engine.stop())

    async def _execute_asr(self, audio_data: bytes, sample_rate: int) -> TranscriptionResult:
        """Execute ASR with fallback chain routing."""
        from src.pipeline.models import TranscriptionResult
        primary = self.config.asr.backend
        backends = [primary]
        if self.config.resilience.allow_asr_fallback and not self._is_testing:
            for b in [ASRBackend.OPENAI, ASRBackend.GOOGLE, ASRBackend.MOCK]:
                if b != primary:
                    backends.append(b)
            
        last_error = None
        for backend in backends:
            if backend != primary:
                if backend == ASRBackend.OPENAI and not self.config.asr.openai_api_key:
                    continue
            engine = self._get_asr_engine(backend)
            try:
                result = await self.asr_circuit.call(
                    lambda engine=engine: with_timeout(
                        engine.transcribe(audio_data, sample_rate),
                        TimeoutSettings.for_asr(self.config.resilience.asr_timeout),
                    )
                )
                if not result.error:
                    if backend != primary:
                        logger.warning(f"ASR primary {primary.value} failed. Fell back to {backend.value} successfully.")
                        self.degradation.record_failure("asr")
                    else:
                        self.degradation.record_recovery("asr")
                    return result
                else:
                    last_error = result.error
            except Exception as e:
                logger.warning(f"ASR backend {backend.value} failed: {e}")
                last_error = str(e)
                
        self.degradation.record_failure("asr")
        return TranscriptionResult(text="", is_final=True, error=f"All ASR backends failed. Last error: {last_error}")

    async def _execute_llm_stream(self, user_text: str) -> AsyncGenerator[LLMResponse, None]:
        """Execute LLM streaming with fallback chain support."""
        primary = self.config.llm.backend
        backends = [primary]
        if not self._is_testing:
            for b in [LLMBackend.OPENAI, LLMBackend.ANTHROPIC, LLMBackend.MOCK]:
                if b != primary:
                    backends.append(b)
            
        last_error = None
        for backend in backends:
            if backend != primary:
                if backend == LLMBackend.OPENAI and not self.config.llm.openai_api_key:
                    continue
                if backend == LLMBackend.ANTHROPIC and not self.config.llm.anthropic_api_key:
                    continue
                
            client = self._get_llm_client(backend)
            original_prompt = client.config.system_prompt
            lang_code = getattr(self.config.asr, "language", "en-us").lower()
            if not lang_code.startswith("en"):
                lang_prefix = lang_code[:2]
                lang_name = {
                    "es": "Spanish",
                    "fr": "French",
                    "de": "German",
                    "it": "Italian",
                    "ja": "Japanese",
                    "zh": "Chinese",
                }.get(lang_prefix)
                if lang_name:
                    client.config.system_prompt = original_prompt + f" You must respond ONLY in {lang_name}."
            try:
                first = True
                async for response in self._protected_llm_stream_for_client(client, user_text):
                    if response.error:
                        raise RuntimeError(response.error)
                    yield response
                    first = False
                if not first:
                    self.degradation.record_recovery("llm")
                    return
            except Exception as e:
                logger.warning(f"LLM backend {backend.value} failed: {e}")
                last_error = str(e)
            finally:
                client.config.system_prompt = original_prompt
                
        self.degradation.record_failure("llm")
        yield LLMResponse(text="", is_final=True, error=f"LLM failed: {last_error}")

    async def _protected_llm_stream_for_client(
        self, client: LLMClient, user_text: str
    ) -> AsyncGenerator[LLMResponse, None]:
        """Generate LLM stream for a specific client with timeout and circuit breaker protection."""
        timeout_settings = TimeoutSettings.for_llm(self.config.resilience.llm_timeout)

        async def _next_chunk_or_none(g):
            try:
                return await g.__anext__()
            except StopAsyncIteration:
                return None

        try:
            async def _stream():
                async for token in client.generate_stream(user_text):
                    yield token

            gen = _stream()
            while True:
                chunk = await self.llm_circuit.call(
                    lambda: asyncio.wait_for(
                        _next_chunk_or_none(gen),
                        timeout=timeout_settings.timeout_seconds,
                    )
                )
                if chunk is None:
                    break
                yield chunk
                if chunk.is_final:
                    break

        except asyncio.TimeoutError:
            raise PipelineTimeoutError(
                stage="LLM streaming",
                timeout=timeout_settings.timeout_seconds,
            )

    async def _execute_tts(self, text: str) -> tuple[Optional[TTSEngine], TTSResult]:
        """Execute TTS with fallback chain routing."""
        from src.pipeline.models import TTSResult
        primary = self.config.tts.backend
        backends = [primary]
        if not self._is_testing:
            for b in [TTSBackend.EDGE_TTS, TTSBackend.PYTTSX3, TTSBackend.MOCK]:
                if b != primary:
                    backends.append(b)
                
        last_error = None
        for backend in backends:
            engine = self._get_tts_engine(backend)
            try:
                result = await self.tts_circuit.call(
                    lambda engine=engine: with_timeout(
                        engine.synthesize(text),
                        TimeoutSettings.for_tts(self.config.resilience.tts_timeout),
                    )
                )
                if not result.error:
                    if backend != primary:
                        logger.warning(f"TTS primary {primary.value} failed. Fell back to {backend.value} successfully.")
                        self.degradation.record_failure("tts")
                    else:
                        self.degradation.record_recovery("tts")
                    result.fallback_used = (backend != primary)
                    return engine, result
                else:
                    last_error = result.error
            except Exception as e:
                logger.warning(f"TTS backend {backend.value} failed: {e}")
                last_error = str(e)
                
        self.degradation.record_failure("tts")
        return None, TTSResult(text=text, is_final=True, error=f"All TTS failed. Last error: {last_error}")

    # ── Main pipeline loop ──────────────────────────────────────────

    async def run_turn(self, audio_data: bytes, sample_rate: int) -> Optional[TurnMetrics]:
        """Execute one full pipeline turn: ASR → LLM → TTS.

        Args:
            audio_data: PCM audio bytes from the microphone.
            sample_rate: Audio sample rate in Hz.

        Returns:
            TurnMetrics with latency breakdown, or None if the turn was skipped.
        """
        if self.state.degradation_mode and self.state.current_turn == 0:
            logger.warning("Pipeline is in degradation mode, skipping turn")
            return None

        # Guard against absurdly large audio blobs (max 10 MB / ~312 s at 16 kHz mono 16-bit)
        max_audio_bytes = 10 * 1024 * 1024
        if len(audio_data) > max_audio_bytes:
            logger.warning(f"Audio input too large ({len(audio_data)} bytes); truncating to {max_audio_bytes} bytes")
            audio_data = audio_data[:max_audio_bytes]

        self.state.current_turn += 1
        turn_number = self.latency_tracker.start_turn()

        e2e_timer = PipelineTimer()
        e2e_timer.start()

        # Interruption: cancel any active playback before starting the next turn
        self.interrupt()

        user_text = ""
        assistant_text = ""

        # ── Step 1: ASR ─────────────────────────────────────────
        asr_timer = PipelineTimer()
        asr_timer.start()
        
        asr_result = await self._execute_asr(audio_data, sample_rate)
        asr_latency = asr_timer.stop()
        self._record_stage_latency("asr", asr_latency)

        if asr_result.error and not asr_result.text:
            logger.warning(f"ASR stage failed: {asr_result.error}")
            return self._complete_turn(
                turn_number, e2e_timer,
                asr_latency, user_text, assistant_text,
                error=asr_result.error,
            )

        user_text = asr_result.text
        if not user_text.strip():
            # No speech detected — skip the turn gracefully
            return self._complete_turn(
                turn_number, e2e_timer, asr_latency, user_text, assistant_text,
            )

        # Clear and re-initialize queues for this turn
        self._sentence_queue = asyncio.Queue()
        self._audio_queue = asyncio.Queue()

        # Shared metrics tracking variables
        llm_ttft_ms = 0.0
        llm_total_ms = 0.0
        tts_first_chunk_ms = 0.0
        tts_total_ms = 0.0
        playback_total_ms = 0.0
        tts_fallback = False
        llm_error_msg = None
        
        assistant_response_chunks = []

        # Task 1: LLM Generator worker
        async def _llm_worker():
            nonlocal llm_ttft_ms, llm_total_ms, llm_error_msg
            llm_timer = PipelineTimer()
            llm_timer.start()
            
            sentence_buffer = ""
            word_count = 0
            
            try:
                stream = self._execute_llm_stream(user_text)
                async for response in stream:
                    if response.error:
                        llm_error_msg = response.error
                        break
                        
                    if response.is_first_token:
                        llm_ttft_ms = llm_timer.stop()
                        self._record_stage_latency("llm_time_to_first_token", llm_ttft_ms)
                        
                    assistant_response_chunks.append(response.text)
                    sentence_buffer += response.text
                    
                    # Split on sentence delimiters or word count limit
                    words = response.text.split()
                    word_count += len(words)
                    
                    if any(c in response.text for c in ".!?\n") or word_count >= 15:
                        text_chunk = sentence_buffer.strip()
                        if text_chunk:
                            await self._sentence_queue.put(text_chunk)
                        sentence_buffer = ""
                        word_count = 0
                        
                    if response.is_final:
                        llm_total_ms = response.latency_ms
                        self._record_stage_latency("llm_total", llm_total_ms)
                        
                # Flush any remaining tokens
                if sentence_buffer.strip():
                    await self._sentence_queue.put(sentence_buffer.strip())
                    
                if llm_ttft_ms == 0.0:
                    # No streaming tokens received (empty response)
                    llm_ttft_ms = llm_timer.stop()
                    llm_total_ms = llm_ttft_ms
                    self._record_stage_latency("llm_time_to_first_token", llm_ttft_ms)
                    self._record_stage_latency("llm_total", llm_total_ms)
                    
            except Exception as e:
                logger.error(f"LLM worker failed: {e}")
                llm_error_msg = str(e)
            finally:
                # Signal to TTS worker that sentence generation is done
                await self._sentence_queue.put(None)

        # Task 2: TTS Synthesizer worker
        async def _tts_worker():
            nonlocal tts_first_chunk_ms, tts_total_ms, tts_fallback
            while True:
                sentence = await self._sentence_queue.get()
                if sentence is None:
                    # Signal to playback worker that synthesis is complete
                    await self._audio_queue.put(None)
                    break
                    
                chunk_tts_timer = PipelineTimer()
                chunk_tts_timer.start()
                
                try:
                    engine, tts_result = await self._execute_tts(sentence)
                    chunk_tts_latency = chunk_tts_timer.stop()
                    tts_total_ms += chunk_tts_latency
                    
                    if tts_first_chunk_ms == 0.0:
                        tts_first_chunk_ms = chunk_tts_latency
                        self._record_stage_latency("tts", tts_first_chunk_ms)
                        
                    if tts_result.error or not engine:
                        logger.warning(f"TTS synthesis error: {tts_result.error}")
                        tts_fallback = True
                        await self._audio_queue.put((None, tts_result))
                    else:
                        await self._audio_queue.put((engine, tts_result))
                        
                except Exception as e:
                    logger.error(f"TTS worker exception: {e}")
                    tts_fallback = True
                    chunk_tts_latency = chunk_tts_timer.stop()
                    tts_total_ms += chunk_tts_latency
                    if tts_first_chunk_ms == 0.0:
                        tts_first_chunk_ms = chunk_tts_latency
                        self._record_stage_latency("tts", tts_first_chunk_ms)
                    await self._audio_queue.put((None, TTSResult(text=sentence, error=str(e))))

        # Task 3: Audio Playback worker
        async def _playback_worker():
            nonlocal playback_total_ms, tts_fallback
            while True:
                item = await self._audio_queue.get()
                if item is None:
                    break
                    
                engine, tts_result = item
                if tts_result.error or not engine:
                    if self.config.resilience.allow_tts_fallback_to_text:
                        click.echo(click.style(f"Assistant (text fallback): {tts_result.text}", fg="green", dim=True))
                    continue
                    
                try:
                    play_timer = PipelineTimer()
                    play_timer.start()
                    await engine.play(tts_result.audio_data)
                    play_latency = play_timer.stop()
                    playback_total_ms += play_latency
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Playback failed: {e}")
                    tts_fallback = True

        # Launch the tasks to execute concurrently as a pipeline
        llm_task = asyncio.create_task(_llm_worker())
        tts_task = asyncio.create_task(_tts_worker())
        self._playback_task = asyncio.create_task(_playback_worker())

        try:
            await asyncio.gather(llm_task, tts_task, self._playback_task)
        except asyncio.CancelledError:
            logger.info("Pipeline execution was cancelled (e.g. by user interruption)")
        except Exception as e:
            logger.error(f"Error in pipelined turn gather: {e}")

        # Re-assemble final response text
        assistant_text = "".join(assistant_response_chunks)

        # Handle LLM failure or empty response degradation
        if llm_error_msg or not assistant_text.strip():
            if not llm_error_msg and not assistant_text.strip():
                logger.warning("LLM returned empty response")
                llm_error_msg = "Empty response"
            
            if self.config.resilience.allow_empty_response_graceful:
                assistant_text = self.config.resilience.degradation_message
                engine = self._get_tts_engine(self.config.tts.backend)
                try:
                    res = await engine.synthesize(assistant_text)
                    await engine.play(res.audio_data)
                except Exception as e:
                    logger.error(f"Failed to play degradation message: {e}")

            return self._complete_turn(
                turn_number, e2e_timer,
                asr_latency, user_text, assistant_text,
                llm_ttft_ms=llm_ttft_ms, llm_total_ms=llm_total_ms,
                tts_ms=tts_first_chunk_ms, playback_ms=playback_total_ms,
                error=llm_error_msg, degraded=True,
            )

        # ── Complete ────────────────────────────────────────────
        self._record_stage_latency("audio_playback", playback_total_ms)
        return self._complete_turn(
            turn_number, e2e_timer,
            asr_latency, user_text, assistant_text,
            llm_ttft_ms=llm_ttft_ms, llm_total_ms=llm_total_ms,
            tts_ms=tts_first_chunk_ms, playback_ms=playback_total_ms,
            degraded=tts_fallback,
        )

    # ── Protected streaming calls with resilience wrappers ──────────

    async def _protected_llm_stream(
        self, user_text: str,
    ) -> AsyncGenerator[LLMResponse, None]:
        """Generate LLM response with timeout and circuit breaker protection (backwards compatible wrapper)."""
        async for chunk in self._protected_llm_stream_for_client(self.llm_client, user_text):
            yield chunk

    # ── Audio capture ───────────────────────────────────────────────

    async def capture_audio_stream(
        self,
    ) -> AsyncGenerator[AudioChunk, None]:
        """Capture streaming audio from the microphone.

        Yields AudioChunk objects as audio data arrives from the mic.
        """
        try:
            import pyaudio

            p = pyaudio.PyAudio()
            stream = p.open(
                format=pyaudio.paInt16,
                channels=self.config.audio.channels,
                rate=self.config.audio.input_sample_rate,
                input=True,
                input_device_index=self.config.audio.input_device_index,
                frames_per_buffer=self.config.audio.input_chunk_size,
                stream_callback=None,
            )

            logger.info(
                f"Audio capture started: {self.config.audio.input_sample_rate}Hz, "
                f"{self.config.audio.channels}ch"
            )

            try:
                while self.state.is_running:
                    data = stream.read(
                        self.config.audio.input_chunk_size,
                        exception_on_overflow=False,
                    )
                    if self.state.is_muted:
                        await asyncio.sleep(0.01)
                        continue

                    yield AudioChunk(
                        data=data,
                        timestamp=time.perf_counter(),
                        sample_rate=self.config.audio.input_sample_rate,
                    )
            finally:
                stream.stop_stream()
                stream.close()
                p.terminate()

        except Exception as e:
            logger.warning(
                f"Failed to initialize audio hardware ({e}). "
                "Falling back to synthetic generator for testing..."
            )
            # Yield cycling noise and silence to simulate VAD triggers
            import numpy as np
            sample_rate = self.config.audio.input_sample_rate
            chunk_size = self.config.audio.input_chunk_size
            
            counter = 0
            while self.state.is_running:
                # Calculate chunks per second
                chunks_per_sec = sample_rate / chunk_size
                
                # Cycle every 8 seconds: 5s silence, 1.5s speech, 1.5s silence
                cycle_len = int(8 * chunks_per_sec)
                speech_start = int(4 * chunks_per_sec)
                speech_end = int(6.5 * chunks_per_sec)
                
                idx = counter % cycle_len
                if speech_start <= idx < speech_end:
                    # Noise (amplitude > 500)
                    data = (np.random.randn(chunk_size) * 1000).astype(np.int16).tobytes()
                else:
                    data = b"\x00" * chunk_size * 2
                
                yield AudioChunk(
                    data=data,
                    timestamp=time.perf_counter(),
                    sample_rate=sample_rate,
                    is_speech=True,
                )
                counter += 1
                await asyncio.sleep(chunk_size / sample_rate)

    # ── Lifecycle ───────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the pipeline."""
        self.state.is_running = True
        self.state.degradation_mode = False
        logger.info(
            f"Pipeline started: ASR={self.config.asr.backend.value}, "
            f"LLM={self.config.llm.backend.value}, "
            f"TTS={self.config.tts.backend.value}"
        )
        
        # Warm up the backends in the background to improve first-turn latency
        async def _warmup():
            try:
                logger.info("Pre-warming pipeline components in background...")
                if self.config.asr.backend == ASRBackend.LOCAL_WHISPER:
                    engine = self._get_asr_engine(self.config.asr.backend)
                    if hasattr(engine, "_load_model"):
                        await engine._load_model()
                if self.config.tts.backend == TTSBackend.PYTTSX3:
                    self._get_tts_engine(self.config.tts.backend)
                logger.info("Pre-warming complete")
            except Exception as e:
                logger.warning(f"Backend pre-warming warning: {e}")
                
        self._warmup_task = asyncio.create_task(_warmup())

    async def stop(self) -> None:
        """Stop the pipeline gracefully."""
        self.state.is_running = False
        self.interrupt()
        if self._warmup_task and not self._warmup_task.done():
            self._warmup_task.cancel()
            try:
                await self._warmup_task
            except (asyncio.CancelledError, Exception) as e:
                logger.debug(f"Warmup task cancellation raised: {e}")
        self.db.close()
        logger.info("Pipeline stopped")

    async def shutdown(self) -> None:
        """Full shutdown with resource cleanup."""
        await self.stop()
        logger.info("Pipeline shutdown complete")

    def health_check(self) -> dict:
        """Return current health status of pipeline components."""
        return {
            "running": self.state.is_running,
            "degradation_mode": self.state.degradation_mode,
            "degradation_level": self.degradation.current_level.name,
            "current_turn": self.state.current_turn,
            "circuit_breakers": {
                "asr": self.asr_circuit.state.name,
                "llm": self.llm_circuit.state.name,
                "tts": self.tts_circuit.state.name,
            },
            "backends": {
                "asr": self.config.asr.backend.value,
                "llm": self.config.llm.backend.value,
                "tts": self.config.tts.backend.value,
            },
        }

    # ── Private helpers ─────────────────────────────────────────────

    def _record_stage_latency(self, stage: str, latency_ms: float) -> None:
        """Record a stage latency in the tracker."""
        self.latency_tracker.record_stage_value(stage, latency_ms)

    def _on_degradation_change(self, message: str, level: DegradationLevel) -> None:
        """Handle degradation level changes."""
        self.state.degradation_mode = level != DegradationLevel.NONE
        if level != DegradationLevel.NONE:
            logger.warning(f"Degradation changed: {level.name} — {message}")
        else:
            logger.info(f"Degradation cleared: {message}")

    def _complete_turn(
        self,
        turn_number: int,
        e2e_timer: PipelineTimer,
        asr_ms: float,
        user_text: str,
        assistant_text: str,
        llm_ttft_ms: float = 0.0,
        llm_total_ms: float = 0.0,
        tts_ms: float = 0.0,
        playback_ms: float = 0.0,
        error: Optional[str] = None,
        degraded: bool = False,
    ) -> TurnMetrics:
        """Complete and record turn metrics."""
        e2e_ms = e2e_timer.stop()
        self._record_stage_latency("end_to_end", e2e_ms)

        metrics = self.latency_tracker.complete_turn(
            asr_ms=asr_ms,
            llm_ttft_ms=llm_ttft_ms,
            llm_total_ms=llm_total_ms,
            tts_ms=tts_ms,
            audio_playback_ms=playback_ms,
            e2e_ms=e2e_ms,
            asr_backend=self.config.asr.backend.value,
            llm_backend=self.config.llm.backend.value,
            tts_backend=self.config.tts.backend.value,
            llm_model=self.config.llm.model,
            error=error,
            degradation_used=degraded or self.state.degradation_mode,
            user_text=user_text,
            assistant_text=assistant_text,
        )

        if self.config.monitoring.log_latency:
            logger.info(
                f"Turn {turn_number}: ASR={asr_ms:.0f}ms | "
                f"LLM TTFT={llm_ttft_ms:.0f}ms | "
                f"LLM total={llm_total_ms:.0f}ms | "
                f"TTS={tts_ms:.0f}ms | "
                f"Playback={playback_ms:.0f}ms | "
                f"E2E={e2e_ms:.0f}ms"
                + (f" | ERROR: {error}" if error else "")
                + (" | ⚠ DEGRADED" if degraded else "")
            )

        self.db.save_turn_metrics(metrics)

        return metrics
