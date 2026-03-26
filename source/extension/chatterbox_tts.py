import asyncio
import logging
import os
import threading
import time
import uuid
from functools import wraps, partial
from typing import TYPE_CHECKING

import torch

try:
    import torchaudio as ta
except Exception:
    ta = None

try:
    from chatterbox.mtl_tts import ChatterboxMultilingualTTS
except Exception:
    ChatterboxMultilingualTTS = None

if TYPE_CHECKING:
    try:
        from extensions.telegram_bot.source.user import User as User
        from extensions.telegram_bot.source.conf import cfg as _cfg
    except Exception:
        from source.user import User as User
        from source.conf import cfg as _cfg


def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        target_func = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, target_func)

    return run


class ChatterboxTTS:
    supported_languages = {
        "ar",
        "da",
        "de",
        "el",
        "en",
        "es",
        "fi",
        "fr",
        "he",
        "hi",
        "it",
        "ja",
        "ko",
        "ms",
        "nl",
        "no",
        "pl",
        "pt",
        "ru",
        "sv",
        "sw",
        "tr",
        "zh",
    }

    def __init__(self):
        forced_device = os.getenv("TTS_DEVICE", "").strip().lower()
        if forced_device in {"cpu", "cuda", "cuda:0"}:
            self.device = forced_device
        else:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = None
        self.sample_rate = None
        self._lock = threading.Lock()
        history_dir = "history"
        try:
            from extensions.telegram_bot.source.conf import cfg as _cfg
            history_dir = _cfg.history_dir_path
        except Exception:
            try:
                from source.conf import cfg as _cfg
                history_dir = _cfg.history_dir_path
            except Exception:
                history_dir = "history"
        self.output_dir = os.path.join(history_dir, "tts_audio")
        os.makedirs(self.output_dir, exist_ok=True)
        logging.info("### Chatterbox TTS INIT DONE ###")

    def _ensure_model(self) -> bool:
        if self.model is not None:
            return True
        if ChatterboxMultilingualTTS is None:
            logging.error("Chatterbox TTS is not installed. Add chatterbox-tts to requirements.")
            return False
        with self._lock:
            if self.model is None:
                try:
                    self.model = ChatterboxMultilingualTTS.from_pretrained(device=self.device)
                    self.sample_rate = self.model.sr
                except Exception as e:
                    logging.error("Chatterbox TTS model load failed: %s", e)
                    return False
        return True

    @staticmethod
    def _map_language(language: str) -> str:
        if not language:
            return "en"
        key = language.strip().lower().replace("_", "-")
        if key.startswith("pt"):
            return "pt"
        if key.startswith("zh"):
            return "zh"
        return key

    async def get_audio(self, text: str, user_id: int, user, voice_path: str | None = None):
        return await self.generate_audio(text, user_id, user, voice_path)

    @async_wrap
    def generate_audio(self, text: str, user_id: int, user, voice_path: str | None = None):
        if not text or len(text.strip()) == 0:
            return None
        if not self._ensure_model():
            return None
        if ta is None:
            logging.error("torchaudio is not installed; cannot write TTS audio.")
            return None

        audio_prompt_path = voice_path or getattr(user, "voice_clone_path", None)
        if not audio_prompt_path or not os.path.exists(audio_prompt_path):
            logging.warning("Chatterbox TTS missing voice clone reference audio.")
            return None

        language_id = self._map_language(getattr(user, "language", "en"))
        if language_id not in self.supported_languages:
            language_id = "en"

        try:
            os.makedirs(self.output_dir, exist_ok=True)
            wav = self.model.generate(
                text=text,
                audio_prompt_path=audio_prompt_path,
                language_id=language_id,
            )
            if isinstance(wav, torch.Tensor):
                wav = wav.detach().cpu()
                if wav.dim() == 1:
                    wav = wav.unsqueeze(0)
            os.makedirs(self.output_dir, exist_ok=True)
            wav_path = os.path.join(
                self.output_dir,
                f"{user_id}_chatterbox_{time.time_ns()}_{uuid.uuid4().hex}.wav",
            )
            ta.save(wav_path, wav, self.sample_rate)
            return wav_path
        except Exception as e:
            logging.error("Chatterbox TTS generate failed: %s", e)
            return None
