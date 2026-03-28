import json
import logging
import asyncio
import os
import random
import re
import time
import shutil
import mimetypes
from os.path import exists, normpath
from os import remove
from pathlib import Path
from threading import Event
from typing import Dict, Union

import backoff
import urllib3
from aiogram import Bot, types
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.dispatcher.dispatcher import Dispatcher
from aiogram.types.input_file import BufferedInputFile
from aiogram.types.input_media_audio import InputMediaAudio
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters.command import Command

logging.basicConfig(level=logging.INFO)

TELEGRAM_CAPTION_LIMIT = 1024
TTS_CHUNK_MAX_CHARS = 500
TTS_MAX_CHARACTER_NAME = 64

try:
    import extensions.telegram_bot.source.text_process as tp
    import extensions.telegram_bot.source.const as const
    import extensions.telegram_bot.source.utils as utils
    import extensions.telegram_bot.source.buttons as buttons
    from extensions.telegram_bot.source.conf import cfg
    from extensions.telegram_bot.source.user import User as User
    from extensions.telegram_bot.source.extension.silero import Silero as Silero
    from extensions.telegram_bot.source.extension.chatterbox_tts import ChatterboxTTS as ChatterboxTTS
    from extensions.telegram_bot.source.extension.sd_api import SdApi as SdApi
    from extensions.telegram_bot.source.extension.comfyui_api import ComfyUIApi as ComfyUIApi
except ImportError:
    import source.text_process as tp
    import source.const as const
    import source.utils as utils
    import source.buttons as buttons
    from source.conf import cfg
    from source.user import User as User
    from source.extension.silero import Silero as Silero
    from source.extension.chatterbox_tts import ChatterboxTTS as ChatterboxTTS
    from source.extension.sd_api import SdApi as SdApi
    from source.extension.comfyui_api import ComfyUIApi as ComfyUIApi


class AiogramLlmBot:
    # Set dummy obj for telegram updater
    bot: Union[Bot, None] = None
    dp: Union[Dispatcher, None] = None
    # dict of User data dicts, here stored all users' session info.
    users: Dict[int, User] = {}

    def __init__(self, config_file_path="configs/app_config.json"):
        """Init telegram bot class. Use run_telegram_bot() to initiate bot.

        Args
            config_file_path: path to config file
        """
        logging.info(f"### TelegramBotWrapper INIT config_file_path: {config_file_path} ###")
        # Set&Load main config file
        self.config_file_path = config_file_path
        cfg.load(self.config_file_path)
        # Silero initiate
        self.silero = Silero()
        # Chatterbox initiate
        self.chatterbox = ChatterboxTTS()
        # Image generation backend initiate
        image_backend = (cfg.image_backend or "sd_webui").strip().lower()
        self.image_backend = image_backend
        if image_backend == "comfyui":
            try:
                self.ImageApi = ComfyUIApi(
                    url=cfg.comfyui_url,
                    workflow_file_path=cfg.comfyui_workflow_file_path,
                    prompt_node_id=cfg.comfyui_prompt_node_id,
                    prompt_field=cfg.comfyui_prompt_field,
                    negative_prompt=cfg.comfyui_negative_prompt,
                    negative_prompt_node_id=cfg.comfyui_negative_prompt_node_id,
                    negative_prompt_field=cfg.comfyui_negative_prompt_field,
                    seed_node_id=cfg.comfyui_seed_node_id,
                    seed_field=cfg.comfyui_seed_field,
                    timeout_sec=cfg.comfyui_timeout_sec,
                    poll_interval_sec=cfg.comfyui_poll_interval_sec,
                )
                logging.info("### Image backend: ComfyUI ###")
            except Exception as exception:
                logging.error("Image backend init failed (%s). Falling back to SD WebUI. %s", image_backend, exception)
                self.ImageApi = SdApi(cfg.sd_api_url, cfg.sd_config_file_path)
                self.image_backend = "sd_webui"
        else:
            self.ImageApi = SdApi(cfg.sd_api_url, cfg.sd_config_file_path)
            logging.info("### Image backend: SD WebUI ###")
        # Load user rules
        if exists(cfg.user_rules_file_path):
            with open(normpath(cfg.user_rules_file_path), "r") as user_rules_file:
                self.user_rules = json.loads(user_rules_file.read())
        else:
            logging.error("Cant find user_rules_file_path: " + cfg.user_rules_file_path)
            self.user_rules = {}
        # initiate generator
        tp.generator.init(
            cfg.generator_script,
            cfg.llm_path,
            n_ctx=cfg.generation_params.get("chat_prompt_size", 1024),
            n_gpu_layers=cfg.generation_params.get("n_gpu_layers", 0),
        )
        logging.info(f"### TelegramBotWrapper INIT DONE ###")
        logging.info(f"### !!! READY !!! ###")

    @staticmethod
    def _slugify_voice_name(name: str) -> str:
        raw = (name or "").strip().lower()
        cleaned = []
        for ch in raw:
            if ch.isascii() and ch.isalnum():
                cleaned.append(ch)
            elif ch in {" ", "-", "_"}:
                cleaned.append("_")
        slug = "".join(cleaned)
        slug = re.sub(r"_+", "_", slug).strip("_")
        return slug or "voice"

    def _voice_ref_base(self, chat_id: int, voice_type: str, char_name: str | None = None) -> Path:
        base_dir = Path(cfg.history_dir_path) / "voice_refs" / "shared"
        if voice_type == "char":
            slug = self._slugify_voice_name(char_name or "character")
            return base_dir / "chars" / slug
        if voice_type == "narrator":
            return base_dir / "narrator"
        return base_dir / "default"

    def _find_voice_ref(self, chat_id: int, voice_type: str, char_name: str | None = None) -> str | None:
        base = self._voice_ref_base(chat_id, voice_type, char_name)
        wav_path = base.with_suffix(".wav")
        if wav_path.exists():
            return str(wav_path)
        if base.parent.exists():
            for candidate in sorted(base.parent.glob(base.stem + ".*")):
                if candidate.is_file():
                    return str(candidate)
        if voice_type == "default":
            legacy_dir = Path(cfg.history_dir_path) / "voice_refs"
            legacy_wav = legacy_dir / f"{chat_id}_ref.wav"
            if legacy_wav.exists():
                return str(legacy_wav)
            if legacy_dir.exists():
                for candidate in sorted(legacy_dir.glob(f"{chat_id}_ref.*")):
                    if candidate.is_file():
                        return str(candidate)
        return None

    def _has_any_voice_files(self, chat_id: int) -> bool:
        if self._find_voice_ref(chat_id, "default"):
            return True
        if self._find_voice_ref(chat_id, "narrator"):
            return True
        char_dir = self._voice_ref_base(chat_id, "char", "character").parent
        if char_dir.exists():
            for entry in char_dir.iterdir():
                if entry.is_file():
                    return True
        return False

    # =============================================================================
    # Run bot with token! Initiate updater obj!
    async def run_telegram_bot(self, bot_token="", token_file_name=""):
        """
        Start the Telegram bot.
        Args:
            param bot_token: (str) The Telegram bot tokens separated by ','
                                If not provided, try to read it from `token_file_name`.
            param token_file_name: (str) The name of the file containing the bot token. Default is `None`.
        :return: None
        """
        if not bot_token:
            token_file_name = token_file_name or cfg.token_file_path
            with open(normpath(token_file_name), "r", encoding="utf-8") as f:
                bot_token = f.read().strip()
        if cfg.proxy_url:
            session = AiohttpSession(proxy="protocol://host:port/")
        else:
            session = None
        self.bot = Bot(token=bot_token, session=session)
        self.dp = Dispatcher()
        self.dp.message.register(self.thread_welcome_message, Command("start"))
        self.dp.message.register(self.thread_voice_clone_command, Command("voice_clone"))
        self.dp.message.register(self.thread_voice_narrator_command, Command("voice_narrator"))
        self.dp.message.register(self.thread_voice_character_command, Command("voice_char"))
        self.dp.message.register(self.thread_voice_language_command, Command("voice_lang"))
        self.dp.message.register(self.thread_voice_language_command, Command("voice_language"))
        self.dp.message.register(self.thread_get_message)
        self.dp.message.register(self.thread_get_document)
        self.dp.callback_query.register(self.thread_push_button)
        await self.dp.start_polling(self.bot)

    # =============================================================================
    # Additional telegram actions

    @staticmethod
    def get_user_profile_name(message) -> str:
        message = message or message.message
        user_name = cfg.user_name_template
        user_name = user_name.replace("FIRSTNAME", message.from_user.first_name or "")
        user_name = user_name.replace("LASTNAME", message.from_user.last_name or "")
        user_name = user_name.replace("USERNAME", message.from_user.username or "")
        user_name = user_name.replace("ID", str(message.from_user.id) or "")
        return user_name

    async def make_template_message(self, request: str, chat_id: int, custom_string="") -> str:
        if chat_id in self.users:
            user = self.users[chat_id]
            if request in const.DEFAULT_MESSAGE_TEMPLATE:
                msg = const.DEFAULT_MESSAGE_TEMPLATE[request]
                msg = msg.replace("_CHAT_ID_", str(chat_id))
                msg = msg.replace("_NAME1_", user.name1)
                msg = msg.replace("_NAME2_", user.name2)
                msg = msg.replace("_CONTEXT_", user.context)
                msg = msg.replace("_GREETING_", user.greeting)
                msg = msg.replace("_CUSTOM_STRING_", custom_string)
                msg = msg.replace("_OPEN_TAG_", cfg.html_tag[0])
                msg = msg.replace("_CLOSE_TAG_", cfg.html_tag[1])
                msg = await utils.prepare_text(msg, user, "to_user")
                return msg
            else:
                return const.UNKNOWN_TEMPLATE
        else:
            return const.UNKNOWN_USER

    # =============================================================================
    # Work with history! Init/load/save functions

    async def get_json_save_file(self, message: Message, text_content: str, file_name: str):
        chat_id = message.chat.id
        if not utils.check_user_permission(chat_id):
            return False
        utils.init_check_user(self.users, chat_id)
        user = self.users[chat_id]

        user.from_json(text_content)
        if user.char_file == "":
            user.char_file = file_name

        last_message = user.last.outbound if user.messages else "<no message in history>"
        send_text = await self.make_template_message("hist_loaded", chat_id, last_message)
        await self.bot.send_message(
            chat_id=chat_id,
            text=send_text,
            reply_markup=self.get_initial_keyboard(chat_id, user),
            parse_mode="HTML",
        )

    async def start_send_typing_status(self, chat_id: int) -> Event:
        typing_active = Event()
        typing_active.set()
        asyncio.create_task(self.thread_typing_status(chat_id, typing_active))
        return typing_active

    async def thread_typing_status(self, chat_id: int, typing_active: Event):
        limit_counter = int(cfg.generation_timeout / 5)
        while typing_active.is_set() and limit_counter > 0:
            await self.bot.send_chat_action(chat_id=chat_id, action="typing")
            await asyncio.sleep(5)
            limit_counter -= 1

    @staticmethod
    def sanitize_image_prompt(text: str) -> str:
        if not text:
            return ""
        if "[TOOL_CALLS]" in text:
            text = text.split("[TOOL_CALLS]")[0]
        text = re.sub(r"<tool_calls>.*?</tool_calls>", " ", text, flags=re.DOTALL | re.IGNORECASE)
        for token in ["</s>", "<s>", "<|endoftext|>", "<|assistant|>", "<|user|>", "<|tool|>"]:
            text = text.replace(token, " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @backoff.on_exception(
        backoff.expo,
        (urllib3.exceptions.HTTPError, urllib3.exceptions.ConnectTimeoutError),
        max_time=10,
    )
    async def send_sd_image(self, message, answer: str, user_text: str):
        chat_id = message.chat.id
        try:
            answer = self.sanitize_image_prompt(answer)
            file_list = await self.ImageApi.get_image(answer)
            answer = answer.replace(cfg.sd_api_prompt_of.replace("OBJECT", user_text[1:].strip()), "")
            for char in ["[", "]", "{", "}", "(", ")", "*", '"', "'"]:
                answer = answer.replace(char, "")
            if len(answer) > 1023:
                answer = answer[:1023]
            if len(file_list) > 0:
                for image_path in file_list:
                    if exists(image_path):
                        photo = FSInputFile(path=image_path)
                        await self.bot.send_photo(caption=answer, chat_id=chat_id, photo=photo)
                        remove(image_path)
        except Exception as e:
            logging.error("send_sd_image: " + str(e))
            await self.bot.send_message(text=answer, chat_id=chat_id)

    @backoff.on_exception(
        backoff.expo,
        (urllib3.exceptions.HTTPError, urllib3.exceptions.ConnectTimeoutError),
        max_time=10,
    )
    async def clean_last_message_markup(self, chat_id: int, previous_factor=0):
        if chat_id in self.users and self.users[chat_id].messages:
            if len(self.users[chat_id].messages) > previous_factor:
                last_msg = self.users[chat_id].messages[-1 - previous_factor].msg_id
            else:
                return
            try:
                await self.bot.edit_message_reply_markup(chat_id=chat_id, message_id=last_msg, reply_markup=None)
            except Exception as exception:
                logging.info("clean_last_message_markup: " + str(exception))

    @backoff.on_exception(
        backoff.expo,
        (urllib3.exceptions.HTTPError, urllib3.exceptions.ConnectTimeoutError),
        max_time=10,
    )
    async def send_message(self, chat_id: int, text: str) -> Message:
        user = self.users[chat_id]
        text = await utils.prepare_text(text, user, "to_user")
        tts_engine = getattr(user, "tts_engine", "silero")
        logging.info(
            "TTS send_message start: engine=%s user=%s voice_clone_path=%s silero_speaker=%s",
            tts_engine,
            chat_id,
            getattr(user, "voice_clone_path", None),
            user.silero_speaker,
        )
        if tts_engine == "chatterbox":
            if ":" in text:
                audio_text = ":".join(text.split(":")[1:])
            else:
                audio_text = text
            voice_clone_path = getattr(user, "voice_clone_path", None)
            if not voice_clone_path:
                existing_default = self._find_voice_ref(chat_id, "default")
                if existing_default:
                    user.voice_clone_path = existing_default
                    voice_clone_path = existing_default
            if not getattr(user, "narrator_voice_path", None):
                existing_narrator = self._find_voice_ref(chat_id, "narrator")
                if existing_narrator:
                    user.narrator_voice_path = existing_narrator
            has_any_voice = (
                bool(voice_clone_path)
                or bool(getattr(user, "narrator_voice_path", None))
                or bool(getattr(user, "voice_map", {}))
                or self._has_any_voice_files(chat_id)
            )
            logging.info(
                "TTS chatterbox: voice_clone_path=%s exists=%s",
                voice_clone_path,
                os.path.exists(voice_clone_path) if voice_clone_path else False,
            )
            if not has_any_voice:
                logging.warning("TTS chatterbox: no voice references set for user=%s", chat_id)
                if getattr(user, "awaiting_voice_clone", False):
                    await self.bot.send_message(
                        text="Voice cloning is enabled, but no reference audio is set. "
                        "Send /voice_clone and then upload a short voice/audio clip.",
                        chat_id=chat_id,
                    )
                message = await self.bot.send_message(
                    text=text,
                    chat_id=chat_id,
                    parse_mode="HTML",
                    reply_markup=self.get_chat_keyboard(chat_id, True),
                )
            else:
                segments = self.parse_tts_segments(audio_text, user)
                missing_characters = []
                for seg in segments:
                    if seg["label"] and seg["label"].lower() not in user.voice_map:
                        missing_characters.append(seg["label"])
                for name in sorted(set(missing_characters)):
                    if name.lower() not in user.voice_prompted:
                        user.voice_prompted.add(name.lower())
                        await self.bot.send_message(
                            chat_id=chat_id,
                            text=f"No voice set for '{name}'. Send /voice_char {name} to assign one.",
                        )
                if any(seg.get("label") == "Narrator" for seg in segments):
                    if not user.narrator_voice_path and not user.narrator_prompted:
                        user.narrator_prompted = True
                        await self.bot.send_message(
                            chat_id=chat_id,
                            text="No narrator voice set. Send /voice_narrator to assign one.",
                        )

                send_text_separately = len(text) > TELEGRAM_CAPTION_LIMIT
                audio_paths = []
                for seg in segments:
                    chunks = self.split_tts_text(seg["text"])
                    for chunk in chunks:
                        audio_path = await self.chatterbox.get_audio(
                            text=chunk,
                            user_id=chat_id,
                            user=user,
                            voice_path=seg["voice_path"],
                        )
                        logging.info("TTS chatterbox: audio_path=%s user=%s", audio_path, chat_id)
                        if audio_path is None:
                            continue
                        audio_paths.append(audio_path)

                if audio_paths:
                    merged_path = self.concat_audio_paths(
                        audio_paths,
                        os.path.join(
                            cfg.history_dir_path,
                            "tts_audio",
                            f"{chat_id}_chatterbox_full_{int(time.time()*1000)}.wav",
                        ),
                    )
                    caption = None if send_text_separately else text
                    message = await self.bot.send_audio(
                        chat_id=chat_id,
                        audio=FSInputFile(merged_path),
                        caption=caption,
                        parse_mode="HTML" if caption else None,
                        reply_markup=self.get_chat_keyboard(chat_id, True),
                    )
                    if send_text_separately:
                        await self.bot.send_message(
                            text=text,
                            chat_id=chat_id,
                            parse_mode="HTML",
                            reply_markup=self.get_chat_keyboard(chat_id, True),
                        )
                else:
                    message = await self.bot.send_message(
                        text=text,
                        chat_id=chat_id,
                        parse_mode="HTML",
                        reply_markup=self.get_chat_keyboard(chat_id, True),
                    )
        elif tts_engine == "silero" and user.silero_speaker != "None" and user.silero_model_id != "None":
            if ":" in text:
                audio_text = ":".join(text.split(":")[1:])
            else:
                audio_text = text
            chunks = self.split_tts_text(audio_text)
            logging.info(
                "TTS silero: generating %s chunk(s) for user=%s",
                len(chunks),
                chat_id,
            )
            send_text_separately = len(text) > TELEGRAM_CAPTION_LIMIT
            audio_paths = []
            for idx, chunk in enumerate(chunks, start=1):
                logging.info(
                    "TTS silero: generating chunk %s/%s for user=%s len=%s",
                    idx,
                    len(chunks),
                    chat_id,
                    len(chunk),
                )
                audio_path = await self.silero.get_audio(text=chunk, user_id=chat_id, user=user)
                logging.info("TTS silero: audio_path=%s user=%s", audio_path, chat_id)
                if audio_path is None:
                    continue
                audio_paths.append(audio_path)
            if audio_paths:
                merged_path = self.concat_audio_paths(
                    audio_paths,
                    os.path.join(
                        cfg.history_dir_path,
                        "tts_audio",
                        f"{chat_id}_silero_full_{int(time.time()*1000)}.wav",
                    ),
                )
                caption = None if send_text_separately else text
                message = await self.bot.send_audio(
                    chat_id=chat_id,
                    audio=FSInputFile(merged_path),
                    caption=caption,
                    parse_mode="HTML" if caption else None,
                    reply_markup=self.get_chat_keyboard(chat_id, True),
                )
                if send_text_separately:
                    await self.bot.send_message(
                        text=text,
                        chat_id=chat_id,
                        parse_mode="HTML",
                        reply_markup=self.get_chat_keyboard(chat_id, True),
                    )
            else:
                message = await self.bot.send_message(
                    text=text,
                    chat_id=chat_id,
                    parse_mode="HTML",
                    reply_markup=self.get_chat_keyboard(chat_id, True),
                )
        else:
            message = await self.bot.send_message(
                text=text,
                chat_id=chat_id,
                parse_mode="HTML",
                reply_markup=self.get_chat_keyboard(chat_id, True),
            )
        return message

    @backoff.on_exception(
        backoff.expo,
        (urllib3.exceptions.HTTPError, urllib3.exceptions.ConnectTimeoutError),
        max_time=10,
    )
    async def edit_message(
            self,
            cbq,
            chat_id: int,
            text: str,
            message_id: int,
    ):
        user = self.users[chat_id]
        text = await utils.prepare_text(text, user, "to_user")
        logging.info("TTS edit_message start: user=%s", chat_id)
        if cbq.message.text is not None:
            await self.bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                parse_mode="HTML",
                message_id=message_id,
                reply_markup=self.get_chat_keyboard(chat_id),
            )
        if cbq.message.audio is not None:
            if ":" in text:
                audio_text = ":".join(text.split(":")[1:])
            else:
                audio_text = text
            tts_engine = getattr(user, "tts_engine", "silero")
            audio_path = None
            if tts_engine == "chatterbox" and getattr(user, "voice_clone_path", None):
                logging.info("TTS edit chatterbox: generating audio for user=%s", chat_id)
                audio_path = await self.chatterbox.get_audio(text=audio_text, user_id=chat_id, user=user)
            elif tts_engine == "silero" and user.silero_speaker != "None" and user.silero_model_id != "None":
                logging.info("TTS edit silero: generating audio for user=%s", chat_id)
                audio_path = await self.silero.get_audio(text=audio_text, user_id=chat_id, user=user)
            logging.info("TTS edit: audio_path=%s user=%s", audio_path, chat_id)
            if audio_path is not None:
                await self.bot.edit_message_media(
                    chat_id=chat_id,
                    media=InputMediaAudio(media=normpath(audio_path)),
                    message_id=message_id,
                    reply_markup=self.get_chat_keyboard(chat_id),
                )
        if cbq.message.caption is not None:
            if len(text) > TELEGRAM_CAPTION_LIMIT:
                logging.warning(
                    "TTS edit_message: caption too long (%s). Sending full text separately.",
                    len(text),
                )
                await self.bot.edit_message_caption(
                    chat_id=chat_id,
                    caption="⬇ Full text sent below (caption too long)",
                    message_id=message_id,
                    reply_markup=self.get_chat_keyboard(chat_id),
                )
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode="HTML",
                    reply_markup=self.get_chat_keyboard(chat_id),
                )
            else:
                await self.bot.edit_message_caption(
                    chat_id=chat_id,
                    caption=text,
                    parse_mode="HTML",
                    message_id=message_id,
                    reply_markup=self.get_chat_keyboard(chat_id),
                )

    # =============================================================================
    # Message handler
    async def thread_get_document(self, message: Message):
        if message.document is None:
            return
        if message.document.file_size > 16000000:
            return
        is_audio_doc = False
        if message.document.mime_type and message.document.mime_type.startswith("audio/"):
            is_audio_doc = True
        if message.document.file_name:
            ext = message.document.file_name.rsplit(".", 1)[-1].lower()
            if ext in {"wav", "mp3", "ogg", "opus", "flac", "m4a"}:
                is_audio_doc = True
        if is_audio_doc:
            handled = await self.thread_get_voice_reference(message)
            if handled:
                return
            return
        print("thread_get_document2")
        file_id = message.document.file_id
        file_name = message.document.file_name
        file_content = await self.bot.download(file_id)
        file_bytes = file_content.read()
        text_content = file_bytes.decode('utf-8')

        chat_id = message.chat.id
        utils.init_check_user(self.users, chat_id)
        user = self.users[chat_id]
        if user.validate_user_json(text_content):
            await self.get_json_save_file(message, text_content, file_name)
        else:
            await self.add_document_to_context(message, text_content, file_name)

    async def thread_voice_clone_command(self, message: types.Message):
        chat_id = message.chat.id
        if not utils.check_user_permission(chat_id):
            return False
        utils.init_check_user(self.users, chat_id)
        user = self.users[chat_id]
        text = (message.text or "").strip()
        replace = text.lower().endswith(" --replace")
        existing_path = self._find_voice_ref(chat_id, "default")
        if existing_path and not replace:
            user.tts_engine = "chatterbox"
            user.voice_clone_path = existing_path
            user.awaiting_voice_clone = False
            user.awaiting_voice_clone_type = ""
            try:
                user.save_user_history(chat_id, cfg.history_dir_path)
            except Exception as e:
                logging.warning("Failed to save user history after voice clone set: %s", e)
            await message.reply(
                "Existing default voice found and activated. "
                "Send /voice_clone --replace to overwrite it."
            )
            return True
        user.tts_engine = "chatterbox"
        user.awaiting_voice_clone = True
        user.awaiting_voice_clone_type = "default"
        await message.reply(
            "Voice cloning enabled. Please send a short voice/audio clip (5-15 seconds) "
            "to set the reference voice."
        )
        return True

    async def thread_voice_narrator_command(self, message: types.Message):
        chat_id = message.chat.id
        if not utils.check_user_permission(chat_id):
            return False
        utils.init_check_user(self.users, chat_id)
        user = self.users[chat_id]
        text = (message.text or "").strip()
        replace = text.lower().endswith(" --replace")
        existing_path = self._find_voice_ref(chat_id, "narrator")
        if existing_path and not replace:
            user.tts_engine = "chatterbox"
            user.narrator_voice_path = existing_path
            user.awaiting_voice_clone = False
            user.awaiting_voice_clone_type = ""
            try:
                user.save_user_history(chat_id, cfg.history_dir_path)
            except Exception as e:
                logging.warning("Failed to save user history after narrator voice set: %s", e)
            await message.reply(
                "Existing narrator voice found and activated. "
                "Send /voice_narrator --replace to overwrite it."
            )
            return True
        user.tts_engine = "chatterbox"
        user.awaiting_voice_clone = True
        user.awaiting_voice_clone_type = "narrator"
        await message.reply(
            "Narrator voice setup. Send a short voice/audio clip (5-15 seconds) "
            "to set the narrator voice."
        )
        return True

    async def thread_voice_character_command(self, message: types.Message):
        chat_id = message.chat.id
        if not utils.check_user_permission(chat_id):
            return False
        utils.init_check_user(self.users, chat_id)
        user = self.users[chat_id]
        text = message.text or ""
        replace = text.strip().lower().endswith(" --replace")
        if replace:
            text = text[: -len(" --replace")].rstrip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await message.reply("Usage: /voice_char <Character Name>")
            return True
        char_name = parts[1].strip()
        if len(char_name) > TTS_MAX_CHARACTER_NAME:
            char_name = char_name[:TTS_MAX_CHARACTER_NAME]
        existing_path = self._find_voice_ref(chat_id, "char", char_name)
        if existing_path and not replace:
            user.tts_engine = "chatterbox"
            user.voice_map[char_name.lower()] = existing_path
            user.awaiting_voice_clone = False
            user.awaiting_voice_clone_type = ""
            try:
                user.save_user_history(chat_id, cfg.history_dir_path)
            except Exception as e:
                logging.warning("Failed to save user history after character voice set: %s", e)
            await message.reply(
                f"Existing voice for '{char_name}' found and activated. "
                f"Send /voice_char {char_name} --replace to overwrite it."
            )
            return True
        user.tts_engine = "chatterbox"
        user.awaiting_voice_clone = True
        user.awaiting_voice_clone_type = f"char:{char_name}"
        await message.reply(
            f"Voice setup for '{char_name}'. Send a short voice/audio clip (5-15 seconds)."
        )
        return True

    def _persist_chatterbox_config(self) -> bool:
        config_path = getattr(cfg, "chatterbox_config_file_path", "")
        if not config_path:
            return False
        config_path = normpath(config_path)
        config_dir = os.path.dirname(config_path)
        if config_dir:
            os.makedirs(config_dir, exist_ok=True)
        config_data = {}
        if exists(config_path):
            try:
                with open(config_path, "r") as config_file:
                    config_data = json.loads(config_file.read())
            except Exception as e:
                logging.warning("Failed to read chatterbox config for update: %s", e)
                config_data = {}
        config_data.update(getattr(cfg, "chatterbox_settings", {}) or {})
        try:
            with open(config_path, "w") as config_file:
                json.dump(config_data, config_file, indent=2)
            return True
        except Exception as e:
            logging.warning("Failed to write chatterbox config: %s", e)
            return False

    async def thread_voice_language_command(self, message: types.Message):
        chat_id = message.chat.id
        if not utils.check_user_permission(chat_id):
            return False
        text = (message.text or "").strip()
        parts = text.split(maxsplit=1)
        current_language = (cfg.chatterbox_settings or {}).get("language_id", "auto")
        if len(parts) < 2 or not parts[1].strip():
            await message.reply(
                f"Chatterbox language is '{current_language}'. "
                "Usage: /voice_lang <language_code|auto|list>"
            )
            return True

        desired = parts[1].strip().lower()
        if desired in {"list", "help"}:
            supported = ", ".join(sorted(self.chatterbox.supported_languages))
            await message.reply(f"Supported languages: {supported}")
            return True

        if desired == "auto":
            cfg.chatterbox_settings["language_id"] = "auto"
            self._persist_chatterbox_config()
            await message.reply("Chatterbox language set to auto (uses user language).")
            return True

        mapped = self.chatterbox._map_language(desired)
        if mapped not in self.chatterbox.supported_languages:
            supported = ", ".join(sorted(self.chatterbox.supported_languages))
            await message.reply(
                f"Unsupported language '{desired}'. Supported languages: {supported}"
            )
            return True

        cfg.chatterbox_settings["language_id"] = mapped
        self._persist_chatterbox_config()
        await message.reply(f"Chatterbox language set to '{mapped}'.")
        return True

    async def thread_get_voice_reference(self, message: types.Message) -> bool:
        chat_id = message.chat.id
        if not utils.check_user_permission(chat_id):
            return False
        utils.init_check_user(self.users, chat_id)
        user = self.users[chat_id]
        if not getattr(user, "awaiting_voice_clone", False):
            return False

        file_id = None
        file_name = None
        mime_type = None
        if message.voice is not None:
            file_id = message.voice.file_id
            mime_type = message.voice.mime_type
        elif message.audio is not None:
            file_id = message.audio.file_id
            file_name = message.audio.file_name
            mime_type = message.audio.mime_type
        elif message.document is not None:
            file_id = message.document.file_id
            file_name = message.document.file_name
            mime_type = message.document.mime_type
        else:
            return False

        ext = None
        if file_name and "." in file_name:
            ext = file_name.rsplit(".", 1)[1].lower()
        if not ext and mime_type:
            ext = mimetypes.guess_extension(mime_type) or ""
            ext = ext.lstrip(".")
        if not ext:
            ext = "ogg"

        voice_type = getattr(user, "awaiting_voice_clone_type", "") or "default"
        char_name = None
        if voice_type.startswith("char:"):
            char_name = voice_type.split(":", 1)[1].strip()
            base_path = self._voice_ref_base(chat_id, "char", char_name)
        elif voice_type == "narrator":
            base_path = self._voice_ref_base(chat_id, "narrator")
        else:
            base_path = self._voice_ref_base(chat_id, "default")
        base_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path = base_path.with_suffix(f".{ext}")
        file_content = await self.bot.download(file_id)
        with raw_path.open("wb") as ref_file:
            ref_file.write(file_content.read())

        final_path = raw_path
        if ext != "wav" and shutil.which("ffmpeg"):
            wav_path = base_path.with_suffix(".wav")
            try:
                import subprocess

                subprocess.run(
                    ["ffmpeg", "-y", "-i", str(raw_path), "-ac", "1", "-ar", "48000", str(wav_path)],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                final_path = wav_path
            except Exception:
                final_path = raw_path

        if voice_type.startswith("char:"):
            if char_name:
                user.voice_map[char_name.lower()] = str(final_path)
        elif voice_type == "narrator":
            user.narrator_voice_path = str(final_path)
        else:
            user.voice_clone_path = str(final_path)
        user.tts_engine = "chatterbox"
        user.awaiting_voice_clone = False
        user.awaiting_voice_clone_type = ""
        try:
            user.save_user_history(chat_id, cfg.history_dir_path)
        except Exception as e:
            logging.warning("Failed to save user history after voice clone set: %s", e)
        if voice_type.startswith("char:"):
            await message.reply("Character voice saved. New TTS voice is active.")
        elif voice_type == "narrator":
            await message.reply("Narrator voice saved. New TTS voice is active.")
        else:
            await message.reply("Voice clone reference saved. New TTS voice is active.")
        return True

    async def add_document_to_context(self, message: Message, text_content: str, file_name: str):
        chat_id = message.chat.id
        utils.init_check_user(self.users, chat_id)
        user = self.users[chat_id]
        document_text = f"{file_name}:\n{text_content}\n\n"
        await tp.aget_answer(
            text_in="\n".join([cfg.permanent_add_context_prefixes[0], document_text]),
            user=user,
            bot_mode=cfg.bot_mode,
            generation_params=cfg.generation_params,
            name_in=self.get_user_profile_name(message),
        )
        await message.reply(text="File '" + file_name + "' added to context.")

    async def thread_welcome_message(self, message: types.Message):
        chat_id = message.chat.id
        if not utils.check_user_permission(chat_id):
            return False
        utils.init_check_user(self.users, chat_id)
        send_text = await self.make_template_message("char_loaded", chat_id)
        await self.bot.send_message(
            chat_id=chat_id,
            text=send_text,
            reply_to_message_id=None,
            reply_markup=self.get_initial_keyboard(chat_id, self.users[chat_id] if chat_id in self.users else None),
            parse_mode="HTML",
        )

    async def thread_get_message(self, message: types.Message):
        if message.document is not None:
            await self.thread_get_document(message)
            return True
        if message.voice is not None or message.audio is not None:
            handled = await self.thread_get_voice_reference(message)
            if not handled and utils.check_user_permission(message.chat.id):
                await message.reply(
                    "If you want to set a voice, use /voice_clone, /voice_narrator, or /voice_char <Name> "
                    "and then send a voice/audio clip."
                )
            return True

        user_text = message.text
        chat_id = message.chat.id
        utils.init_check_user(self.users, chat_id)
        user = self.users[chat_id]
        if not utils.check_user_permission(chat_id):
            return False
        if not user.check_flooding(cfg.flood_avoid_delay):
            return False

        if cfg.only_mention_in_chat and message.chat.type != "CHAT_PRIVATE":
            me = await self.bot.get_me()
            if "".join(["@", me["username"]]) in user_text:
                user_text = user_text.replace("".join(["@", me["username"]]), "")
            else:
                if user.messages:
                    user.last.outbound += user_text
                return

        if 1 > cfg.chance_to_get_answer > 0:
            if cfg.chance_to_get_answer > random.uniform(0, 1):
                if user.messages:
                    user.last.outbound += f"{self.get_user_profile_name(message)}: {user_text}"
                return

        typing = await self.start_send_typing_status(chat_id)
        try:
            if utils.check_user_rule(chat_id=chat_id, option=const.GET_MESSAGE) is not True:
                return False
            if not user_text.startswith(tuple(cfg.sd_api_prefixes)):
                user_text = await utils.prepare_text(user_text, user, "to_model")
            answer, system_message = await tp.aget_answer(
                text_in=user_text,
                user=user,
                bot_mode=cfg.bot_mode,
                generation_params=cfg.generation_params,
                name_in=self.get_user_profile_name(message),
            )
            if system_message == const.MSG_SYSTEM:
                await message.reply(text=answer)
            elif system_message == const.MSG_SD_API:
                user.truncate_last_message()
                await self.send_sd_image(message, answer, user_text)
            else:
                if system_message == const.MSG_DEL_LAST:
                    await message.delete()
                reply = await self.send_message(text=answer, chat_id=chat_id)
                await self.clean_last_message_markup(chat_id, 1)
                if user.messages:
                    user.last.msg_id = reply.message_id
                user.save_user_history(chat_id, cfg.history_dir_path)
        except Exception as e:
            logging.error("thread_get_message" + str(e) + str(e.args))
        finally:
            typing.clear()

    # =============================================================================
    # button
    async def thread_push_button(self, cbq: types.CallbackQuery):
        chat_id = cbq.message.chat.id
        msg_id = cbq.message.message_id
        option = cbq.data
        if not utils.check_user_permission(chat_id):
            return False
        typing = await self.start_send_typing_status(chat_id)
        await self.bot.answer_callback_query(cbq.id)
        try:
            utils.init_check_user(self.users, chat_id)
            if option in [const.BTN_IMPERSONATE, const.BTN_NEXT, const.BTN_CONTINUE, const.BTN_DEL_WORD,
                          const.BTN_REGEN, const.BTN_CUTOFF]:
                if not self.users[chat_id].messages:
                    await cbq.message.edit_reply_markup(reply_markup=None)
                    return
                elif msg_id != (self.users[chat_id].last.msg_id if self.users[chat_id].messages else 0):
                    await cbq.message.edit_reply_markup(reply_markup=None)
                    return
            await self.handle_button_option(option, chat_id, cbq)
            self.users[chat_id].save_user_history(chat_id, cfg.history_dir_path)
        except Exception as e:
            logging.error("thread_push_button " + str(e) + str(e.args))
        finally:
            typing.clear()

    async def handle_button_option(self, option, chat_id, cbq: types.CallbackQuery):
        if option == const.BTN_RESET and utils.check_user_rule(chat_id, option):
            await self.on_reset_history_button(cbq)
        if option == const.BTN_SWITCH_GREETING and utils.check_user_rule(chat_id, option):
            await self.on_switch_greeting_button(cbq)
        elif option == const.BTN_CONTINUE and utils.check_user_rule(chat_id, option):
            await self.on_continue_message_button(cbq)
        elif option == const.BTN_IMPERSONATE and utils.check_user_rule(chat_id, option):
            await self.on_impersonate_button(cbq)
        elif option == const.BTN_NEXT and utils.check_user_rule(chat_id, option):
            await self.on_next_message_button(cbq)
        elif option == const.BTN_IMPERSONATE_INIT and utils.check_user_rule(chat_id, option):
            await self.on_impersonate_button(cbq, initial=True)
        elif option == const.BTN_NEXT_INIT and utils.check_user_rule(chat_id, option):
            await self.on_next_message_button(cbq, initial=True)
        elif option == const.BTN_DEL_WORD and utils.check_user_rule(chat_id, option):
            await self.on_delete_word_button(cbq)
        elif option == const.BTN_PREVIOUS and utils.check_user_rule(chat_id, option):
            await self.on_previous_message_button(cbq)
        elif option == const.BTN_REGEN and utils.check_user_rule(chat_id, option):
            await self.on_regenerate_message_button(cbq)
        elif option == const.BTN_CUTOFF and utils.check_user_rule(chat_id, option):
            await self.on_delete_message_button(cbq)
        elif option == const.BTN_IMAGE and utils.check_user_rule(chat_id, option):
            await self.on_image_button(cbq)
        elif option == const.BTN_DOWNLOAD and utils.check_user_rule(chat_id, option):
            await self.on_download_json_button(cbq)
        elif option == const.BTN_OPTION and utils.check_user_rule(chat_id, option):
            await self.show_options_button(cbq)
        elif option == const.BTN_DELETE and utils.check_user_rule(chat_id, option):
            await self.on_delete_pressed_button(cbq)
        elif option == const.BTN_GET_LONG_TEXT_FILE:
            await self.on_get_long_text_as_file_button(cbq)
        elif option == const.BTN_GET_LONG_TEXT_MSG:
            await self.on_get_long_text_as_message_button(cbq)
        elif option.startswith(const.BTN_CHAR_LIST) and utils.check_user_rule(chat_id, option):
            await self.keyboard_characters_button(cbq, option=option)
        elif option.startswith(const.BTN_CHAR_LOAD) and utils.check_user_rule(chat_id, option):
            await self.load_character_button(cbq, option=option)
        elif option.startswith(const.BTN_PRESET_LIST) and utils.check_user_rule(chat_id, option):
            await self.keyboard_presets_button(cbq, option=option)
        elif option.startswith(const.BTN_PRESET_LOAD) and utils.check_user_rule(chat_id, option):
            await self.load_presets_button(cbq, option=option)
        elif option.startswith(const.BTN_MODEL_LIST) and utils.check_user_rule(chat_id, option):
            await self.on_keyboard_models_button(cbq, option=option)
        elif option.startswith(const.BTN_MODEL_LOAD) and utils.check_user_rule(chat_id, option):
            await self.on_load_model_button(cbq, option=option)
        elif option.startswith(const.BTN_LANG_LIST) and utils.check_user_rule(chat_id, option):
            await self.on_keyboard_language_button(cbq, option=option)
        elif option.startswith(const.BTN_LANG_LOAD) and utils.check_user_rule(chat_id, option):
            await self.on_load_language_button(cbq, option=option)
        elif option.startswith(const.BTN_VOICE_LIST) and utils.check_user_rule(chat_id, option):
            await self.on_keyboard_voice_button(cbq, option=option)
        elif option.startswith(const.BTN_VOICE_LOAD) and utils.check_user_rule(chat_id, option):
            await self.on_load_voice_button(cbq, option=option)

    async def show_options_button(self, cbq: CallbackQuery):
        chat_id = cbq.message.chat.id
        user = self.users[chat_id]
        send_text = utils.get_conversation_info(user)
        await self.bot.send_message(
            text=send_text,
            chat_id=chat_id,
            reply_markup=self.get_options_keyboard(chat_id, user),
            parse_mode="HTML",
        )

    async def on_delete_pressed_button(self, cbq):
        chat_id = cbq.message.chat.id
        message_id = cbq.message.message_id
        await self.bot.delete_message(chat_id=chat_id, message_id=message_id)

    async def on_impersonate_button(self, cbq, initial=False):
        chat_id = cbq.message.chat.id
        message_id = cbq.message.message_id
        user = self.users[chat_id]
        if initial:
            await self.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=self.get_options_keyboard(chat_id, user),
            )
        else:
            await self.clean_last_message_markup(chat_id)
        answer, _ = await tp.aget_answer(
            text_in=const.GENERATOR_MODE_IMPERSONATE,
            user=user,
            bot_mode=cfg.bot_mode,
            generation_params=cfg.generation_params,
            name_in=self.get_user_profile_name(cbq),
        )
        message = await self.send_message(text=answer, chat_id=chat_id)
        if user.messages:
            user.last.msg_id = message.message_id
        user.save_user_history(chat_id, cfg.history_dir_path)

    async def on_next_message_button(self, cbq, initial=False):
        chat_id = cbq.message.chat.id
        message_id = cbq.message.message_id
        user = self.users[chat_id]
        if initial:
            await self.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=self.get_options_keyboard(chat_id, user),
            )
        else:
            await self.clean_last_message_markup(chat_id)
        answer, _ = await tp.aget_answer(
            text_in=const.GENERATOR_MODE_NEXT,
            user=user,
            bot_mode=cfg.bot_mode,
            generation_params=cfg.generation_params,
            name_in=self.get_user_profile_name(cbq),
        )
        message = await self.send_message(text=answer, chat_id=chat_id)
        if user.messages:
            user.last.msg_id = message.message_id
        user.save_user_history(chat_id, cfg.history_dir_path)

    async def on_continue_message_button(self, cbq):
        chat_id = cbq.message.chat.id
        message = cbq.message
        user = self.users[chat_id]
        answer, _ = await tp.aget_answer(
            text_in=const.GENERATOR_MODE_CONTINUE,
            user=user,
            bot_mode=cfg.bot_mode,
            generation_params=cfg.generation_params,
            name_in=self.get_user_profile_name(cbq),
        )
        await self.edit_message(
            text=answer,
            chat_id=chat_id,
            message_id=message.message_id,
        )
        user.save_user_history(chat_id, cfg.history_dir_path)

    async def on_previous_message_button(self, cbq):
        chat_id = cbq.message.chat.id
        message = cbq.message
        user = self.users[chat_id]
        answer = user.back_to_previous_out(msg_id=message.message_id) if user.messages else None
        if answer is not None:
            await self.edit_message(text=answer, chat_id=chat_id, message_id=message.message_id, cbq=cbq)
        user.save_user_history(chat_id, cfg.history_dir_path)

    async def on_delete_word_button(self, cbq):
        chat_id = cbq.message.chat.id
        user = self.users[chat_id]
        answer, return_msg_action = await tp.aget_answer(
            text_in=const.GENERATOR_MODE_DEL_WORD,
            user=user,
            bot_mode=cfg.bot_mode,
            generation_params=cfg.generation_params,
            name_in=self.get_user_profile_name(cbq),
        )
        if return_msg_action != const.MSG_NOTHING_TO_DO and user.messages:
            await self.edit_message(text=answer, chat_id=chat_id, message_id=user.last.msg_id, cbq=cbq)
            user.save_user_history(chat_id, cfg.history_dir_path)

    async def on_regenerate_message_button(self, cbq):
        chat_id = cbq.message.chat.id
        msg = cbq.message
        user = self.users[chat_id]
        await self.clean_last_message_markup(chat_id)
        answer, _ = await tp.aget_answer(
            text_in=const.GENERATOR_MODE_REGENERATE,
            user=user,
            bot_mode=cfg.bot_mode,
            generation_params=cfg.generation_params,
            name_in=self.get_user_profile_name(cbq),
        )
        await self.edit_message(
            text=answer,
            chat_id=chat_id,
            message_id=msg.message_id,
            cbq=cbq,
        )
        user.save_user_history(chat_id, cfg.history_dir_path)

    async def on_image_button(self, cbq):
        chat_id = cbq.message.chat.id
        user = self.users[chat_id]
        await self.clean_last_message_markup(chat_id)
        image_prefix = cfg.sd_api_prefixes[0] if cfg.sd_api_prefixes else "📷"
        answer, return_msg_action = await tp.aget_answer(
            text_in=image_prefix,
            user=user,
            bot_mode=cfg.bot_mode,
            generation_params=cfg.generation_params,
            name_in=self.get_user_profile_name(cbq),
        )
        if return_msg_action == const.MSG_SD_API:
            user.truncate_last_message()
            await self.send_sd_image(cbq.message, answer, image_prefix)
        else:
            message = await self.send_message(text=answer, chat_id=chat_id)
            if user.messages:
                user.last.msg_id = message.message_id
        user.save_user_history(chat_id, cfg.history_dir_path)

    async def on_delete_message_button(self, cbq):
        chat_id = cbq.message.chat.id
        user = self.users[chat_id]
        if not user.messages:
            return

        last_msg_id = user.last.msg_id
        await self.bot.delete_message(chat_id=chat_id, message_id=last_msg_id)
        user.truncate_last_message()
        if user.messages:
            message_id = user.last.msg_id
            await self.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=self.get_chat_keyboard(chat_id),
            )
        user.save_user_history(chat_id, cfg.history_dir_path)

    async def on_download_json_button(self, cbq):
        chat_id = cbq.message.chat.id
        if chat_id not in self.users:
            return

        json_file = self.users[chat_id].to_json()
        await self.bot.send_document(
            chat_id=chat_id,
            document=BufferedInputFile(file=bytes(json_file, "utf-8"), filename=self.users[chat_id].name2 + ".json"),
        )

    async def on_get_long_text_as_message_button(self, cbq):
        chat_id = cbq.message.chat.id
        user = self.users[chat_id]
        message_length = 1995 if user.language != cfg.llm_lang else 3995
        lines = [user.last.outbound[i:i + message_length] for i in range(0, len(user.last.outbound), message_length)]
        for line in lines[1:]:
            prepared_line = await utils.prepare_text(line, user, "to_user")
            await self.bot.send_message(chat_id=chat_id, text=prepared_line, parse_mode="HTML",
                                        reply_markup=self.keyboard_raw_to_keyboard_tg(buttons.get_delete_keyboard()))

    async def on_get_long_text_as_file_button(self, cbq):
        chat_id = cbq.message.chat.id
        if chat_id not in self.users:
            return
        user_file = self.users[chat_id].last.outbound
        await self.bot.send_document(
            chat_id=chat_id,
            document=BufferedInputFile(file=bytes(user_file, "utf-8"), filename=self.users[chat_id].name2 + ".txt"),
        )

    async def on_reset_history_button(self, cbq):
        # check if it is a callback_query or a command
        if cbq:
            chat_id = cbq.message.chat.id
        else:
            chat_id = cbq.chat.id
        if chat_id not in self.users:
            return
        user = self.users[chat_id]
        if user.messages:
            await self.clean_last_message_markup(chat_id)
        user.reset()
        user.load_character_file(cfg.characters_dir_path, user.char_file)
        send_text = await self.make_template_message("mem_reset", chat_id)
        await self.bot.send_message(
            chat_id=chat_id,
            text=send_text,
            reply_markup=self.get_initial_keyboard(chat_id, user),
            parse_mode="HTML",
        )

    async def on_switch_greeting_button(self, cbq):
        # check if it is a callback_query or a command
        if cbq:
            chat_id = cbq.message.chat.id
        else:
            chat_id = cbq.chat.id
        if chat_id not in self.users:
            return
        user = self.users[chat_id]
        if not user.switch_greeting():
            return
        if user.messages:
            await self.clean_last_message_markup(chat_id)
        send_text = await self.make_template_message("mem_reset", chat_id)
        await self.bot.send_message(
            chat_id=chat_id,
            text=send_text,
            reply_markup=self.get_initial_keyboard(chat_id, user),
            parse_mode="HTML",
        )

    # =============================================================================
    # switching keyboard
    async def on_load_model_button(self, cbq, option: str):
        if tp.generator.get_model_list is not None:
            model_list = tp.generator.get_model_list()
            model_file = model_list[int(option.replace(const.BTN_MODEL_LOAD, ""))]
            chat_id = cbq.message.chat.id
            send_text = "Loading " + model_file + ". 🪄"
            message_id = cbq.message.message_id
            await self.bot.edit_message_text(
                text=send_text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode="HTML",
            )
            try:
                tp.generator.load_model(model_file)
                send_text = await self.make_template_message(
                    request="model_loaded", chat_id=chat_id, custom_string=model_file
                )
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=send_text,
                    parse_mode="HTML",
                    reply_markup=self.get_options_keyboard(
                        chat_id, self.users[chat_id] if chat_id in self.users else None
                    ),
                )
            except Exception as e:
                logging.error("model button error: " + str(e))
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text="Error during " + model_file + " loading. ⛔",
                    parse_mode="HTML",
                    reply_markup=self.get_options_keyboard(
                        chat_id, self.users[chat_id] if chat_id in self.users else None
                    ),
                )
                raise e

    async def on_keyboard_models_button(self, cbq, option: str):
        if tp.generator.get_model_list() is not None:
            chat_id = cbq.message.chat.id
            msg = cbq.message
            model_list = tp.generator.get_model_list()
            if option == const.BTN_MODEL_LIST + const.BTN_OPTION:
                await self.bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=msg.message_id,
                    reply_markup=self.get_options_keyboard(
                        chat_id, self.users[chat_id] if chat_id in self.users else None
                    ),
                )
                return
            shift = int(option.replace(const.BTN_MODEL_LIST, ""))
            characters_buttons = self.get_switch_keyboard(
                opt_list=model_list,
                shift=shift,
                data_list=const.BTN_MODEL_LIST,
                data_load=const.BTN_MODEL_LOAD,
            )
            await self.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=msg.message_id,
                reply_markup=characters_buttons,
            )

    async def load_presets_button(self, cbq, option: str):
        chat_id = cbq.message.chat.id
        preset_char_num = int(option.replace(const.BTN_PRESET_LOAD, ""))
        cfg.preset_file = utils.parse_presets_dir()[preset_char_num]
        cfg.load_preset(preset_file=cfg.preset_file)
        user = self.users[chat_id]
        send_text = utils.get_conversation_info(user)
        message_id = cbq.message.message_id
        await self.bot.edit_message_text(
            text=send_text,
            message_id=message_id,
            chat_id=chat_id,
            parse_mode="HTML",
            reply_markup=self.get_options_keyboard(chat_id, user),
        )

    async def keyboard_presets_button(self, cbq, option: str):
        chat_id = cbq.message.chat.id
        msg = cbq.message
        if option == const.BTN_PRESET_LIST + const.BTN_OPTION:
            await self.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=msg.message_id,
                reply_markup=self.get_options_keyboard(chat_id, self.users[chat_id] if chat_id in self.users else None),
            )
            return
        shift = int(option.replace(const.BTN_PRESET_LIST, ""))
        preset_list = utils.parse_presets_dir()
        characters_buttons = self.get_switch_keyboard(
            opt_list=preset_list,
            shift=shift,
            data_list=const.BTN_PRESET_LIST,
            data_load=const.BTN_PRESET_LOAD,
            keyboard_column=3,
        )
        await self.bot.edit_message_reply_markup(
            chat_id=chat_id, message_id=msg.message_id, reply_markup=characters_buttons
        )

    async def load_character_button(self, cbq, option: str):
        chat_id = cbq.message.chat.id
        char_num = int(option.replace(const.BTN_CHAR_LOAD, ""))
        char_list = utils.parse_characters_dir()
        await self.clean_last_message_markup(chat_id)
        utils.init_check_user(self.users, chat_id)
        user = self.users[chat_id]
        char_file = char_list[char_num]
        user.load_character_file(characters_dir_path=cfg.characters_dir_path, char_file=char_file)
        user.find_and_load_user_char_history(chat_id, cfg.history_dir_path)
        if user.messages:
            send_text = await self.make_template_message("hist_loaded", chat_id, user.last.outbound)
        else:
            send_text = await self.make_template_message("char_loaded", chat_id)
        await self.bot.send_message(
            text=send_text,
            chat_id=chat_id,
            parse_mode="HTML",
            reply_markup=self.get_initial_keyboard(chat_id, self.users[chat_id] if chat_id in self.users else None),
        )

    async def keyboard_characters_button(self, cbq, option: str):
        chat_id = cbq.message.chat.id
        msg = cbq.message
        if option == const.BTN_CHAR_LIST + const.BTN_OPTION:
            await self.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=msg.message_id,
                reply_markup=self.get_options_keyboard(chat_id, self.users[chat_id] if chat_id in self.users else None),
            )
            return
        shift = int(option.replace(const.BTN_CHAR_LIST, ""))
        char_list = utils.parse_characters_dir()
        if shift == -9999 and self.users[chat_id].char_file in char_list:
            shift = char_list.index(self.users[chat_id].char_file)
        characters_buttons = self.get_switch_keyboard(
            opt_list=char_list,
            shift=shift,
            data_list=const.BTN_CHAR_LIST,
            data_load=const.BTN_CHAR_LOAD,
        )
        await self.bot.edit_message_reply_markup(
            chat_id=chat_id, message_id=msg.message_id, reply_markup=characters_buttons
        )

    async def on_load_language_button(self, cbq, option: str):
        chat_id = cbq.message.chat.id
        message_id = cbq.message.message_id
        user = self.users[chat_id]
        lang_num = int(option.replace(const.BTN_LANG_LOAD, ""))
        language = list(cfg.language_dict.keys())[lang_num]
        self.users[chat_id].language = language
        send_text = utils.get_conversation_info(user)
        await self.bot.edit_message_text(
            text=send_text,
            message_id=message_id,
            chat_id=chat_id,
            parse_mode="HTML",
            reply_markup=self.get_options_keyboard(chat_id, user),
        )

    async def on_keyboard_language_button(self, cbq, option: str):
        chat_id = cbq.message.chat.id
        msg = cbq.message
        if option == const.BTN_LANG_LIST + const.BTN_OPTION:
            await self.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=msg.message_id,
                reply_markup=self.get_options_keyboard(chat_id, self.users[chat_id] if chat_id in self.users else None),
            )
            return
        shift = int(option.replace(const.BTN_LANG_LIST, ""))
        lang_buttons = self.get_switch_keyboard(
            opt_list=list(cfg.language_dict.keys()),
            shift=shift,
            data_list=const.BTN_LANG_LIST,
            data_load=const.BTN_LANG_LOAD,
            keyboard_column=4,
        )
        await self.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg.message_id, reply_markup=lang_buttons)

    @staticmethod
    def get_voice_option_lists(language: str):
        lang = language if language in Silero.voices else "en"
        male = Silero.voices[lang]["male"]
        female = Silero.voices[lang]["female"]
        values = ["None", "__CLONE__", "__NARRATOR__", "__CHAR__"] + male + female
        labels = (
            ["🔇None", "🧬Clone", "🎭Narrator", "👤Character"]
            + list(map(lambda x: x + "🚹", male))
            + list(map(lambda x: x + "🚺", female))
        )
        return values, labels

    @staticmethod
    def split_tts_text(text: str, max_chars: int = TTS_CHUNK_MAX_CHARS):
        text = text.strip()
        if not text:
            return []
        paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
        if not paragraphs:
            paragraphs = [text]
        chunks = []
        for para in paragraphs:
            if len(para) <= max_chars:
                chunks.append(para)
                continue
            sentences = re.split(r"(?<=[.!?])\s+", para)
            if len(sentences) == 1:
                sentences = [para]
            current = ""
            for sentence in sentences:
                sentence = sentence.strip()
                if not sentence:
                    continue
                if len(sentence) > max_chars:
                    if current:
                        chunks.append(current.strip())
                        current = ""
                    for i in range(0, len(sentence), max_chars):
                        part = sentence[i:i + max_chars].strip()
                        if part:
                            chunks.append(part)
                    continue
                if not current:
                    current = sentence
                elif len(current) + len(sentence) + 1 <= max_chars:
                    current = f"{current} {sentence}"
                else:
                    chunks.append(current.strip())
                    current = sentence
            if current:
                chunks.append(current.strip())
        return chunks

    @staticmethod
    def split_asterisk_segments(text: str):
        segments = []
        last = 0
        for match in re.finditer(r"\*([^*]+)\*", text):
            if match.start() > last:
                segments.append((text[last:match.start()], False))
            segments.append((match.group(1), True))
            last = match.end()
        if last < len(text):
            segments.append((text[last:], False))
        return segments

    def parse_tts_segments(self, text: str, user: User):
        segments = []
        default_voice_path = None
        if getattr(user, "user_id", 0):
            existing_default = self._find_voice_ref(user.user_id, "default")
            if existing_default:
                user.voice_clone_path = existing_default
                default_voice_path = existing_default
        if not default_voice_path:
            default_voice_path = getattr(user, "voice_clone_path", None)
        current_speaker = None
        for line in text.splitlines():
            line = line.rstrip()
            if not line.strip():
                continue
            match = re.match(r"^\s*([^:]{1,%s}):\s*(.*)$" % TTS_MAX_CHARACTER_NAME, line)
            if match:
                current_speaker = match.group(1).strip()
                line = match.group(2)
            for part, is_narrator in self.split_asterisk_segments(line):
                part = part.strip()
                if not part:
                    continue
                if is_narrator:
                    voice_path = None
                    if getattr(user, "user_id", 0):
                        existing_narrator = self._find_voice_ref(user.user_id, "narrator")
                        if existing_narrator:
                            user.narrator_voice_path = existing_narrator
                            voice_path = existing_narrator
                    if not voice_path:
                        voice_path = user.narrator_voice_path
                    if not voice_path:
                        voice_path = default_voice_path
                    label = "Narrator"
                else:
                    label = current_speaker
                    voice_path = None
                    if current_speaker:
                        if getattr(user, "user_id", 0):
                            existing_char = self._find_voice_ref(user.user_id, "char", current_speaker)
                            if existing_char:
                                user.voice_map[current_speaker.lower()] = existing_char
                                voice_path = existing_char
                        if not voice_path:
                            voice_path = user.voice_map.get(current_speaker.lower())
                    if not voice_path:
                        voice_path = default_voice_path
                segments.append(
                    {
                        "text": part,
                        "voice_path": voice_path,
                        "label": label,
                    }
                )
        if not segments:
            segments = [{"text": text.strip(), "voice_path": default_voice_path, "label": None}]
        merged = []
        for seg in segments:
            if (
                merged
                and seg["voice_path"] == merged[-1]["voice_path"]
                and seg["label"] == merged[-1]["label"]
            ):
                merged[-1]["text"] = f"{merged[-1]['text']} {seg['text']}".strip()
            else:
                merged.append(seg)
        return merged

    @staticmethod
    def concat_audio_paths(paths: list[str], out_path: str):
        if not paths:
            return None
        if len(paths) == 1:
            return paths[0]
        try:
            import torchaudio as ta
            import torch
        except Exception as e:
            logging.warning("TTS concat failed to import torchaudio/torch: %s", e)
            return paths[0]

        merged = None
        target_sr = None
        target_channels = None
        for path in paths:
            try:
                wav, sr = ta.load(path)
            except Exception as e:
                logging.warning("TTS concat failed to load %s: %s", path, e)
                continue
            if target_sr is None:
                target_sr = sr
            if sr != target_sr:
                wav = ta.functional.resample(wav, sr, target_sr)
            if target_channels is None:
                target_channels = wav.shape[0]
            if wav.shape[0] != target_channels:
                wav = wav.mean(dim=0, keepdim=True)
            merged = wav if merged is None else torch.cat([merged, wav], dim=1)
        if merged is None:
            return paths[0]
        out_dir = os.path.dirname(out_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        ta.save(out_path, merged, target_sr)
        return out_path

    async def on_load_voice_button(self, cbq, option: str):
        chat_id = cbq.message.chat.id
        user = self.users[chat_id]
        voice_values, _ = self.get_voice_option_lists(user.language)
        voice_num = int(option.replace(const.BTN_VOICE_LOAD, ""))
        if voice_num >= len(voice_values):
            return
        selected_voice = voice_values[voice_num]
        if selected_voice == "None":
            user.tts_engine = "none"
            user.silero_speaker = "None"
            user.silero_model_id = "None"
            user.awaiting_voice_clone = False
            user.awaiting_voice_clone_type = ""
        elif selected_voice == "__CLONE__":
            user.tts_engine = "chatterbox"
            if not getattr(user, "voice_clone_path", None):
                user.awaiting_voice_clone = True
                user.awaiting_voice_clone_type = "default"
                await self.bot.send_message(
                    chat_id=chat_id,
                    text="Send a short voice/audio clip (5-15 seconds) to set the clone voice.",
                )
        elif selected_voice == "__NARRATOR__":
            user.tts_engine = "chatterbox"
            user.awaiting_voice_clone = True
            user.awaiting_voice_clone_type = "narrator"
            await self.bot.send_message(
                chat_id=chat_id,
                text="Send a short voice/audio clip (5-15 seconds) to set the narrator voice.",
            )
        elif selected_voice == "__CHAR__":
            user.tts_engine = "chatterbox"
            user.awaiting_voice_clone = False
            user.awaiting_voice_clone_type = ""
            await self.bot.send_message(
                chat_id=chat_id,
                text="To set a character voice, use /voice_char <Character Name> and then send the voice clip.",
            )
        else:
            user.tts_engine = "silero"
            user.silero_speaker = selected_voice
            lang = user.language if user.language in Silero.voices else "en"
            user.silero_model_id = Silero.voices[lang]["model"]
            user.awaiting_voice_clone = False
            user.awaiting_voice_clone_type = ""
        try:
            user.save_user_history(chat_id, cfg.history_dir_path)
        except Exception as e:
            logging.warning("Failed to save user history after voice change: %s", e)
        send_text = utils.get_conversation_info(user)
        message_id = cbq.message.message_id
        await self.bot.edit_message_text(
            text=send_text,
            message_id=message_id,
            chat_id=chat_id,
            parse_mode="HTML",
            reply_markup=self.get_options_keyboard(chat_id, user),
        )

    async def on_keyboard_voice_button(self, cbq, option: str):
        chat_id = cbq.message.chat.id
        msg = cbq.message
        if option == const.BTN_VOICE_LIST + const.BTN_OPTION:
            await self.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=msg.message_id,
                reply_markup=self.get_options_keyboard(chat_id, self.users[chat_id] if chat_id in self.users else None),
            )
            return
        shift = int(option.replace(const.BTN_VOICE_LIST, ""))
        user = self.users[chat_id]
        _, voice_labels = self.get_voice_option_lists(user.language)
        voice_dict = voice_labels
        voice_buttons = self.get_switch_keyboard(
            opt_list=list(voice_dict),
            shift=shift,
            data_list=const.BTN_VOICE_LIST,
            data_load=const.BTN_VOICE_LOAD,
            keyboard_column=4,
        )
        await self.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg.message_id, reply_markup=voice_buttons)

    # =============================================================================
    # load characters char_file from ./characters
    def get_initial_keyboard(self, chat_id, user: User):
        options = buttons.get_options_keyboard(chat_id=chat_id, user=user)
        alter_greeting_exist = True if len(user.alternate_greetings) > 0 and not user.messages else False
        chat_actions = buttons.get_chat_init_keyboard(chat_id=chat_id, alter_greeting_exist=alter_greeting_exist)
        return self.keyboard_raw_to_keyboard_tg([options[0], chat_actions[0]])

    def get_options_keyboard(self, chat_id, user: User):
        return self.keyboard_raw_to_keyboard_tg(buttons.get_options_keyboard(chat_id=chat_id, user=user))

    def get_chat_keyboard(self, chat_id=0, no_previous=False):
        if chat_id in self.users:
            user = self.users[chat_id]
        else:
            user = None
        return self.keyboard_raw_to_keyboard_tg(buttons.get_chat_keyboard(chat_id, user, no_previous))

    def get_switch_keyboard(
            self,
            opt_list: list,
            shift: int,
            data_list: str,
            data_load: str,
            keyboard_rows=6,
            keyboard_column=2,
    ):
        return self.keyboard_raw_to_keyboard_tg(
            buttons.get_switch_keyboard(
                opt_list=opt_list,
                shift=shift,
                data_list=data_list,
                data_load=data_load,
                keyboard_rows=keyboard_rows,
                keyboard_column=keyboard_column,
            )
        )

    @staticmethod
    def keyboard_raw_to_keyboard_tg(keyboard_raw):
        keyboard_tg = []
        for buttons_row in keyboard_raw:
            keyboard_tg.append([])
            for button_dict in buttons_row:
                keyboard_tg[-1].append(InlineKeyboardButton(**button_dict))
        return InlineKeyboardMarkup(inline_keyboard=keyboard_tg)

