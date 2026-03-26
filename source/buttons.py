from typing import List, Dict, Optional

try:
    import extensions.telegram_bot.source.text_process as tp
    import extensions.telegram_bot.source.const as const
    import extensions.telegram_bot.source.utils as utils
    from extensions.telegram_bot.source.conf import cfg
    from extensions.telegram_bot.source.user import User as User
except ImportError:
    import source.text_process as tp
    import source.const as const
    import source.utils as utils
    from source.conf import cfg
    from source.user import User as User


def get_options_keyboard(chat_id, user: User):
    keyboard_raw = []
    # get language
    if user is not None:
        language = user.language
    else:
        language = "en"
    language_flag = cfg.language_dict[language]
    # get voice
    if user is not None:
        tts_engine = getattr(user, "tts_engine", "silero")
        if tts_engine == "chatterbox":
            voice = "🔈" if getattr(user, "voice_clone_path", "") else "🔇"
        elif tts_engine == "silero":
            voice = "🔈" if user.silero_speaker != "None" else "🔇"
        else:
            voice = "🔇"
    else:
        voice = "🔇"

    if utils.check_user_rule(chat_id, const.BTN_DOWNLOAD):
        keyboard_raw.append({"text": "💾Save", "callback_data": const.BTN_DOWNLOAD})
    if utils.check_user_rule(chat_id, const.BTN_CHAR_LIST):
        keyboard_raw.append({"text": "🎭Chars", "callback_data": const.BTN_CHAR_LIST + "-9999"})
    if utils.check_user_rule(chat_id, const.BTN_RESET):
        keyboard_raw.append({"text": "⚠Reset", "callback_data": const.BTN_RESET})
    if utils.check_user_rule(chat_id, const.BTN_LANG_LIST):
        keyboard_raw.append({"text": language_flag + "Language", "callback_data": const.BTN_LANG_LIST + "0"})
    if utils.check_user_rule(chat_id, const.BTN_VOICE_LIST):
        keyboard_raw.append({"text": voice + "Voice", "callback_data": const.BTN_VOICE_LIST + "0"})
    if utils.check_user_rule(chat_id, const.BTN_PRESET_LIST) and tp.generator.generator.preset_change_allowed:
        keyboard_raw.append({"text": "🔧Presets", "callback_data": const.BTN_PRESET_LIST + "0"})
    if utils.check_user_rule(chat_id, const.BTN_MODEL_LIST) and tp.generator.generator.model_change_allowed:
        keyboard_raw.append({"text": "🔨Model", "callback_data": const.BTN_MODEL_LIST + "0"})
    if utils.check_user_rule(chat_id, const.BTN_DELETE):
        keyboard_raw.append({"text": "❌Delete", "callback_data": const.BTN_DELETE})
    return [keyboard_raw]


def get_chat_keyboard(chat_id, user: Optional[User], no_previous=False):
    keyboard = []
    keyboard_row = []
    if utils.check_user_rule(chat_id, const.BTN_IMPERSONATE):
        keyboard_row.append({"text": "🥸Impersonate", "callback_data": const.BTN_IMPERSONATE})
    if utils.check_user_rule(chat_id, const.BTN_NEXT):
        keyboard_row.append({"text": "▶Next", "callback_data": const.BTN_NEXT})
    if utils.check_user_rule(chat_id, const.BTN_DEL_WORD):
        keyboard_row.append({"text": "⬅Del sentence", "callback_data": const.BTN_DEL_WORD})

    # Previous variant button logic
    previous_button_enabled = False
    if not no_previous and user and user.messages:
        last_msg = user.messages[-1]
        if last_msg.previous_out:
            previous_button_enabled = True

    if utils.check_user_rule(chat_id, const.BTN_PREVIOUS):
        if previous_button_enabled:
            keyboard_row.append({"text": "↪️Previous variant", "callback_data": const.BTN_PREVIOUS})
        else:
            keyboard_row.append({"text": "-", "callback_data": "none"})

    if utils.check_user_rule(chat_id, const.BTN_REGEN):
        keyboard_row.append({"text": "🔄Regenerate", "callback_data": const.BTN_REGEN})
    if utils.check_user_rule(chat_id, const.BTN_OPTION):
        keyboard_row.append({"text": "⚙Options", "callback_data": const.BTN_OPTION})
    if utils.check_user_rule(chat_id, const.BTN_CUTOFF):
        keyboard_row.append({"text": "❌Delete", "callback_data": const.BTN_CUTOFF})
    keyboard.append(keyboard_row)
    if len(user.last.outbound) > 4000 or (user.language != cfg.llm_lang and len(user.last.outbound) > 2000):
        keyboard_row2 = [{"text": "💾🔻download full", "callback_data": const.BTN_GET_LONG_TEXT_FILE},
                         {"text": "✉️🔻send full", "callback_data": const.BTN_GET_LONG_TEXT_MSG}]
        keyboard.append(keyboard_row2)
    return keyboard


def get_delete_keyboard():
    keyboard = [[{"text": "❌Delete", "callback_data": const.BTN_DELETE}]]
    return keyboard


def get_chat_init_keyboard(chat_id=0, alter_greeting_exist=False):
    keyboard_raw = []
    if utils.check_user_rule(chat_id, const.BTN_IMPERSONATE):
        keyboard_raw.append({"text": "🥸Impersonate", "callback_data": const.BTN_IMPERSONATE_INIT})
    if utils.check_user_rule(chat_id, const.BTN_NEXT):
        keyboard_raw.append({"text": "▶Next", "callback_data": const.BTN_NEXT_INIT})
    if utils.check_user_rule(chat_id, const.BTN_SWITCH_GREETING):
        if alter_greeting_exist:
            keyboard_raw.append({"text": "🔀Switch greeting", "callback_data": const.BTN_SWITCH_GREETING})
        else:
            keyboard_raw.append({"text": "-", "callback_data": "empty"})
    return [keyboard_raw]


def get_sd_api_keyboard():
    keyboard_raw = [{"text": "🔄Regenerate", "callback_data": const.BTN_REGEN},
                    {"text": "❌Delete", "callback_data": const.BTN_CUTOFF}]
    return [keyboard_raw]


def get_switch_keyboard(
        opt_list: list,
        shift: int,
        data_list: str,
        data_load: str,
        keyboard_rows=6,
        keyboard_column=2,
):
    # find shift
    opt_list_length = len(opt_list)
    keyboard_length = keyboard_rows * keyboard_column
    if shift >= opt_list_length - keyboard_length:
        shift = opt_list_length - keyboard_length
    if shift < 0:
        shift = 0
    # append list
    characters_buttons: List[List[Dict]] = []
    column = 0
    for i in range(shift, keyboard_length + shift):
        if i >= len(opt_list):
            break
        if column == 0:
            characters_buttons.append([])
        column += 1
        if column >= keyboard_column:
            column = 0
        characters_buttons[-1].append({"text": f"{opt_list[i]}", "callback_data": f"{data_load}{str(i)}"})
        i += 1
    # add switch buttons
    ordinary_shift = keyboard_length
    improved_shift = int(opt_list_length / 8) if opt_list_length / (keyboard_length * 3) > 8 else keyboard_length * 3
    begin_shift = 0
    l_shift = shift - ordinary_shift
    l_shift3 = shift - improved_shift
    r_shift = shift + ordinary_shift
    r_shift3 = shift + improved_shift
    end_shift = opt_list_length - keyboard_length
    switch_buttons = [
        {"text": "⏮", "callback_data": data_list + str(begin_shift)},
        {"text": "⏪", "callback_data": data_list + str(l_shift3)},
        {"text": "◀", "callback_data": data_list + str(l_shift)},
        {"text": "🔺", "callback_data": data_list + const.BTN_OPTION},
        {"text": "▶", "callback_data": data_list + str(r_shift)},
        {"text": "⏩", "callback_data": data_list + str(r_shift3)},
        {"text": "⏭", "callback_data": data_list + str(end_shift)},
    ]
    characters_buttons.append(switch_buttons)
    return characters_buttons
