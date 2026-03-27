
![Image1](https://raw.githubusercontent.com/innightwolfsleep/llm_telegram_bot/refs/heads/main/manuals/images/textgen_telegram.png)

- wrAPPer for llama.cpp(default), exllama or transformers. 

- wrAPPer for [ollama](https://github.com/ollama/ollama) (config example - config/ollama_config.json)
  
- wrAPPer for llama.cpp server (config example - config/llamacpp_server.json)

- an EXTension for [oobabooga/text-generation-webui](https://github.com/oobabooga/text-generation-webui).s

Provide telegram chat with various additional functional like buttons, prefixes, voice/image generation.


---------------
HOW TO INSTALL (**standalone app**):
1) clone this repo  
```git clone https://github.com/innightwolfsleep/llm_telegram_bot ```
2) install requirements.  
```pip install -r llm_telegram_bot\requirements_app.txt```

HOW TO RUN (**standalone app**):
1) get bot token from https://t.me/BotFather 
2) copy bot token to `configs\telegram_token.txt`
3) move your model file to `models\`
4) set **model_path** to your model in `configs\app_config.json`
5) start `run.cmd`(windows) or `run.sh`(linux). 

(optional) to use exllama: 
```
git clone https://github.com/turboderp/exllama source\generators\exllama
pip install -r source\generators\exllama\requirements.txt
```

(optional) to use exllamav2: 
```
git clone https://github.com/turboderp/exllamav2 source\generators\exllamav2
cd source\generators\exllamav2
python setup.py install --user
```

(optional) to use llama.cpp with GPU acceleration reinstall abetlen/llama-cpp-python by guide: [llama-cpp-python#installation-with-hardware-acceleration](https://github.com/abetlen/llama-cpp-python#installation-with-hardware-acceleration) or [this guide for windows](https://github.com/abetlen/llama-cpp-python/discussions/871)

---------------
HOW TO INSTALL (**extension mode**):
1) obviously, install  [oobabooga/text-generation-webui](https://github.com/oobabooga/text-generation-webui) first, add model, set all options you need
2) run `cmd_windows.bat` or `cmd_linux.sh` to enable venv
3) clone this repo to "text-generation-webui\extensions"  
`git clone https://github.com/innightwolfsleep/llm_telegram_bot extensions\telegram_bot`
4) install requirements  
`pip install -r extensions\telegram_bot\requirements_ext.txt`

HOW TO USE (**extension mode**):
1) get bot token from https://t.me/BotFather 
2) add your bot token in `extensions\telegram_bot\configs\telegram_token.txt` file or oobabooga environment
3) add flags to CMD_FLAGS.txt `--api --extension telegram_bot`
4) run appropriate start_OS script.
(direct integration is not available now)
---------------

HOW TO INSTALL/USE (**google collab**):
1) run notebook at [manuals/llm_telegram_bot_cublas.ipynb](https://colab.research.google.com/drive/1nTX1q7WRkXwSbLLCUs3clPL5eoJXShJq?usp=sharing)
2) install, set bot token, run
---------------
(optional) if you are facing internet issue, change `proxy_url` at `app_config.json` into your own proxy. For example: `https://127.0.0.1:10808`



FEATURES:
- chat templates (see [manuals/custom_prompt_templates.md])
- chat and notebook modes
- session for all users are separative (by chat_id)
- local session history - conversation won't be lost if server restarts. Separated history between users and chars.
- nice "X typing" during generating (users will not think that bot stuck)
- buttons: impersonate, continue previous message, regenerate last message, remove last messages from history, reset history button, new char loading menu
- you can load new characters from text-generation-webui\characters with button
- you can load new model during conversation with button
- "+" or "#" user message prefix for impersonate: "#Chiharu sister" or "+Castle guard". Or even ask bot generate your own message "+You"
- "-" or "!" prefix to replace last bot message
- "++" prefix replace bot name during chat (switch conversation to another character)
- "--" prefix replace you name during chat
- "==" prefix to add message to context
- "📷" prefix to make photo via SD WebUI or ComfyUI. Write like "📷Chiharu Yamada", not single "📷". For SD WebUI, run ([StableDiffusion](https://github.com/AUTOMATIC1111/stable-diffusion-webui)) with --api. For ComfyUI, run ComfyUI with the HTTP API enabled.
- save/load history in chat by downloading/forwarding to chat .json file
- integrated auto-translate (you can set model/user language parameter) 
- receiving text files (code, text, etc)
- voice generating ([silero](https://github.com/snakers4/silero-models)), en and ru variants
- voice cloning TTS ([Chatterbox Multilingual](https://github.com/resemble-ai/chatterbox)) via Voice menu -> Clone or `/voice_clone` (send 5-15s reference audio)
- translation_as_hidden_text option in .cfg - if you want to learn english with bot
- telegram_users.txt - list of permitted users (if empty - permit for all)
- antiflood - one message per 15 sec from one user
- improved group chatting mode

**Images**
Image generation is triggered by a prefix. The bot does not generate images unless a prefix is used.

How to generate an image:
1. Send a message that starts with one of `📷`, `📸`, `📹`, `🎥`, `📽`.
2. The rest of the message becomes the image prompt.
3. If you send only the prefix (for example `📷`), the bot uses `sd_api_prompt_self` to build a prompt.

Examples:
```text
📷A cozy wooden cabin in a snowy forest, warm window light, cinematic
```
```text
📷
```

SD WebUI (AUTOMATIC1111) setup:
1. Run SD WebUI with `--api`.
2. Set `image_backend` to `sd_webui`.
3. Set `sd_api_url` to your SD WebUI URL.
4. Tune `configs/sd_config.json` for your preferred quality and size.

Complete SD WebUI config example (`configs/sd_config.json`):
```json
{
	"prompt": "",
	"steps": 30,
	"cfg_scale": 6,
	"width": 768,
	"height": 768,
	"negative_prompt": "(worst quality, low quality:1.4), jpeg artifacts, signature, watermark, interlocked fingers"
}
```

ComfyUI setup:
1. Run ComfyUI with the HTTP API enabled.
2. Export your workflow in API format (ComfyUI `Save -> API`).
3. Set `image_backend` to `comfyui`.
4. Point `comfyui_workflow_file_path` to the exported API workflow.
5. Set `comfyui_prompt_node_id` to the node id of your `CLIPTextEncode` prompt node.

Complete ComfyUI config example (`configs/comfyui_config.json`):
```json
{
	"comfyui_url": "http://127.0.0.1:8188",
	"comfyui_workflow_file_path": "configs/comfyui_workflow.json",
	"comfyui_prompt_node_id": "",
	"comfyui_prompt_field": "text",
	"comfyui_negative_prompt": "",
	"comfyui_negative_prompt_node_id": "",
	"comfyui_negative_prompt_field": "text",
	"comfyui_seed_node_id": "",
	"comfyui_seed_field": "seed",
	"comfyui_timeout_sec": 120,
	"comfyui_poll_interval_sec": 1.0
}
```

Extension mode paths:
1. SD WebUI config: `extensions/telegram_bot/configs/sd_config.json`.
2. ComfyUI config: `extensions/telegram_bot/configs/comfyui_config.json`.
3. ComfyUI workflow: `extensions/telegram_bot/configs/comfyui_workflow.json`.

**Voice Cloning**
Voice cloning uses Chatterbox TTS. Once a voice is set, the bot sends audio replies.

You can also open the Voice menu: Options -> Voice.

Quick start (default clone):
1. Send `/voice_clone`.
2. Upload a 5-15 second voice or audio clip.
3. The bot will confirm the clone is active.

Narrator and character voices:
1. `/voice_narrator` to set the narrator voice.
2. `/voice_char <Character Name>` to set a voice for a specific speaker.

Examples:
```text
/voice_clone
/voice_narrator
/voice_char Alice
/voice_char Alice --replace
```

Notes:
1. Use `--replace` to overwrite an existing voice.
2. You can send `voice`, `audio`, or `document` files as the reference clip.
3. Voice references are stored under `history/voice_refs`.
4. To disable TTS, open the Voice menu and choose `None`.
5. You can force the TTS device by setting `TTS_DEVICE=cpu` or `TTS_DEVICE=cuda`.

CONFIGURATION:
`app_config.json` - config for running as standalone app (`run.sh` or `run.cmd`)  
`ext_config.json` - config for running as extension for [oobabooga/text-generation-webui](https://github.com/oobabooga/text-generation-webui)

```
x_config.json
    bot_mode=admin  
        specific bot mode. admin for personal use
            - admin - bot answer for everyone in chat-like mode. All buttons, include settings-for-all are avariable for everyone. (Default)
            - chat - bot answer for everyone in chat-like mode. All buttons, exclude settings-for-all are avariable for everyone. (Recommended for chatting)
            - chat-restricted - same as chat, but user can't change default character
            - persona - same as chat-restricted, but reset/regenerate/delete message are unavailable too. 
            - notebook - notebook-like mode. Prefixes wont added automaticaly, only "\n" separate user and bot messages. Restriction like chat mode.
            - query - same as notebook, but without history. Each question for bot is like new convrsation withot influence of previous questions
    user_name_template=
        user name template, useful for group chat.
        if empty bot always get default name of user - You. By default even in group chats bot perceive all users as single entity "You"
        but it is possible force bot to perceive telegram users names with templates: 
            FIRSTNAME - user first name (Jon)
            LASTNAME - user last name (Dow)
            USERNAME - user nickname (superguy)
            ID - user Id (999999999)
        so, user_name_template="USERNAME FIRSTNAME ID" translatede to user name "superguy Jon 999999999"
        but if you planed to use template and group chat - you shold add "\n" sign to stopping_strings to prevent bot impersonating!!!
    generator_script=GeneratorLlamaCpp
        name of generator script (generators folder):
            - generator_exllama - based on llama-cpp-python, recommended
            - generator_llama_cpp - based on llama-cpp-python, recommended
            - generator_llamacpp_server_chat - using llama.cpp server
            - generator_llamacpp_server_completions - using llama.cpp server
            - generator_ollama_chat - using ollama_chat server
            - generator_ollama_completions - using ollama_competition server
            - generator_langchain_llama_cpp - based in langchain+llama
            - generator_transformers - based on transformers, untested
            - generator_text_generator_webui_openapi - use oobabooga/text-generation-webui OpenAPI extension
            - (BROKEN) generator_text_generator_webui - module to integrate in oobabooga/text-generation-webui (curently broken:( )
            - (OUTDATED) generator_text_generator_webui_api - use oobabooga/text-generation-webui API (old api version)
    model_path=models\llama-13b.ggml.q4_0.gguf
        path to model file or directory
    characters_dir_path=characters
    default_char=Example.yaml
        default cahracter and path to cahracters folder
    presets_dir_path=presets
    default_preset=Shortwave.yaml
        default generation preset and path to preset folder
    model_lang=en
    user_lang=en
        default model and user language. User language can be switched by users, individualy.
    html_tag_open=<pre>
    html_tag_close=</pre>
        tags for bot answers in tg. By default - preformatted text (pre)
    history_dir_path=history
        directory for users history
    token_file_path=configs\\telegram_token.txt
        bot token. Ask https://t.me/BotFather
    admins_file_path=configs\\telegram_admins.txt
        users whos id's in admins_file switched to admin mode and can choose settings-for-all (generating settings and model)
    users_file_path=configs\\telegram_users.txt
        if just one id in users_file - bot will ignore all users except this id (id's). Even admin will be ignored
    generator_params_file_path=configs\\telegram_generator_params.json
        default text generation params, overwrites by choosen preset 
    user_rules_file_path=configs\\telegram_user_rules.json
        user rules matrix 
    telegram_sd_config=configs\\telegram_sd_config.json
        stable diffusion api config
    stopping_strings=<END>,<START>,end{code}
        generating settings - which text pattern stopping text generating? Add "\n" if bot sent too much text.
    eos_token=None
        generating settings
    translation_as_hidden_text=on
        if "on" and model/user lang not the same - translation will be writed under spoiler. If "off" - translation without spoiler, no original text in message.
    sd_api_url="http://127.0.0.1:7860"
    stable diffusion api url, need to use "photo" prefixes
    image_backend=sd_webui
        image backend: sd_webui or comfyui
    comfyui_config_file_path=configs\\comfyui_config.json
        comfyui config file (all comfyui settings)
    comfyui_url="http://127.0.0.1:8188"
        comfyui api url
    comfyui_workflow_file_path=configs\\comfyui_workflow.json
        comfyui workflow exported in API format (Save -> API)
    comfyui_prompt_node_id=
        node id to inject prompt into (CLIPTextEncode). Leave empty only if your workflow has a single prompt node
    comfyui_prompt_field=text
        field name for prompt input
    comfyui_negative_prompt=
        optional negative prompt text when node is configured
    comfyui_negative_prompt_node_id=
        node id to inject negative prompt
    comfyui_negative_prompt_field=text
        field name for negative prompt input
    comfyui_seed_node_id=
        node id to inject random seed
    comfyui_seed_field=seed
        field name for seed input
    comfyui_timeout_sec=120
        max seconds to wait for comfyui to finish
    comfyui_poll_interval_sec=1.0
        comfyui polling interval while waiting for results
    proxy_url
        to avoid provider blocking


generator_params.json
    config for generator 

sd_config.json
    config for stable diffusion

telegram_admins.txt
    list of users id who forced to admin mode. If telegram_users not empty - must be in telegram_users too!

telegram_users.txt
    list og users id (or groups id) who permitted interact with bot. If empty - everyone permitted

telegram_token.txt (you can use .env instead)
    telegram bot token

telegram_user_rules.json
    buttons visibility config for various bot modes

```
