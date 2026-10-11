# telegram-coder

Telegram bot that drives an [`omp`](https://www.npmjs.com/package/@oh-my-pi/omp) session on your PC: you receive the work stream in real time, reply mid-turn (steer), and approve or deny shell commands via buttons.

```
Telegram  ──►  bridge (Python)  ──NDJSON──►  omp --mode rpc-ui
   ▲                │                              │
   └──── stream/edits/approvals ◄──────────────────┘
```

## Setup

### 1. Create the bot with @BotFather

The token **only** exists on Telegram, in a conversation with the [@BotFather](https://t.me/BotFather) — there is nowhere to look it up in this repository.

1. Open Telegram and search for `@BotFather` (verified account, blue).
2. Send `/newbot`.
3. Choose a display **name** (e.g. `My Coder`).
4. Choose a **username** ending in `bot` (e.g. `my_coder_bot`). It has to be unique on Telegram.
5. BotFather replies with the token, in the format `123456789:AAE...`. That whole line, including the numeric prefix and the colon, is the value of `TELEGRAM_BOT_TOKEN`.
6. Recommended tweaks in the same conversation:
   - `/setjoingroups` → your bot → **Disable** (the bridge is 1:1).
   - `/setprivacy` → **Enable**.
7. Paste the token into `.env`:

   ```
   TELEGRAM_BOT_TOKEN=123456789:AAE...
   ```

**The token is a password.** Whoever holds the token controls the bot and reads everything that passes through it. Do not post it in screenshots/issues. If it leaks, `/revoke` in BotFather generates another one and invalidates the old one.

If the token is wrong, the bridge fails immediately with `TELEGRAM_BOT_TOKEN inválido: o Telegram recusou o token do .env` (no traceback).

### 2. Find out your user id

The bot is public by `@username`; the only barrier is the allowlist by `from.id` in the code.

1. `python -m venv .venv`
2. `.venv\Scripts\pip install -r requirements.txt`
3. `copy .env.example .env` and fill in `TELEGRAM_BOT_TOKEN`.
4. Leave `TELEGRAM_ALLOWED_USER_ID=0` (or empty) and run `run.bat`. The bridge comes up in
   **setup mode**: it does not start omp and only answers `/id`.
5. Send `/id` to your bot on Telegram and copy the `user_id` from the reply.
6. Put that number into `TELEGRAM_ALLOWED_USER_ID` and run again.

### 3. Configure the target

`.env`:

|Key|Meaning|
|---|---|
|`TELEGRAM_BOT_TOKEN`|token from BotFather|
|`TELEGRAM_ALLOWED_USER_ID`|your numeric id; any other user is ignored|
|`OMP_BIN`|path to `omp` (`omp` if it is on PATH, or `C:/Users/.../omp.exe`)|
|`OMP_CWD`|**the project the agent will edit** (not the bridge directory)|
|`OMP_APPROVAL_MODE`|`write` = edits go straight through, `bash` asks for approval|
|`OMP_MODEL`|initial model (empty = whatever omp already uses)|
|`STREAM_EDIT_SECONDS`|minimum interval between edits of the stream message|
|`MAX_MSG_CHARS`|cut-off of the message being edited (default 3800, below the 4096 limit)|
|`DOC_THRESHOLD`|above this the full reply goes as a document|
|`DATA_DIR`|Telegram offset, session index, logs, omp sessions|

An empty `OMP_CWD` uses the bridge process cwd. A relative path is accepted and
resolved to absolute. Unquoted values accept a trailing comment
(`TELEGRAM_ALLOWED_USER_ID=123  # my id`).

### 4. Run

First confirm that the bridge to omp works — **this does not need the token**:

```bat
run.bat --selftest
```

Expected: the model's reply and `SELFTEST PASS`. Then start the bot:

```bat
run.bat
```

To test safely before pointing at a real project, point
`OMP_CWD` at an empty folder (e.g. `./sandbox`) and only switch when you are
comfortable.

## Structure

|File|Role|
|---|---|
|`bridge/config.py`|reading `.env` (custom parser) and path validation|
|`bridge/omp_client.py`|NDJSON client for `omp --mode rpc-ui` + restart supervisor|
|`bridge/streamer.py`|turn stream to Telegram: throttled edits, overflow, footer, document|
|`bridge/session_index.py`|local session index (name, usage) for `/sessions`|
|`bridge/app.py`|long polling, commands, callbacks and approval dialogs|
|`bridge/main.py`|logging, single-instance lock, `--selftest`|

The bridge speaks `omp --mode rpc-ui` (NDJSON over stdin/stdout). It requests
`set_event_filter` with `messageUpdates: "delta"` at startup, because full
`message_update` snapshots blow up large frames; if the command does not exist
in the installed build, the log warns and the bridge continues with full
snapshots.

## Usage

|Command|Effect|
|---|---|
|text|becomes a prompt; if the agent is running, it is delivered as a steer|
|photo|goes as an image to the model (the caption becomes the text)|
|`.txt`/`.md` document ≤ 64 KB|content becomes the prompt text|
|`/status`|model, thinking, session file, messages, context, state, mode|
|`/plan [task]`|turns on **plan mode**: investigates and proposes steps only|
|`/aprovar`|implements the proposed plan (back to normal mode)|
|`/ask [question]`|turns on **read-only mode** (or asks in it right away)|
|`/work`|back to normal mode|
|`/stop`|aborts the turn|
|`/new`|new session|
|`/sessions`|lists the bridge sessions; tap to switch|
|`/name <text>`|names the current session|
|`/model [query]`|lists the available models and switches by button|
|`/thinking [level]`|reasoning level (validated against the active model's levels)|
|`/compact`|compacts the context|
|`/id`|shows `chat_id` and `user_id`|
|`/cancel`|cancels a pending text dialog|

omp approval requests (tier `exec`) arrive as `Approve`/`Deny` buttons. The button message is **deleted** when you answer — the activity line already records the tool, so no `✅ Approve` piling up in the chat.

A turn uses at most two messages: the **reply** (edited as the text arrives) and the **activity**. During the turn the activity lists the tools live, one line each, with the most useful detail the frame carries — the intent declared by the model, or the relevant field (`path`, `command`), never the raw JSON:

```
⚙️ atividade
✓ write: Creating hello.py
✓ bash: Running hello.py
🔧 read: src/lib.rs
```

Each tool is updated in place when it finishes (`🔧` → `✓`, or `✗` on error), keeping the 15 most recent. When the turn ends, the list is **collapsed into a single line**:

```
⚙️ bash ×3 · read ×2 · write ×1 · 19s
```

That way the chat does not keep a wall of lines: during the work you see the detail, then only the summary remains.

While an omp text dialog is pending (`input`/`editor`), the next message you send is its answer; `/cancel` gives up.

The final reply is **rendered**: the Markdown the model writes becomes Telegram formatting. During the stream the text arrives raw — partial and with open formatting it would break the parser — and formatting is applied when each chunk is closed: when the turn ends and also for each block that exceeds the limit (a long reply becomes several messages, and **all** of them are formatted). If the model's Markdown is invalid, the message automatically falls back to plain text instead of failing.

Blocks are split at a **paragraph boundary** (or, failing that, a line boundary): splitting in the middle would leave a table cut in two, and the half without the header stops being recognized as a table.

Telegram has no table, so a Markdown table is rewritten as a list. With three columns or more, each value keeps its column label — without that, `| | CBD | THC |` would become "no · yes" and lose its meaning:

```
| | CBD | THC |                  ▸ Psychoactive — CBD: no · THC: yes
|---|---|---|            →
| Psychoactive | no | yes |       ▸ Risk — CBD: hepatotoxicity · THC: psychosis
| Risk | hepatotoxicity | psychosis |
```

Also translated: `**bold**`/`__bold__`, `*italic*`/`_italic_`, `` `code` ``, fenced blocks, `#` headings, `~~strikethrough~~`, `[text](url)`, `> quote` (blockquote) and `---` (separator). The model's HTML is escaped, so nothing it writes becomes a real tag.

Long code blocks (more than ~400 characters or 12 lines) are born **collapsed**, using Telegram's `<blockquote expandable>`: a 100-line JSON stays closed behind one tap, instead of becoming a wall in the chat. The language is preserved (`<pre><code class="language-json">`).

## First test, step by step

With `OMP_CWD` pointing at an empty folder and `run.bat` running:

|#|Send on Telegram|What should happen|
|---|---|---|
|1|`/status`|Block with model, thinking, session file name, `0` messages, `estado: ocioso`|
|2|`/id`|`user_id` matching the one in `.env`|
|3|`create hello.py with print("hi") and then run it with bash`|One message gets **edited** (not several), `⚙️ atividade` appears with `✓ write: ...`, then an `Allow tool: bash` button|
|4|Tap **Approve**|`✓ bash: ...` in the activity, `hi` in the text, footer `— ✅ pronto em Ns · <modelo> · ctx N%`|
|5|`run bash: echo test` → **Deny**|`✗ bash` and the agent says the tool was denied|
|6|`count from 1 to 100` and, **while it runs**, `switch to hi`|Immediate reply `↩︎ steer enviado`; the turn continues and incorporates the change, without starting another|
|7|`/stop` during a turn|`🛑 abortado` and the stream stops|
|8|`/new` then `/name test` then `/sessions`|New session created, `test` in the list with `✅`; tapping the other session brings the history back|
|9|`/model flash` and tap one|Model keyboard; `/status` confirms the new `provider/id`|
|10|`/thinking high`|`🧠 thinking: high` and `/status` confirms|
|11|`/plan create a file notas.md with hello`|Mode `📋 plano`; the agent only investigates and proposes steps; `notas.md` is **not** created|
|12|`/aprovar`|The work agent implements the plan; `notas.md` appears|
|13|`/ask what is in this repo?`|Read-only answer; `/status` shows `🔍 ask`|
|14|Send a photo|The agent describes the image|
|15|Send a small `.md`|The agent receives the content as text|
|16|`Ctrl+C` in the bridge and `run.bat` again|No old message is reprocessed|

If something hangs, `data/bridge.log` and `data/omp-stderr.log` show what happened.

## `/plan` mode (propose only)

`/plan` uses the same read-only process as `/ask` (so nothing can be touched) plus a planning instruction, and remembers the plan so you can approve it:

- `/plan` — turns the mode on
- `/plan rename dobro to triplo and fix the callers` — turns it on and plans right away
- `/aprovar` — sends the plan back to the work agent, which implements it in normal mode
- `/work` — leaves the mode and discards the plan

`omp`'s own plan mode is not reachable from RPC: it is a TUI toggle (`Alt+Shift+P`), `plan.defaultOnStartup` is ignored over RPC (verified: a `--config` overlay setting it still let `write` through), and `--plan-yolo` engages it but auto-approves the plan, which defeats the review. So the bridge implements the equivalent with what *is* enforceable — the read-only tool list — and the approval step is yours, in the chat.

The plan the agent registers with the `todo` tool gets its own message, updated in place as steps complete:

```
📋 plano · 1/3

1. Rename in util.py
  ✅ Rename `dobro` to `triplo`
  🔄 Run the tests

2. Update callers
  ⬜ Map every usage with grep
```

Before, that frame was rendered as `🔧 todo: {"op": "create", ...}` — a wall of raw JSON. The plan itself lives in its own message; the summary only counts the `todo` call (`todo ×1`), never its arguments.

## `/ask` mode (read-only)

`/ask` starts a second omp process with **read-only and research** tools:

```
omp --mode rpc-ui --cwd <project> --session-dir data/ask-sessions --tools=read,glob,grep,web_search,lsp
```

The isolation is guaranteed by omp itself (`--tools=`), not the bridge: `write`, `edit`, `bash` and `task` simply do not exist in that session, so no prompt can change the repository or run commands. Ask-mode sessions live in `data/ask-sessions/`, separate from the work ones, and do not appear in `/sessions`.

- `/ask` — turns the mode on (the process starts the first time)
- `/ask what is the capital of France?` — turns it on and asks right away
- `/work` — back to normal mode

`/status` shows which mode you are in. `--no-tools` would also block everything, but it was discarded: with no tool at all the model starts *writing* the tool-call syntax as text, which pollutes the reply.

## Operation

- **One instance per token.** The bridge writes `data/bridge.lock`; a second instance exits with `já existe uma instância rodando (pid N)`. Two `getUpdates` on the same token would give `409 Conflict`.
- **Persisted offset.** `data/telegram-offset.json` is only written after the update has been handled; restarting does not reprocess messages.
- **Sessions.** `data/sessions/` is private to the bot (`--session-dir`), and `data/session-index.json` stores each one's label/usage.
- **omp crash.** The supervisor restarts with backoff 2→30 s and notifies the chat. If the binary does not come back within 5 attempts, the bridge exits with code 1.
- **Rate limit.** Telegram accepts ~1 message per second per chat and edits count toward the same limit. The stream respects `STREAM_EDIT_SECONDS`, the activity line has its own throttle and there is a shared floor of 1 s between the two; `429` is honored with `retry_after`.
- **Steering.** A message sent while the turn is running goes as `streamingBehavior: "steer"`. If the turn ends between reading the state and sending, it automatically falls back as a normal prompt.
- **Background work.** If omp wakes the agent after the `prompt_result` (auto-backgrounded bash, `task`, `eval`), the bridge opens a new stream message for that run instead of discarding the output.
- **Logs.** `data/bridge.log` (rotating, 3 backups) and `data/omp-stderr.log`.

## Tests

Unit tests (no network, no omp):

```bat
.venv\Scripts\python -m pytest -q
```

End-to-end smoke against a fake Bot API server and the real `omp` (creates a file, approves/denies `bash`, steer, model/session switch, photo, document, `/stop`, allowlist, offset):

```bat
.venv\Scripts\python smoke_e2e.py
.venv\Scripts\python smoke_failures.py
```

Both create `data/smoke-*` with a timestamp and print `SMOKE PASS` / `SMOKE FAILURES PASS`.
