# Native terminal integration

These patches add opt-in AMQ input to the existing Codex and Gemini terminal UIs. `feed.js` is a modern MCP subscription client. It emits typed notices over a private pipe, and the patched host submits them through its native message queue.

No keyboard input is injected. Peer notices remain literal text, human drafts stay in the composer, and busy terminals defer delivery through their normal queue. Native permissions and confirmation dialogs still apply.

## Feed contract

Set `AMQ_MCP_FEED` to the absolute executable path of `feed.js`. `bin/with-amq` sets it automatically and loads the credential matching `AM_ME`.

Each line is exactly one JSON object:

```json
{"type":"amq_message","content":"Incoming AMQ peer messages. Call read_message for each ID before acting. ..."}
```

The parser accepts only `type` and nonempty string `content`, with a maximum line size of 256 KiB. Notices contain bounded headers and IDs. They never consume mail; the model retrieves bodies through MCP tools. The feed announces an unread ID once per process, retries an offline service, and replays unread mail after a terminal restart.

## Codex 0.171.0

Base tag `rust-v0.171.0`, commit [`ccf9be8cc458bd6b8336fe84d68d8893f2691a5e`](https://github.com/openai/codex/tree/ccf9be8cc458bd6b8336fe84d68d8893f2691a5e).

Set `AMQ_MCP_DIR` to the absolute path of this checkout before changing directories. Build in a separate directory:

```sh
mkdir -p "$HOME/.cache/amq-mcp/builds"
cd "$HOME/.cache/amq-mcp/builds"
git clone --depth 1 --branch rust-v0.171.0 https://github.com/openai/codex.git
cd codex
git apply --check "$AMQ_MCP_DIR/integrations/codex-0.171.0.patch"
git apply "$AMQ_MCP_DIR/integrations/codex-0.171.0.patch"
cd codex-rs
CARGO_NET_GIT_FETCH_WITH_CLI=true cargo test -p codex-tui amq --lib --no-default-features
cargo build -p codex-cli -p codex-code-mode-host -p codex-exec-server
export CODEX_AMQ_BIN="$PWD/target/debug/codex"
```

The pinned source selects Rust 1.99.1. A release build is optional. If relocating the CLI, copy `codex-code-mode-host` and `codex-exec-server` beside it. The CLI alone starts but cannot execute tools without its tool host. Matching companions from the same upstream release also worked in the live tests.

The patch adds a private-feed reader, a native UI event, and a peer input source. Peer input stays literal, preserves the composer, queues while busy, and does not clear pending questions as a human follow-up would. Three targeted Rust tests passed. Formatting and the CLI build also passed during development. The companion executables used in the live test came from the matching upstream distribution.

## Gemini CLI 0.44.1

Base tag `v0.44.1`, commit [`b9b7416f861c1b10efc39c477dbe66ed8f2155a5`](https://github.com/google-gemini/gemini-cli/tree/b9b7416f861c1b10efc39c477dbe66ed8f2155a5).

```sh
mkdir -p "$HOME/.cache/amq-mcp/builds"
cd "$HOME/.cache/amq-mcp/builds"
git clone --depth 1 --branch v0.44.1 https://github.com/google-gemini/gemini-cli.git
cd gemini-cli
git apply --check "$AMQ_MCP_DIR/integrations/gemini-0.44.1.patch"
git apply "$AMQ_MCP_DIR/integrations/gemini-0.44.1.patch"
npm ci
npm run generate
npm run build --workspace packages/core
npm run build --workspace packages/devtools
npm run build --workspace packages/cli
npm exec -w packages/cli -- vitest run src/ui/hooks/useAmqInbox.test.ts src/ui/hooks/useMessageQueue.test.tsx
npm run bundle
export GEMINI_AMQ_BUNDLE="$PWD/bundle/gemini.js"
```

The patch starts the feed from a React effect and forwards validated notices to `useMessageQueue`. Cleanup terminates only its own feed child. The targeted suite includes three new feed tests and 17 existing queue tests. All 20 passed, as did the builds, bundle, and targeted lint checks.

The development build used `npm ci --ignore-scripts` and reused matching native PTY/keychain addons from an installed 0.44.1 distribution. A fresh source build should use the normal installation scripts to prepare those addons. Keep the complete bundle and its required native dependencies together when relocating it.

The unchanged upstream 0.44.1 dependency tree reported 94 advisory entries during development. This patch does not upgrade that dependency tree. The AMQ service's own dependency audit was clean at verification time.

## Optional macOS launch helpers

The manual setup in the [project README](../README.md) does not require `local.js` or `bin/launch.mjs`. Those files support an existing macOS LaunchAgent and per-handle provisioning. They do not install the service or replace your launchers automatically.

The default manifest path is `~/.local/state/amq-mcp/install.json`. Override it with `AMQ_MCP_INSTALL`. Use absolute paths, an owner-only manifest, and an owner-only credential file:

```json
{
  "root": "/absolute/path/to/mailboxes",
  "tokens": "/absolute/path/to/tokens.json",
  "url": "http://127.0.0.1:8765/mcp",
  "label": "local.amq-mcp",
  "amq": "/absolute/path/to/amq",
  "flock": "/absolute/path/to/flock",
  "binaries": {
    "claude": "/absolute/path/to/claude",
    "codex": "/absolute/path/to/patched/codex",
    "jcode": "/absolute/path/to/jcode"
  }
}
```

`label` must name an already-loaded LaunchAgent running this server against the same root and credential file. New handles receive a mailbox and credential under locks; the helper reloads the service to activate a new credential. Existing handles do not reload it. Reconnecting delivery clients retain unread mail in AMQ.

Invoke a configured helper with:

```sh
AM_ME=agent-a node "$AMQ_MCP_DIR/bin/launch.mjs" codex
```

The associated MCP entries are `node /absolute/path/to/local.js channel claude`, `node /absolute/path/to/local.js channel jcode`, and `node /absolute/path/to/local.js tools codex`. Use `shared: false` for the jcode entry.

Managed jcode sessions resolve the forwarded Yaran session ID through the AMQ ownership registry. An absent or ambiguous owner fails instead of inheriting a stale daemon identity. Standalone jcode launches use a stable private socket per handle until the shared daemon forwards `AM_ME`. An explicit `--socket` remains the caller's responsibility.

## Compatibility and attribution

Both patches are version-specific. Reapply and validate them when upgrading a terminal. Compiled harness binaries are not distributed here. Upstream copyrights and licenses are recorded in [third-party notices](../THIRD_PARTY_NOTICES.md).
