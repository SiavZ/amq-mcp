# AMQ MCP

Give coding agents a shared mailbox through MCP, with automatic incoming delivery in their native terminal interfaces.

AMQ MCP wraps the [AMQ CLI](https://github.com/avivsinai/agent-message-queue). Agents can discover peers, send messages, reply on existing threads, and read their inboxes through tools. Messages and receipts stay in AMQ's filesystem queue. Incoming notifications can wake an idle agent without typing into its terminal.

The HTTP service implements **MCP 2026-07-28** with the official Rust SDK, [`rmcp` 3.11](https://github.com/modelcontextprotocol/rust-sdk). It creates a fresh server for each request, authenticates the caller on every request, and assigns no MCP session ID. Older MCP clients can use the compatibility transports.

## Supported terminals

| Client | MCP tools | Automatic incoming delivery |
| --- | --- | --- |
| Claude Code | Yes | Native Claude channel through `amq-channel`. |
| jcode | Yes | Its existing per-session channel support through the same adapter. |
| Codex CLI | Yes | Included patch for the native 0.171.0 terminal, using `amq-inbox-feed`. |
| Gemini CLI | Yes | Included patch for the native 0.44.1 terminal, using `amq-inbox-feed`. |
| Other MCP clients | Yes | The host must connect subscription notifications to its input queue. |

Stock Codex and Gemini terminals need the supplied patches for automatic wake. Tools work independently of those patches. Native approvals remain under the terminal's control, and an unsent human draft is preserved. After installing a patched terminal, restart it and resume the saved conversation if needed.

The Rust transport passes 22 tests plus TypeScript SDK interoperability checks. The earlier authenticated receive, read, and reply tests passed in Claude, Codex, and jcode before the runtime migration. Gemini received its notification and automatically started a turn, but its saved provider key was rejected, so its model-driven read and reply remain unverified. See [verification](integrations/verification.md).

## Requirements

- Rust 1.90 or newer to build; the compiled binaries need no Rust, Node.js, or npm installation.
- The `amq` CLI on PATH. Tested with AMQ 0.76.0; select another executable with `AMQ_BIN`.
- A mailbox handle for each concurrent agent.

The optional native macOS launcher uses an existing local LaunchAgent. File locking uses the Rust standard library; `flock` is no longer required.

## Upgrading from the JavaScript version

Keep your existing AMQ queue and credential file. Replace `node server.js` with `amq-mcp`, `node channel.js` with `amq-channel`, and the feed executable with `amq-inbox-feed`. Installed local adapters become `amq-mcp local channel <harness>` or `amq-mcp local tools codex`; launch helpers become `amq-mcp launch <harness>`. Reload the service and relaunch clients after updating their MCP entries and shell aliases.

The tools, credentials, mailbox format, and terminal feed frames are unchanged. The Codex and Gemini patches do not need rebuilding just to switch the MCP runtime.

## Quick start

```sh
git clone https://github.com/SiavZ/amq-mcp.git
cd amq-mcp
cargo install --path . --locked
export AMQ_MCP_DIR="$PWD"
```

For a new demo queue:

```sh
export AM_ROOT="$AMQ_MCP_DIR/.agent-mail/demo"
amq init --root "$AM_ROOT" --agents claude,codex,gemini,jcode
```

To use an existing fleet, take its queue root from `amq env --json` and use its registered agent handles instead. Do not reinitialize an existing queue.

Create one credential per handle. This example writes an owner-only file, refuses to overwrite it, and does not print the tokens:

```sh
python3 - <<'PY'
import json, os, secrets
from pathlib import Path
directory = Path.home() / '.local/state/amq-mcp'
directory.mkdir(parents=True, exist_ok=True, mode=0o700)
tokens = {name: secrets.token_hex(32) for name in ['claude', 'codex', 'gemini', 'jcode']}
fd = os.open(directory / 'tokens.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'w') as file:
    json.dump(tokens, file)
PY
```

Python is used only to generate this example credential file, not by the server. The file format is a JSON object mapping handles to distinct random tokens of at least 32 characters.

Start the service and keep it running:

```sh
amq-mcp --http 8765 \
  --tokens "$HOME/.local/state/amq-mcp/tokens.json" \
  --root "$AM_ROOT"
```

In each agent terminal, set `AMQ_MCP_DIR` to the absolute checkout path. The examples below use `amq-mcp with --` to load the token matching `AM_ME` and export the subscription feed. It preserves the terminal, working directory, and command arguments.

Optional overrides are `AMQ_MCP_URL` and `AMQ_MCP_TOKENS`. An explicit `AMQ_MCP_TOKEN` takes precedence over the credential file.

## Connect your agents

### Claude Code

Register the native channel adapter once:

```sh
claude mcp add --scope user --transport stdio amq -- amq-channel
```

Then launch with incoming channels enabled:

```sh
AM_ME=claude amq-mcp with -- \
  claude --dangerously-load-development-channels server:amq
```

That flag opts into a local development channel. It does not bypass tool permissions. Organization policies may also govern channel availability. See the [Claude channel contract](https://code.claude.com/docs/en/channels-reference).

### jcode

Merge an entry into the `servers` object in `~/.jcode/mcp.json`. Replace the example paths with absolute paths on your machine:

```json
{
  "servers": {
    "amq": {
      "command": "/absolute/path/to/amq-channel",
      "env": {
        "AM_ME": "jcode",
        "AMQ_MCP_TOKEN": "",
        "AMQ_MCP_TOKENS": "/absolute/path/to/tokens.json"
      },
      "shared": false
    }
  }
}
```

Then start a new jcode session:

```sh
AM_ME=jcode amq-mcp with -- jcode
```

jcode's shared daemon keeps its startup environment. The explicit mailbox and credential file above prevent a channel from borrowing another terminal's token. Give additional agents distinct handles in project-specific entries. Keep `shared: false` so the native channel belongs to its session.

### Codex CLI

Build the [patched native terminal](integrations/README.md), then set `CODEX_AMQ_BIN` to its absolute executable path.

Register the HTTP tools:

```sh
codex mcp add amq --url http://127.0.0.1:8765/mcp \
  --bearer-token-env-var AMQ_MCP_TOKEN
```

Launch the patched terminal:

```sh
AM_ME=codex amq-mcp with -- "$CODEX_AMQ_BIN" \
  -c mcp_servers.amq.enabled=true
```

The configuration override selects Codex's embedded server so it uses this terminal's identity and token. Add `resume --last` to resume a conversation. Keep the matching `codex-code-mode-host` and `codex-exec-server` executables beside the patched binary.

### Gemini CLI

Build the [patched native terminal](integrations/README.md), then set `GEMINI_AMQ_BUNDLE` to the absolute path of its `bundle/gemini.js` entry point.

Merge this into Gemini's settings:

```json
{
  "mcpServers": {
    "amq": {
      "httpUrl": "http://127.0.0.1:8765/mcp",
      "headers": { "Authorization": "Bearer ${AMQ_MCP_TOKEN}" }
    }
  }
}
```

Launch it with a valid Gemini provider credential:

```sh
AM_ME=gemini amq-mcp with -- node "$GEMINI_AMQ_BUNDLE"
```

Add `--resume latest` to resume a conversation. Provider authentication is separate from AMQ authentication. Gemini itself is JavaScript and still requires Node.js; the AMQ server and feed do not.

### Any MCP client over stdio

For tools without a shared HTTP service, register:

```json
{
  "mcpServers": {
    "amq": {
      "command": "/absolute/path/to/amq-mcp",
      "args": [
        "--root", "/absolute/path/to/mailboxes",
        "--me", "agent-a"
      ]
    }
  }
}
```

This supports modern and legacy stdio clients. Automatic wake still requires a host delivery adapter; `wait_for_messages` provides a bounded wait when the host has none.

## Tools

| Tool | Purpose |
| --- | --- |
| `status` | Show the fixed identity, queue, AMQ version, and inbox URI. |
| `list_peers` | Discover sessions, agents, and presence. |
| `send_message` | Send a literal body with recipient, thread, routing, and priority. |
| `reply` | Preserve the original thread and reply routing. |
| `check_messages` | Read unread mail; `peek: true` lists headers without consuming it. |
| `read_message` | Read or reread a message by ID. |
| `wait_for_messages` | Wait up to 50 seconds, then read incoming mail. |
| `get_thread` | Read thread history within the caller's mailbox and outbox. |
| `set_summary` | Update AMQ presence and a short work summary. |

For example, an agent can call `send_message` with:

```json
{
  "to": ["codex"],
  "subject": "Review request",
  "body": "Review the latest changes and reply on this thread."
}
```

Replies use the returned message ID with `reply`. Multiple recipients require an explicit thread.

## Stateless transport and delivery

The HTTP endpoint authenticates a fixed handle on every request. Request handlers hold no protocol sessions, session IDs, or durable delivery cursors. Messages, threads, read state, and receipts belong to AMQ.

Incoming updates use standard `subscriptions/listen` streams for `amq://inbox/<handle>`. Open streams are transient transport state. The Claude/jcode adapter translates those updates into `notifications/claude/channel`; the Codex/Gemini feed sends typed JSON frames over a private pipe to the native terminal queue.

Notifications contain bounded headers and IDs. The agent retrieves the body with `read_message`. Notification delivery never marks a message read. Reading records an AMQ `drained` receipt, which proves ingestion rather than task completion. Messages remain available in `cur`, and agents should deduplicate repeated notices by message ID.

The feed announces an unread ID once per process. It retries an unavailable service and checks unread mail after reconnecting. The channel adapter can repeat unread notices. Requests can use any service instance against the same queue; each delivery client reopens its subscription against its configured endpoint.

Feed deduplication retains message IDs for that process's lifetime. This is transient delivery-client state; the HTTP service holds no delivery cursor.

Protocol references: [MCP base protocol](https://modelcontextprotocol.io/specification/2026-07-28/basic), [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http), and the [official Rust SDK](https://github.com/modelcontextprotocol/rust-sdk/tree/rmcp-v3.11.0).

## Deployment boundaries

The HTTP service binds to `127.0.0.1`, validates Host and Origin, and uses distinct bearer credentials for each mailbox. Another token's inbox and thread history cannot be read by changing tool arguments. Peer discovery exposes handles and presence by design.

Local processes with filesystem access to the queue remain trusted. Remote hosting, TLS termination, and OAuth are outside this version. Never commit credential files or mailbox contents.

When migrating a mailbox, retire its previous terminal-injection waker and inbox-draining hooks before enabling native delivery. Give every concurrent terminal a unique handle. Moving to this server does not automatically stop an existing fleet.

`amq-mcp local` and `amq-mcp launch` are optional native macOS installation helpers for a configured service. They use an owner-only installation manifest and support managed jcode identities. They are not a cross-platform one-command installer; the manual setup above works independently of them. See [local helper configuration](integrations/README.md#optional-macos-launch-helpers).

## Development and verification

```sh
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

The 22 Rust tests use temporary queues and actual AMQ/MCP processes. They cover stateless requests across replicas, caller isolation, literal bodies, receipts, unread subscriptions, reconnects, stdio compatibility, channel delivery, offline feed startup, and local launcher behavior. Unit coverage includes CLI limits, frozen environments, strict validation, and macOS symlinked watcher roots. Independent TypeScript SDK clients also completed modern HTTP and legacy channel notification/read/reply checks against the Rust binaries.

The terminal patches also have targeted native tests: three Codex tests and 20 Gemini tests, including the existing Gemini message-queue checks. See [build instructions](integrations/README.md) and [verification status](integrations/verification.md). Compiled harness binaries, credentials, and machine-specific installation records are not included in this repository.

Upstream patch attributions are in [third-party notices](THIRD_PARTY_NOTICES.md).
