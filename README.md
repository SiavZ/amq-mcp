# AMQ MCP

Give coding agents a shared mailbox through MCP, with automatic incoming delivery in their native terminal interfaces.

AMQ MCP wraps the [AMQ CLI](https://github.com/avivsinai/agent-message-queue). Agents can discover peers, send messages, reply on existing threads, and read their inboxes through tools. Messages and receipts stay in AMQ's filesystem queue. Incoming notifications can wake an idle agent without typing into its terminal.

The HTTP service implements **MCP 2026-07-28** with the TypeScript v2 SDK. It creates a fresh server for each request, authenticates the caller on every request, and assigns no MCP session ID. Older MCP clients can use the compatibility transports.

## Supported terminals

| Client | MCP tools | Automatic incoming delivery |
| --- | --- | --- |
| Claude Code | Yes | Native Claude channel through `channel.js`. |
| jcode | Yes | Its existing per-session channel support through the same adapter. |
| Codex CLI | Yes | Included patch for the native 0.171.0 terminal, using `feed.js`. |
| Gemini CLI | Yes | Included patch for the native 0.44.1 terminal, using `feed.js`. |
| Other MCP clients | Yes | The host must connect subscription notifications to its input queue. |

Stock Codex and Gemini terminals need the supplied patches for automatic wake. Tools work independently of those patches. Native approvals remain under the terminal's control, and an unsent human draft is preserved. After installing a patched terminal, restart it and resume the saved conversation if needed.

Authenticated receive, read, and reply tests passed in Claude, Codex, and jcode. Gemini received its notification and automatically started a turn, but its saved provider key was rejected, so its model-driven read and reply remain unverified. See [verification](integrations/verification.md).

## Requirements

- Node.js 22 or newer.
- The `amq` CLI on PATH. Tested with AMQ 0.76.0; select another executable with `AMQ_BIN`.
- A mailbox handle for each concurrent agent.

The optional macOS launch helpers also require `flock` and a configured local service. The core server does not require them.

## Quick start

```sh
git clone https://github.com/SiavZ/amq-mcp.git
cd amq-mcp
npm ci
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
node --input-type=module <<'JS'
import { randomBytes } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
const directory = join(homedir(), '.local/state/amq-mcp');
mkdirSync(directory, { recursive: true, mode: 0o700 });
const handles = ['claude', 'codex', 'gemini', 'jcode'];
const tokens = Object.fromEntries(handles.map(handle => [handle, randomBytes(32).toString('hex')]));
writeFileSync(join(directory, 'tokens.json'), JSON.stringify(tokens, null, 2), {
  mode: 0o600,
  flag: 'wx',
});
JS
```

Start the service and keep it running:

```sh
node server.js --http 8765 \
  --tokens "$HOME/.local/state/amq-mcp/tokens.json" \
  --root "$AM_ROOT"
```

In each agent terminal, set `AMQ_MCP_DIR` to the absolute checkout path. The examples below use `bin/with-amq` to load the token matching `AM_ME` and export the subscription feed. It preserves the terminal, working directory, and command arguments.

Optional overrides are `AMQ_MCP_URL` and `AMQ_MCP_TOKENS`. An explicit `AMQ_MCP_TOKEN` takes precedence over the credential file.

## Connect your agents

### Claude Code

Register the native channel adapter once:

```sh
claude mcp add --scope user --transport stdio amq -- \
  node "$AMQ_MCP_DIR/channel.js"
```

Then launch with incoming channels enabled:

```sh
AM_ME=claude "$AMQ_MCP_DIR/bin/with-amq" \
  claude --dangerously-load-development-channels server:amq
```

That flag opts into a local development channel. It does not bypass tool permissions. Organization policies may also govern channel availability. See the [Claude channel contract](https://code.claude.com/docs/en/channels-reference).

### jcode

Merge an entry into the `servers` object in `~/.jcode/mcp.json`. Replace both example paths with absolute paths on your machine:

```json
{
  "servers": {
    "amq": {
      "command": "node",
      "args": ["/absolute/path/to/amq-mcp/channel.js"],
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
AM_ME=jcode "$AMQ_MCP_DIR/bin/with-amq" jcode
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
AM_ME=codex "$AMQ_MCP_DIR/bin/with-amq" "$CODEX_AMQ_BIN" \
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
AM_ME=gemini "$AMQ_MCP_DIR/bin/with-amq" node "$GEMINI_AMQ_BUNDLE"
```

Add `--resume latest` to resume a conversation. Provider authentication is separate from AMQ authentication.

### Any MCP client over stdio

For tools without a shared HTTP service, register:

```json
{
  "mcpServers": {
    "amq": {
      "command": "node",
      "args": [
        "/absolute/path/to/amq-mcp/server.js",
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

Protocol references: [MCP base protocol](https://modelcontextprotocol.io/specification/2026-07-28/basic), [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http), and [TypeScript SDK migration](https://ts.sdk.modelcontextprotocol.io/v2/migration/support-2026-07-28).

## Deployment boundaries

The HTTP service binds to `127.0.0.1`, validates Host and Origin, and uses distinct bearer credentials for each mailbox. Another token's inbox and thread history cannot be read by changing tool arguments. Peer discovery exposes handles and presence by design.

Local processes with filesystem access to the queue remain trusted. Remote hosting, TLS termination, and OAuth are outside this version. Never commit credential files or mailbox contents.

When migrating a mailbox, retire its previous terminal-injection waker and inbox-draining hooks before enabling native delivery. Give every concurrent terminal a unique handle. Moving to this server does not automatically stop an existing fleet.

`local.js` and `bin/launch.mjs` are optional macOS installation helpers for a configured service. They use an owner-only installation manifest and support managed jcode identities. They are not a cross-platform one-command installer; the manual setup above works independently of them. See [local helper configuration](integrations/README.md#optional-macos-launch-helpers).

## Development and verification

```sh
npm ci
npm test
```

The 11 Node tests use temporary queues and actual AMQ/MCP processes. They cover stateless requests across replicas, caller isolation, literal bodies, receipts, unread subscriptions, reconnects, stdio compatibility, channel delivery, offline feed startup, and local launcher behavior. The launcher tests require `flock` on PATH.

The terminal patches also have targeted native tests: three Codex tests and 20 Gemini tests, including the existing Gemini message-queue checks. See [build instructions](integrations/README.md) and [verification status](integrations/verification.md). Compiled harness binaries, credentials, and machine-specific installation records are not included in this repository.

Upstream patch attributions are in [third-party notices](THIRD_PARTY_NOTICES.md).
