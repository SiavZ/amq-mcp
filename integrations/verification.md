# Verification

Results recorded on 2026-11-24. These describe the tested versions, not a promise about future harness releases.

## Automated checks

`npm test` passes 11 Node tests using temporary AMQ queues and real MCP processes. Coverage includes stateless HTTP requests across service instances, caller isolation, literal bodies, threads, receipts, subscriptions that do not consume mail, reconnects, modern and legacy stdio, native channel adaptation, offline feed startup, a pending-scan notification race, and local launch helpers.

The pinned native terminal patches passed three Codex Rust tests and 20 Gemini tests. Both source builds passed, along with Codex formatting, targeted Gemini lint, and patch applicability checks. Native build checkouts were removed after validation; these source tests are separate from `npm test`.

## Authenticated native terminal tests

Each test used isolated identities and fresh bearer credentials. Agents announced readiness, ended their turn, and remained idle without polling or calling `wait_for_messages`. A controller then sent a message through modern HTTP MCP. A passing round trip required a real `read_message` call, a matching AMQ drained receipt, and an exact nonce reply received on the original thread. Incoming notices were not typed into terminal input.

| Terminal | Result |
| --- | --- |
| Claude Code 2.1.331 | Native channel wake, read, and reply passed. |
| Patched Codex 0.171.0 | Native feed wake, read, and reply passed twice. |
| jcode 1.0.1335-dev | Per-session channel wake, read, and reply passed using its Claude Code provider. |
| Patched Gemini CLI 0.44.1 | Notice entered the native queue and automatically started a model turn. Google rejected the saved provider key with `API_KEY_INVALID`; read and reply remain unverified. |

Codex's second message arrived while an unsent draft remained in its composer. It read and replied without submitting or changing that draft. Gemini's message remained unread when provider authentication failed.

Live testing found a missing Codex tool-host companion in a relocated build. Keeping the matching `codex-code-mode-host` and `codex-exec-server` beside the patched executable fixed startup, and the subsequent round trips passed.

The installed Claude, Codex, and jcode MCP entries were also checked independently. Each connected, exposed nine tools, and reported the intended mailbox identity. jcode's own `mcp test amq` connected and disconnected cleanly.

Temporary test sessions, mailbox roots, and copied authentication material were removed after testing. Credentials and machine-specific reports are excluded from the public repository and package contents.
