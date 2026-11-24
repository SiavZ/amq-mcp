# Verification

Results recorded on 2026-11-24. These describe the tested versions, not a promise about future harness releases.

## Automated checks

The Rust migration passes `cargo test --all-targets`: 14 unit tests and eight integration tests using temporary AMQ queues and real MCP processes. Coverage includes stateless HTTP requests across service instances, caller isolation, literal bodies, threads, receipts, subscriptions that do not consume mail, reconnects, modern and legacy stdio, native channel adaptation, offline feed startup, and local launch helpers. Watcher tests cover macOS symlinked roots, per-mailbox event isolation, startup failures, and an initial invalidation that closes the acknowledgment/replay race.

`cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and optimized builds of all three native binaries pass. No JavaScript runtime, npm package, or Node dependency tree remains in the server checkout.

Independent TypeScript SDK v2 clients completed modern HTTP calls and legacy channel notification/read/reply checks against the Rust SDK v3 service. This check ran before removing the old development dependency tree. It confirms client interoperability beyond tests that use the Rust SDK on both sides.

The pinned native terminal patches previously passed three Codex Rust tests and 20 Gemini tests. Both source builds passed, along with Codex formatting, targeted Gemini lint, and patch applicability checks. The terminal patches are unchanged by the runtime migration. Their build checkouts were removed after validation and were not rebuilt during this port.

## Authenticated native terminal tests

These model-driven tests were run before the Rust migration. Each used isolated identities and fresh bearer credentials. Agents announced readiness, ended their turn, and remained idle without polling or calling `wait_for_messages`. A controller then sent a message through modern HTTP MCP. A passing round trip required a real `read_message` call, a matching AMQ drained receipt, and an exact nonce reply received on the original thread. Incoming notices were not typed into terminal input.

| Terminal | Result |
| --- | --- |
| Claude Code 2.1.331 | Native channel wake, read, and reply passed. |
| Patched Codex 0.171.0 | Native feed wake, read, and reply passed twice. |
| jcode 1.0.1335-dev | Per-session channel wake, read, and reply passed using its Claude Code provider. |
| Patched Gemini CLI 0.44.1 | Notice entered the native queue and automatically started a model turn. Google rejected the saved provider key with `API_KEY_INVALID`; read and reply remain unverified. |

Codex's second message arrived while an unsent draft remained in its composer. It read and replied without submitting or changing that draft. Gemini's message remained unread when provider authentication failed.

Live testing found a missing Codex tool-host companion in a relocated build. Keeping the matching `codex-code-mode-host` and `codex-exec-server` beside the patched executable fixed startup, and the subsequent round trips passed.

After the migration, the installed Claude, Codex, and jcode MCP entries were checked independently against the Rust service. Each connected, exposed nine tools, and reported the intended mailbox identity using AMQ 0.76.0. jcode's own `mcp test amq` connected and disconnected cleanly with server version 0.2.0. Fresh model-driven turns were not rerun during the port.

Temporary test sessions, mailbox roots, and copied authentication material were removed after testing. Credentials and machine-specific reports are excluded from the public repository.
