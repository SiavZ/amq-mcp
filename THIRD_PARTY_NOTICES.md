# Third-party notices

AMQ MCP invokes the [AMQ CLI](https://github.com/avivsinai/agent-message-queue), which is installed separately.

The terminal integration patches modify these upstream projects:

- [OpenAI Codex](https://github.com/openai/codex), tag `rust-v0.171.0`, commit `ccf9be8cc458bd6b8336fe84d68d8893f2691a5e`. Its [Apache License 2.0](https://github.com/openai/codex/blob/ccf9be8cc458bd6b8336fe84d68d8893f2691a5e/LICENSE) and any relevant upstream notices continue to apply to the patched code.
- [Google Gemini CLI](https://github.com/google-gemini/gemini-cli), tag `v0.44.1`, commit `b9b7416f861c1b10efc39c477dbe66ed8f2155a5`. Its [Apache License 2.0](https://github.com/google-gemini/gemini-cli/blob/b9b7416f861c1b10efc39c477dbe66ed8f2155a5/LICENSE) and copyright notices continue to apply to the patched code.

The patch files identify the changed files and added behavior. A copy of the upstream Apache 2.0 license is in [integrations/licenses/Apache-2.0.txt](integrations/licenses/Apache-2.0.txt). Any accompanying upstream NOTICE files are kept in that directory as well. Compiled upstream harness binaries are not included.

Runtime dependency names, versions, integrity values, and license metadata are recorded in `package-lock.json`. Their licenses remain with their respective packages.
