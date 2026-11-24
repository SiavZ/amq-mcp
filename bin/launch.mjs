#!/usr/bin/env node
import { readFile, mkdir } from "node:fs/promises";
import { spawn, execFileSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { loadInstall, validHandle, claudeChannels } from "../local.js";

const project = dirname(dirname(fileURLToPath(import.meta.url)));
const [harness, ...args] = process.argv.slice(2);
const config = await loadInstall();
if (!config.binaries[harness]) throw new Error("Choose claude, codex, or jcode.");
const plainClaudeArgs = harness === "claude" ? claudeChannels(args, false) : args;
const commandOnly = args.some((arg) => ["--version", "--help", "-V", "-h"].includes(arg)) ||
  (harness === "claude" && args.some((arg) => ["-p", "--print", "--bare"].includes(arg))) ||
  (harness === "claude" && ["auth", "mcp", "plugin", "install", "update", "doctor", "agents",
    "attach", "logs", "stop", "rm", "config", "setup-token", "remote-control", "daemon"].includes(plainClaudeArgs[0])) ||
  (harness !== "claude" && args[0] && !args[0].startsWith("-") &&
    !["resume", "fork"].includes(args[0]));
let launchArgs = commandOnly ? plainClaudeArgs : args;
const env = { ...process.env };
if (!commandOnly) {
  const me = validHandle(env.AM_ME || harness);
  if (env.AM_ROOT && env.AM_ROOT !== config.root)
    throw new Error(`This AMQ MCP installation serves ${config.root}; AM_ROOT names another queue.`);
  execFileSync(config.flock, ["-x", `${config.tokens}.lock`, process.execPath,
    join(project, "local.js"), "provision", me]);
  const tokens = JSON.parse(await readFile(config.tokens, "utf8"));
  Object.assign(env, { AM_ME: me, AM_ROOT: config.root, AMQ_BIN: config.amq,
    AMQ_MCP_TOKEN: tokens[me], AMQ_MCP_TOKENS: config.tokens,
    AMQ_MCP_URL: config.url, AMQ_MCP_FEED: join(project, "feed.js"), AMQ_MCP_NATIVE: "1" });
  const deadline = Date.now() + 15_000;
  for (;;) {
    try {
      const response = await fetch(config.url, { method: "POST",
        headers: { Authorization: `Bearer ${tokens[me]}`, "Content-Type": "application/json",
          Accept: "application/json, text/event-stream", "MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "tools/list" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: { _meta: {
          "io.modelcontextprotocol/protocolVersion": "2026-07-28",
          "io.modelcontextprotocol/clientCapabilities": {},
        } } }),
        signal: AbortSignal.timeout(1000) });
      await response.body?.cancel();
      if (response.ok) break;
      throw new Error(`HTTP ${response.status}`);
    } catch (error) {
      if (Date.now() >= deadline) throw new Error(`AMQ MCP service is unavailable: ${error.message}`);
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  }
  if (harness === "claude") {
    launchArgs = claudeChannels(args);
    if (!env.CLAUDE_CODE_PROCESS_WRAPPER && config.claudeRestart) {
      env.CLAUDE_CODE_PROCESS_WRAPPER = config.claudeRestart;
      try { env.YARAN_CLAUDE_PEERS_TTY = execFileSync("tty", { stdio: [0, "pipe", "pipe"], encoding: "utf8" }).trim(); } catch {}
    }
  } else if (harness === "codex") {
    // Per-terminal identities require the embedded server rather than an older daemon's environment.
    launchArgs = ["-c", "mcp_servers.amq.enabled=true", ...args];
  } else if (!env.YARAN_SESSION_ID) {
    // shortcut: standalone jcode uses a daemon per handle until the shared daemon forwards AM_ME.
    env.AMQ_MCP_JCODE_HANDLE = me;
    if (!args.some((arg) => arg === "--socket" || arg.startsWith("--socket="))) {
      const directory = join(dirname(config.tokens), "jcode");
      await mkdir(directory, { recursive: true, mode: 0o700 });
      const id = createHash("sha256").update(`${config.root}\0${me}`).digest("hex").slice(0, 16);
      launchArgs = ["--socket", join(directory, `${id}.sock`), ...args];
    }
  }
}
const child = spawn(config.binaries[harness], launchArgs, { env, stdio: "inherit" });
process.once("SIGTERM", () => child.kill("SIGTERM"));
child.once("error", (error) => { console.error(error.message); process.exitCode = 1; });
child.once("exit", (code, signal) => { process.exitCode = code ?? (signal ? 1 : 0); });
