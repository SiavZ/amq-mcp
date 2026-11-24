#!/usr/bin/env node
import { readFile, readdir, lstat, writeFile, rename, mkdir } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { execFileSync } from "node:child_process";
import { pathToFileURL, fileURLToPath } from "node:url";
import { serveStdio } from "@modelcontextprotocol/server/stdio";
import { StdioServerTransport } from "@modelcontextprotocol/server/stdio";
import { connectAmq } from "./amq.js";
import { buildServer } from "./server.js";
import { createChannel } from "./channel.js";

export const installPath = () =>
  process.env.AMQ_MCP_INSTALL || join(homedir(), ".local/state/amq-mcp/install.json");
export const loadInstall = async () => JSON.parse(await readFile(installPath(), "utf8"));

export function validHandle(handle) {
  if (typeof handle !== "string" || !/^[a-z0-9][a-z0-9_-]{0,63}$/.test(handle))
    throw new Error("AM_ME must be a lowercase AMQ handle of at most 64 characters.");
  return handle;
}

// shortcut: a reconnect made from jcode's debug socket carries no Yaran session, so it is read from the
// lane's jcode client process; drop this when jcode forwards YARAN_SESSION_ID on every adapter start.
function clientYaranSession(session) {
  try {
    const processes = execFileSync("/bin/ps", ["eww", "-axo", "command="], { encoding: "utf8", maxBuffer: 1 << 26 });
    const client = processes.split("\n").find((line) => line.includes(`jcode --resume ${session} `));
    return client?.match(/ YARAN_SESSION_ID=(\S+)/)?.[1] || "";
  } catch { return ""; }
}

export async function localHandle(harness, env, root) {
  // jcode also starts the adapter registered for claude, so the session decides, not the argument.
  if (env.JCODE_MCP_CHANNEL_SESSION) {
    // The shared jcode daemon forwards this client's Yaran session, not AM_ME.
    const yaranSession = env.YARAN_SESSION_ID || clientYaranSession(env.JCODE_MCP_CHANNEL_SESSION);
    if (yaranSession) {
      const matches = [];
      for (const entry of await readdir(join(root, "agents"), { withFileTypes: true })) {
        if (!entry.isDirectory()) continue;
        for (const marker of [".dux-amq-source", ".yaran-amq-source"]) {
          const file = join(root, "agents", entry.name, marker);
          let owner;
          try {
            if (!(await lstat(file)).isFile()) continue;
            owner = JSON.parse(await readFile(file, "utf8"));
          } catch (error) {
            if (error.code === "ENOENT" || error instanceof SyntaxError) continue;
            throw error;
          }
          if (owner.session_id === yaranSession) matches.push(entry.name);
        }
      }
      const handles = [...new Set(matches)];
      if (handles.length !== 1) throw new Error("Cannot resolve a unique AMQ owner for this jcode session.");
      return validHandle(handles[0]);
    }
    return validHandle(env.AMQ_MCP_JCODE_HANDLE || "jcode");
  }
  // The Claude process jcode runs as its provider carries no lane identity: jcode's own adapter serves the lane.
  if (harness === "claude" && env.JCODE_RUNTIME_PROVIDER && !env.AM_ME)
    throw new Error("This Claude process is jcode's provider; use the lane's own AMQ adapter.");
  return validHandle(env.AM_ME || harness);
}

export function claudeChannels(args, enabled = true) {
  const remaining = [], channels = new Set(["server:amq"]);
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--") { remaining.push(...args.slice(i)); break; }
    if (args[i] === "--dangerously-load-development-channels") {
      while (/^(server:|plugin:)/.test(args[i + 1] || "")) channels.add(args[++i]);
    } else if (args[i].startsWith("--dangerously-load-development-channels=")) {
      channels.add(args[i].split("=", 2)[1]);
    } else remaining.push(args[i]);
  }
  if (!enabled) return remaining;
  const split = remaining.indexOf("--");
  remaining.splice(split < 0 ? remaining.length : split, 0,
    "--dangerously-load-development-channels", ...channels);
  return remaining;
}

async function provision(config, handle) {
  execFileSync(config.flock, ["-x", join(config.root, "meta/config.lock"), process.execPath,
    fileURLToPath(import.meta.url), "mailbox", handle]);
  const tokens = JSON.parse(await readFile(config.tokens, "utf8"));
  if (!tokens[handle]) {
    tokens[handle] = randomBytes(32).toString("hex");
    const temp = `${config.tokens}.${process.pid}.tmp`;
    await writeFile(temp, JSON.stringify(tokens, null, 2), { mode: 0o600, flag: "wx" });
    await rename(temp, config.tokens);
    // ponytail: new handles reload the service; add credential hot reload if frequent reconnects become a problem.
    execFileSync("/bin/launchctl", ["kickstart", "-k", `gui/${process.getuid()}/${config.label}`]);
  }
}

async function main() {
  const [mode, harness] = process.argv.slice(2), config = await loadInstall();
  if (mode === "provision") return provision(config, validHandle(harness));
  if (mode === "mailbox") {
    const handle = validHandle(harness), directory = join(config.root, "agents", handle);
    for (const suffix of ["inbox/new", "inbox/cur", "inbox/tmp", "outbox/sent", "receipts", "dlq/new", "dlq/cur", "dlq/tmp"])
      await mkdir(join(directory, suffix), { recursive: true, mode: 0o700 });
    const file = join(config.root, "meta/config.json");
    const metadata = JSON.parse(await readFile(file, "utf8"));
    if (!metadata.agents.includes(handle)) {
      metadata.agents.push(handle);
      const temp = `${file}.${process.pid}.tmp`;
      await writeFile(temp, JSON.stringify(metadata, null, 2), { mode: 0o600, flag: "wx" });
      await rename(temp, file);
    }
    return;
  }
  if (!["claude", "codex", "jcode"].includes(harness)) throw new Error("Unknown harness.");
  const me = await localHandle(harness, process.env, config.root);
  const env = { ...process.env, AMQ_BIN: config.amq, AM_ROOT: config.root, AM_ME: me };
  let app;
  if (mode === "channel") {
    const tokens = JSON.parse(await readFile(config.tokens, "utf8"));
    if (!tokens[me]) throw new Error(`No credential for ${me}; launch with ${harness}-amq first.`);
    app = await createChannel({ url: config.url, token: tokens[me] });
    await app.server.connect(new StdioServerTransport());
  } else if (mode === "tools") {
    const amq = await connectAmq({ root: config.root, me, env });
    app = serveStdio(() => buildServer(amq));
  } else throw new Error("Unknown local adapter mode.");
  for (const signal of ["SIGINT", "SIGTERM"])
    process.once(signal, () => app.close().catch(console.error));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
  main().catch((error) => { console.error(`amq-mcp: ${error.message}`); process.exitCode = 1; });
