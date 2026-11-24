import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, mkdir, writeFile, readFile, symlink, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile, execFileSync } from "node:child_process";
import { promisify } from "node:util";
import { localHandle, claudeChannels, validHandle } from "../local.js";
import { startHttp } from "../http.js";

test("local adapters bind shared jcode sessions to their registered owner", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "amq-local-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await mkdir(join(root, "agents", "current-agent"), { recursive: true });
  await writeFile(join(root, "agents", "current-agent", ".dux-amq-source"), JSON.stringify({ session_id: "current-session" }));
  await mkdir(join(root, "agents", "linked-agent"), { recursive: true });
  await symlink(join(root, "agents", "current-agent", ".dux-amq-source"), join(root, "agents", "linked-agent", ".dux-amq-source"));
  const env = { JCODE_MCP_CHANNEL_SESSION: "native-session", YARAN_SESSION_ID: "current-session", AM_ME: "stale-daemon-agent", AMQ_MCP_JCODE_HANDLE: "stale-private-agent" };
  assert.equal(await localHandle("jcode", env, root), "current-agent");
  await assert.rejects(localHandle("jcode", { ...env, YARAN_SESSION_ID: "missing" }, root), /unique AMQ owner/);
  assert.equal(await localHandle("jcode", { ...env, YARAN_SESSION_ID: "", AMQ_MCP_JCODE_HANDLE: "standalone" }, root), "standalone");
  assert.equal(await localHandle("jcode", { JCODE_MCP_CHANNEL_SESSION: "native" }, root), "jcode");
  assert.equal(await localHandle("claude", { AM_ME: "current-agent" }, root), "current-agent");
  assert.equal(await localHandle("claude", env, root), "current-agent");
  assert.equal(await localHandle("codex", {}, root), "codex");
  await assert.rejects(localHandle("claude", { JCODE_RUNTIME_PROVIDER: "claude" }, root), /jcode's provider/);
  for (const handle of [undefined, "", "../other", "UPPER", "x".repeat(65)]) assert.throws(() => validHandle(handle));
});

test("Claude launch preserves prompts, existing channels, and option terminators", () => {
  assert.deepEqual(claudeChannels(["resume", "--dangerously-load-development-channels", "server:claude-peers", "--model", "fable"]),
    ["resume", "--model", "fable", "--dangerously-load-development-channels", "server:amq", "server:claude-peers"]);
  assert.deepEqual(claudeChannels(["--model", "fable", "--", "a prompt"]),
    ["--model", "fable", "--dangerously-load-development-channels", "server:amq", "--", "a prompt"]);
  assert.deepEqual(claudeChannels(["a prompt", "--dangerously-load-development-channels=server:amq"]),
    ["a prompt", "--dangerously-load-development-channels", "server:amq"]);
  assert.deepEqual(claudeChannels(["--dangerously-load-development-channels", "server:claude-peers", "mcp", "list"], false), ["mcp", "list"]);
});

test("installed launchers attach native delivery, preserve CLI arguments, and initialize only their mailbox", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "amq-installed-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const root = join(directory, "queue");
  const env = { ...process.env, AM_ROOT: root, AM_BASE_ROOT: root, AMQ_NO_UPDATE_CHECK: "1" };
  for (const key of ["AM_ME", "AM_ROOT_ID", "AM_BASE_ROOT_ID", "AM_SESSION", "YARAN_SESSION_ID", "AMQ_MCP_TOKEN", "AMQ_MCP_JCODE_HANDLE", "CLAUDE_CODE_PROCESS_WRAPPER", "AMQ_MCP_NATIVE", "AMQ_MCP_FEED"])
    delete env[key];
  execFileSync("amq", ["init", "--root", root, "--agents", "existing"], { env, stdio: "pipe" });
  const metadata = join(root, "meta/config.json");
  const original = JSON.parse(await readFile(metadata, "utf8"));
  const credentials = { claude: "a".repeat(40), codex: "b".repeat(40), jcode: "c".repeat(40) };
  const tokens = join(directory, "tokens.json"), settings = join(directory, "install.json"), fake = join(directory, "fake-harness");
  await writeFile(tokens, JSON.stringify(credentials), { mode: 0o600 });
  await writeFile(fake, '#!/usr/bin/env node\nconsole.log(JSON.stringify({args:process.argv.slice(2),me:process.env.AM_ME,native:process.env.AMQ_MCP_NATIVE,token:!!process.env.AMQ_MCP_TOKEN,feed:process.env.AMQ_MCP_FEED,cwd:process.cwd()}));\n', { mode: 0o700 });
  const app = await startHttp({ http: "0", root, env: { ...env, AMQ_BIN: "amq" }, credentials });
  t.after(() => app.close());
  await writeFile(settings, JSON.stringify({ root, tokens, url: app.url, label: "unused-test-label", amq: "amq", flock: "flock", binaries: { claude: fake, codex: fake, jcode: fake } }));
  env.AMQ_MCP_INSTALL = settings;
  const run = async (harness, args) => JSON.parse((await promisify(execFile)(process.execPath,
    ["bin/launch.mjs", harness, ...args], { env, timeout: 10_000 })).stdout);
  const claude = await run("claude", ["--dangerously-load-development-channels", "server:claude-peers", "--", "literal $(text)"]);
  assert.equal(claude.me, "claude");
  assert.equal(claude.native, "1");
  assert.equal(claude.token, true);
  assert.ok(claude.feed.endsWith("/feed.js"));
  assert.deepEqual(claude.args, ["--dangerously-load-development-channels", "server:amq", "server:claude-peers", "--", "literal $(text)"]);
  const codex = await run("codex", ["resume", "a-session"]);
  assert.deepEqual(codex.args, ["-c", "mcp_servers.amq.enabled=true", "resume", "a-session"]);
  const jcode = await run("jcode", ["--resume", "a-session"]);
  assert.equal(jcode.args[0], "--socket");
  assert.ok(jcode.args[1].endsWith(".sock"));
  assert.deepEqual(jcode.args.slice(2), ["--resume", "a-session"]);
  assert.equal(codex.cwd, process.cwd());
  const updated = JSON.parse(await readFile(metadata, "utf8"));
  assert.deepEqual(updated.agents, [...original.agents, "claude", "codex", "jcode"]);
  assert.equal(updated.created_utc, original.created_utc);
  assert.equal((await stat(join(root, "agents/jcode/inbox/new"))).isDirectory(), true);
  const oneshot = await run("codex", ["exec", "a prompt"]);
  assert.deepEqual(oneshot.args, ["exec", "a prompt"]);
  assert.equal(oneshot.token, false);
  assert.equal(oneshot.native, undefined);
  const command = await run("claude", ["--dangerously-load-development-channels", "server:claude-peers", "mcp", "list"]);
  assert.deepEqual(command.args, ["mcp", "list"]);
  assert.equal(command.token, false);
});
