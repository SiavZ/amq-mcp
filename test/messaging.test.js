import assert from "node:assert/strict";
import { test } from "node:test";
import { execFileSync, spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { StdioClientTransport } from "@modelcontextprotocol/client/stdio";
import { startHttp } from "../http.js";
import { createChannel } from "../channel.js";
import { openInbox } from "../inbox-client.js";
import { InMemoryTransport } from "@modelcontextprotocol/client";
import { z } from "zod";

const credentials = {
  alice: "a".repeat(40),
  bob: "b".repeat(40),
  charlie: "c".repeat(40),
};

test("terminal launcher preserves arguments and loads the current handle without printing credentials", () => {
  const output = execFileSync(
    "bin/with-amq",
    [
      process.execPath,
      "-e",
      "process.stdout.write(JSON.stringify({me:process.env.AM_ME,feed:process.env.AMQ_MCP_FEED,token_present:!!process.env.AMQ_MCP_TOKEN,args:process.argv.slice(1)}))",
      "a spaced argument",
      "$(literal)",
    ],
    {
      env: {
        ...process.env,
        AM_ME: "test-terminal",
        AMQ_MCP_TOKEN: "test-credential",
      },
      encoding: "utf8",
    },
  );
  const state = JSON.parse(output);
  assert.equal(state.me, "test-terminal");
  assert.equal(state.token_present, true);
  assert.ok(state.feed.endsWith("/feed.js"));
  assert.deepEqual(state.args, ["a spaced argument", "$(literal)"]);
  assert.ok(!output.includes("test-credential"));
});

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "amq-mcp-test-"));
  const env = {
    ...process.env,
    AM_ROOT: root,
    AM_BASE_ROOT: root,
    AMQ_NO_UPDATE_CHECK: "1",
  };
  for (const key of ["AM_ROOT_ID", "AM_BASE_ROOT_ID", "AM_SESSION", "AM_ME"])
    delete env[key];
  execFileSync(
    "amq",
    ["init", "--root", root, "--agents", "alice,bob,charlie"],
    { env, stdio: "pipe" },
  );
  t.after(() => rm(root, { recursive: true, force: true }));
  return { root, env };
}

async function httpClient(t, url, handle, mode = { pin: "2026-07-28" }) {
  const client = new Client(
    { name: "amq-test", version: "1" },
    { versionNegotiation: { mode } },
  );
  await client.connect(
    new StreamableHTTPClientTransport(new URL(url), {
      requestInit: {
        headers: { Authorization: `Bearer ${credentials[handle]}` },
      },
    }),
  );
  t.after(() => client.close());
  return client;
}

async function call(client, name, args = {}) {
  const result = await client.callTool({ name, arguments: args });
  assert.equal(result.isError, undefined, JSON.stringify(result));
  return JSON.parse(result.content[0].text);
}

test("stateless 2026 HTTP delivers across instances, preserves literal bodies, threads, and receipts", async (t) => {
  const { root, env } = await fixture(t);
  const one = await startHttp({ root, env, credentials, http: "0" });
  const two = await startHttp({ root, env, credentials, http: "0" });
  t.after(async () => {
    await one.close();
    await two.close();
  });
  const alice = await httpClient(t, one.url, "alice");
  const bob = await httpClient(t, two.url, "bob");
  assert.equal(alice.getProtocolEra(), "modern");
  assert.equal((await call(bob, "status")).me, "bob");
  const payload =
    "@a-file-that-must-not-be-read\n$(touch should-not-exist) `echo literal`";
  const sent = await call(alice, "send_message", {
    to: ["bob"],
    body: payload,
    subject: "review",
  });
  const charlie = await httpClient(t, one.url, "charlie");
  assert.deepEqual(await call(charlie, "get_thread", { id: sent.thread }), []);
  await assert.rejects(
    charlie.readResource({ uri: "amq://inbox/bob" }),
    /not found/i,
  );
  const peek = await call(bob, "check_messages", { peek: true });
  assert.equal(peek[0].id, sent.id);
  const history = await call(bob, "get_thread", { id: sent.thread });
  assert.ok(
    JSON.stringify(history).includes("@a-file-that-must-not-be-read"),
    JSON.stringify(history),
  );
  assert.equal((await call(bob, "check_messages", { peek: true })).length, 1);
  const mail = await call(bob, "check_messages");
  assert.equal(mail.count, 1);
  assert.equal(mail.drained[0].body.trimEnd(), payload);
  assert.equal(mail.drained[0].from, "alice");
  const receipts = JSON.parse(
    execFileSync(
      "amq",
      ["receipts", "list", "--me", "bob", "--msg-id", sent.id, "--json"],
      { env, encoding: "utf8" },
    ),
  );
  assert.ok(JSON.stringify(receipts).includes("drained"));
  const reply = await call(bob, "reply", { id: sent.id, body: "Reviewed." });
  assert.equal(reply.thread, sent.thread);
  assert.deepEqual(reply.to, ["alice"]);
  const incoming = await call(alice, "wait_for_messages", {
    timeout_seconds: 1,
  });
  assert.equal(incoming.count, 1);
  assert.equal(incoming.drained[0].body.trim(), "Reviewed.");
  await call(bob, "set_summary", { summary: "Reviewing the parser." });
  assert.ok(
    JSON.stringify(await call(alice, "list_peers")).includes(
      "Reviewing the parser.",
    ),
  );
});

test("subscriptions announce mail without consuming it and survive a client reconnect", async (t) => {
  const { root, env } = await fixture(t);
  const app = await startHttp({ root, env, credentials, http: "0" });
  t.after(() => app.close());
  const alice = await httpClient(t, app.url, "alice");
  const bob = await httpClient(t, app.url, "bob");
  let notify;
  const changed = new Promise((resolve) => {
    notify = resolve;
  });
  bob.setNotificationHandler(
    "notifications/resources/updated",
    (notification) => notify(notification),
  );
  const sub = await bob.listen({ resourceSubscriptions: ["amq://inbox/bob"] });
  t.after(() => sub.close());
  const sent = await call(alice, "send_message", {
    to: ["bob"],
    body: "An unread message.",
  });
  const notification = await Promise.race([
    changed,
    new Promise((_, reject) => {
      const timer = setTimeout(
        () => reject(new Error("No subscription update.")),
        3000,
      );
      timer.unref();
    }),
  ]);
  assert.equal(notification.params.uri, "amq://inbox/bob");
  assert.ok(
    notification.params._meta["io.modelcontextprotocol/subscriptionId"],
  );
  const resource = await bob.readResource({ uri: "amq://inbox/bob" });
  assert.equal(JSON.parse(resource.contents[0].text)[0].id, sent.id);
  await bob.close();
  const reconnected = await httpClient(t, app.url, "bob");
  assert.equal(
    (await call(reconnected, "check_messages", { peek: true }))[0].id,
    sent.id,
  );
  await call(reconnected, "read_message", { id: sent.id });
  await call(reconnected, "read_message", { id: sent.id });
  assert.deepEqual(
    await call(reconnected, "check_messages", { peek: true }),
    [],
  );
});

test("HTTP authenticates each request, rejects browser origins, and never assigns a session", async (t) => {
  const { root, env } = await fixture(t);
  const app = await startHttp({ root, env, credentials, http: "0" });
  t.after(() => app.close());
  const request = {
    jsonrpc: "2.0",
    id: 1,
    method: "tools/list",
    params: {
      _meta: {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
      },
    },
  };
  const headers = {
    Accept: "application/json, text/event-stream",
    "Content-Type": "application/json",
    "MCP-Protocol-Version": "2026-07-28",
    "Mcp-Method": "tools/list",
  };
  assert.equal(
    (
      await fetch(app.url, {
        method: "POST",
        headers,
        body: JSON.stringify(request),
      })
    ).status,
    401,
  );
  const authorized = {
    ...headers,
    Authorization: `Bearer ${credentials.alice}`,
  };
  assert.equal(
    (
      await fetch(app.url, {
        method: "POST",
        headers: { ...authorized, Origin: "https://untrusted.example" },
        body: JSON.stringify(request),
      })
    ).status,
    403,
  );
  const response = await fetch(app.url, {
    method: "POST",
    headers: authorized,
    body: JSON.stringify(request),
  });
  assert.equal(
    response.status,
    200,
    response.status === 200 ? "" : await response.text(),
  );
  assert.equal(response.headers.get("mcp-session-id"), null);
  const result = await response.json();
  assert.equal(result.result.resultType, "complete", JSON.stringify(result));
  assert.ok(result.result.tools.some((tool) => tool.name === "send_message"));
  assert.equal((await fetch(app.url, { headers: authorized })).status, 405);
  const alice = await httpClient(t, app.url, "alice");
  const invalid = await alice.callTool({
    name: "send_message",
    arguments: { to: ["bob"], body: " ", me: "bob" },
  });
  assert.equal(invalid.isError, true);
  assert.equal((await call(alice, "status")).me, "alice");
});

test("stdio supports the new stateless protocol and older harnesses", async (t) => {
  const { root, env } = await fixture(t);
  for (const mode of [{ pin: "2026-07-28" }, "legacy"]) {
    const client = new Client(
      { name: "stdio-test", version: "1" },
      { versionNegotiation: { mode } },
    );
    const transport = new StdioClientTransport({
      command: process.execPath,
      args: ["server.js", "--root", root, "--me", "alice"],
      env,
      cwd: process.cwd(),
      stderr: "pipe",
    });
    await client.connect(transport);
    try {
      assert.equal(
        client.getProtocolEra(),
        mode === "legacy" ? "legacy" : "modern",
      );
      assert.equal((await call(client, "status")).me, "alice");
    } finally {
      await client.close();
    }
  }
});

test("Claude/jcode channel adapter announces unread mail through the stateless service", async (t) => {
  const { root, env } = await fixture(t);
  const app = await startHttp({ root, env, credentials, http: "0" });
  t.after(() => app.close());
  const channel = await createChannel({ url: app.url, token: credentials.bob });
  t.after(() => channel.close());
  const [hostTransport, channelTransport] =
    InMemoryTransport.createLinkedPair();
  const host = new Client({ name: "channel-host-test", version: "1" });
  t.after(() => host.close());
  let delivered;
  const notification = new Promise((resolve) => {
    delivered = resolve;
  });
  host.setNotificationHandler(
    "notifications/claude/channel",
    {
      params: z.object({
        content: z.string(),
        meta: z.record(z.string(), z.string()),
      }),
    },
    (params) => delivered({ params }),
  );
  await channel.server.connect(channelTransport);
  await host.connect(hostTransport);
  assert.ok(host.getServerCapabilities().experimental["claude/channel"]);
  const alice = await httpClient(t, app.url, "alice");
  const sent = await call(alice, "send_message", {
    to: ["bob"],
    body: "Please review.",
  });
  const received = await Promise.race([
    notification,
    new Promise((_, reject) => {
      const timer = setTimeout(
        () => reject(new Error("Channel did not receive mail.")),
        3000,
      );
      timer.unref();
    }),
  ]);
  assert.ok(received.params.content.includes(sent.id));
  assert.equal(
    (await call(host, "check_messages", { peek: true }))[0].id,
    sent.id,
  );
  await call(host, "read_message", { id: sent.id });
  assert.deepEqual(await call(host, "check_messages", { peek: true }), []);
});

test("terminal feed reconnects after an offline start and delivers unread mail over its private pipe", async (t) => {
  const { root, env } = await fixture(t);
  const initial = await startHttp({ root, env, credentials, http: "0" });
  const url = initial.url;
  const port = new URL(url).port;
  await initial.close();
  const child = spawn(process.execPath, ["feed.js"], {
    cwd: process.cwd(),
    env: { ...env, AMQ_MCP_URL: url, AMQ_MCP_TOKEN: credentials.bob },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const frames = createInterface({ input: child.stdout });
  t.after(async () => {
    if (child.exitCode === null) {
      const exited = new Promise((resolve) => child.once("exit", resolve));
      child.kill("SIGTERM");
      await exited;
    }
    frames.close();
  });
  const timeout = (message) =>
    new Promise((_, reject) => {
      const timer = setTimeout(() => reject(new Error(message)), 5000);
      timer.unref();
    });
  await Promise.race([
    new Promise((resolve) => child.stderr.once("data", resolve)),
    timeout("Feed did not report offline service."),
  ]);
  const app = await startHttp({ root, env, credentials, http: port });
  t.after(() => app.close());
  const alice = await httpClient(t, app.url, "alice");
  const received = new Promise((resolve) =>
    frames.once("line", (line) => resolve(JSON.parse(line))),
  );
  const sent = await call(alice, "send_message", {
    to: ["bob"],
    body: "Native terminal notification.",
  });
  const frame = await Promise.race([
    received,
    timeout("Feed did not deliver mail after reconnecting."),
  ]);
  assert.equal(frame.type, "amq_message");
  assert.ok(frame.content.includes(sent.id));
  const bob = await httpClient(t, app.url, "bob");
  assert.equal(
    (await call(bob, "check_messages", { peek: true }))[0].id,
    sent.id,
  );
  await call(bob, "read_message", { id: sent.id });
});

test("notifications arriving during an inbox scan are processed without waiting for the next poll", async (t) => {
  const { root, env } = await fixture(t);
  const app = await startHttp({ root, env, credentials, http: "0" });
  t.after(() => app.close());
  const alice = await httpClient(t, app.url, "alice");
  let entered;
  let release;
  const firstEntered = new Promise((resolve) => {
    entered = resolve;
  });
  const blocked = new Promise((resolve) => {
    release = resolve;
  });
  const delivered = [];
  const inbox = await openInbox({
    url: app.url,
    token: credentials.bob,
    onMessages: async (messages) => {
      delivered.push(...messages.map((message) => message.id));
      if (delivered.length === 1) {
        entered();
        await blocked;
      }
    },
  });
  t.after(() => inbox.close());
  const first = await call(alice, "send_message", {
    to: ["bob"],
    body: "first",
  });
  const scanning = inbox.refresh();
  await firstEntered;
  const second = await call(alice, "send_message", {
    to: ["bob"],
    body: "second",
  });
  const overlapping = inbox.refresh();
  release();
  await Promise.all([scanning, overlapping]);
  assert.ok(delivered.includes(first.id));
  assert.ok(delivered.includes(second.id));
});
