import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { timingSafeEqual } from "node:crypto";
import { createMcpHandler } from "@modelcontextprotocol/server";
import {
  toNodeHandler,
  localhostHostValidation,
  localhostOriginValidation,
} from "@modelcontextprotocol/node";
import { connectAmq } from "./amq.js";
import { buildServer, inboxUri, watchMail } from "./server.js";

export async function startHttp({
  http = "8765",
  tokens,
  credentials,
  root,
  env = process.env,
} = {}) {
  const port = Number(http);
  if (!Number.isInteger(port) || port < 0 || port > 65535)
    throw new Error("--http must be a valid port.");
  if (!credentials && !tokens)
    throw new Error(
      'HTTP requires --tokens FILE with {"handle":"random-token",...}.',
    );
  const configured = credentials ?? JSON.parse(await readFile(tokens, "utf8"));
  if (
    !configured ||
    typeof configured !== "object" ||
    Array.isArray(configured)
  )
    throw new Error("Tokens must be a JSON object of handle to token.");
  const identities = Object.entries(configured);
  if (
    !identities.length ||
    identities.some(
      ([me, token]) =>
        !/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/.test(me) ||
        typeof token !== "string" ||
        token.length < 32,
    )
  ) {
    throw new Error(
      "Each token entry needs a valid AMQ handle and a token of at least 32 characters.",
    );
  }
  if (new Set(identities.map(([, token]) => token)).size !== identities.length)
    throw new Error("Each identity must have a distinct token.");

  const contexts = await Promise.all(
    identities.map(([me]) => connectAmq({ root, me, env })),
  );
  // Static configured identities get separate event buses. A token cannot subscribe
  // to another mailbox's activity by guessing its resource URI.
  const handlers = new Map(
    contexts.map((amq) => [
      amq.identity.me,
      createMcpHandler(() => buildServer(amq)),
    ]),
  );
  const nodeHandlers = new Map(
    [...handlers].map(([me, handler]) => [me, toNodeHandler(handler)]),
  );
  const validateHost = localhostHostValidation();
  const validateOrigin = localhostOriginValidation();
  const listener = createServer((req, res) => {
    if (!validateHost(req, res) || !validateOrigin(req, res)) return;
    if (req.url !== "/mcp") {
      res.writeHead(404).end();
      return;
    }
    const supplied = Buffer.from(
      (req.headers.authorization ?? "").replace(/^Bearer /, ""),
    );
    const caller = identities.find(([, token]) => {
      const expected = Buffer.from(token);
      return (
        supplied.length === expected.length &&
        timingSafeEqual(supplied, expected)
      );
    });
    if (!req.headers.authorization?.startsWith("Bearer ") || !caller) {
      res.writeHead(401, { "WWW-Authenticate": "Bearer" }).end();
      return;
    }
    req.auth = { token: supplied.toString(), clientId: caller[0], scopes: [] };
    void nodeHandlers.get(caller[0])(req, res);
  });
  const watcher = watchMail(contexts[0].identity.root, (me) =>
    handlers.get(me)?.notify.resourceUpdated(inboxUri(me)),
  );
  try {
    await new Promise((resolve, reject) => {
      listener.once("error", reject);
      listener.listen(port, "127.0.0.1", resolve);
    });
  } catch (error) {
    watcher.close();
    await Promise.all([...handlers.values()].map((handler) => handler.close()));
    throw error;
  }
  const url = `http://127.0.0.1:${listener.address().port}/mcp`;
  console.error(`amq-mcp: ${url}`);
  return {
    url,
    async close() {
      watcher.close();
      await Promise.all(
        [...handlers.values()].map((handler) => handler.close()),
      );
      await new Promise((resolve) => listener.close(resolve));
    },
  };
}
