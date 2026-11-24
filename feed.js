#!/usr/bin/env node
import { openInbox, notice } from "./inbox-client.js";
import { setTimeout as delay } from "node:timers/promises";

// Native terminal clients read these frames over a private pipe. No terminal input
// is typed, and mail stays unread until the model calls an AMQ read tool.
let inbox;
let stopping = false;
const controller = new AbortController();
let stopped;
const untilStopped = new Promise((resolve) => {
  stopped = resolve;
});
async function close() {
  stopping = true;
  controller.abort();
  stopped();
  await inbox?.close();
}
process.stdout.on("error", () => {
  void close();
});
for (const signal of ["SIGTERM", "SIGINT"])
  process.once(signal, () => {
    void close();
  });
let retryMs = 1000;
while (!stopping) {
  try {
    inbox = await openInbox({
      repeatMs: Infinity,
      onMessages: async (messages) => {
        for (let offset = 0; offset < messages.length; offset += 20) {
          await new Promise((resolve, reject) => {
            process.stdout.write(
              `${JSON.stringify({ type: "amq_message", content: notice(messages.slice(offset, offset + 20)) })}\n`,
              (error) => (error ? reject(error) : resolve()),
            );
          });
        }
      },
    });
    if (stopping) await inbox.close();
    else {
      await inbox.refresh();
      await untilStopped;
    }
  } catch (error) {
    console.error(`amq-feed: ${error.message}`);
    await delay(retryMs, undefined, { signal: controller.signal }).catch(
      () => {},
    );
    retryMs = Math.min(retryMs * 2, 30_000);
  }
}
