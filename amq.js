import { execFile } from "node:child_process";

export class AmqError extends Error {
  constructor(message, exitCode) {
    super(message);
    this.exitCode = exitCode;
  }
}

export async function connectAmq({
  root,
  me,
  cwd = process.cwd(),
  env = process.env,
} = {}) {
  const commandEnv = { ...env, AMQ_NO_UPDATE_CHECK: "1" };

  const run = (args, { body, signal, timeout = 15_000 } = {}) =>
    new Promise((resolve, reject) => {
      // Bodies go over stdin. AMQ treats @path as a file reference in --body values.
      const child = execFile(
        env.AMQ_BIN || "amq",
        args,
        {
          cwd,
          env: commandEnv,
          signal,
          timeout,
          maxBuffer: 8 * 1024 * 1024,
          encoding: "utf8",
        },
        (error, stdout, stderr) => {
          if (error) {
            reject(new AmqError(stderr.trim() || error.message, error.code));
            return;
          }
          try {
            resolve(stdout.trim() ? JSON.parse(stdout) : null);
          } catch {
            reject(
              new AmqError(
                "AMQ returned invalid JSON. Check your AMQ version.",
                1,
              ),
            );
          }
        },
      );
      child.stdin.on("error", () => {}); // Early CLI errors can close stdin before the body is written.
      child.stdin.end(body ?? "");
    });

  const identity = await run([
    "env",
    "--json",
    ...(root ? ["--root", root] : []),
    ...(me ? ["--me", me] : []),
  ]);
  if (!identity.me)
    throw new Error("Set AM_ME or pass --me with a unique AMQ handle.");
  // Freeze the CLI-resolved context for the lifetime of this MCP connection.
  for (const [name, key] of Object.entries({
    AM_ROOT: "root",
    AM_ROOT_ID: "root_id",
    AM_BASE_ROOT: "base_root",
    AM_BASE_ROOT_ID: "base_root_id",
    AM_SESSION: "session_name",
    AM_ME: "me",
  })) {
    if (identity[key] !== undefined) commandEnv[name] = identity[key];
    else delete commandEnv[name];
  }

  return { run, identity };
}
