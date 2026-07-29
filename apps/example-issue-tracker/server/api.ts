// Optional standalone Node HTTP shell for the app authority. `pnpm dev` serves `/api/rindle/*`
// through TanStack Start server routes in the Vite process; this file remains useful for focused
// API/smoke harnesses that want the authority without booting the full web tier.

import { createServer } from "node:http";
import type { IncomingHttpHeaders, IncomingMessage } from "node:http";

import { createIssueApi, httpErrorOf } from "./app-api.ts";
import type { User } from "./app-api.ts";
import type { ApiContext } from "@rindle/api-server";

const DAEMON_URL = process.env.RINDLE_DAEMON_URL ?? "http://127.0.0.1:7600";
const DAEMON_TOKEN = process.env.RINDLE_DAEMON_TOKEN ?? "dev-daemon-token";
// One topology (design 214): the replicator write-master's ingress. Set ⇒ writes + mutation
// sessions route there (SplitDaemonClient); reads keep coming off the follower (DAEMON_URL).
const REPLICATOR_URL = process.env.RINDLE_REPLICATOR_URL;
const REPLICATOR_TOKEN = process.env.RINDLE_REPLICATOR_TOKEN;
// The master's PUBLIC SQL credential — authoritative mutations execute there. Shares the write
// ingress listener with REPLICATOR_URL, so it needs no separate origin, but must be its own secret.
const DATABASE_TOKEN = process.env.RINDLE_DATABASE_TOKEN;
const PORT = Number(process.env.API_PORT ?? 7700);

const api = createIssueApi({
  daemonUrl: DAEMON_URL,
  daemonToken: DAEMON_TOKEN,
  replicatorUrl: REPLICATOR_URL,
  replicatorToken: REPLICATOR_TOKEN,
  databaseToken: DATABASE_TOKEN,
});

function userHeader(headers: IncomingHttpHeaders): User {
  const raw = headers["x-user"];
  return Array.isArray(raw) ? raw[0] : raw;
}

async function readJsonBody(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
  return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
}

const server = createServer((req, res) => {
  void (async () => {
    try {
      const body = await readJsonBody(req);
      // Demo auth: the user rides a header. A real deployment verifies a session/JWT here.
      const context: ApiContext<User> = { user: userHeader(req.headers), request: req };
      let out: unknown;
      if (req.url === api.routes.query) out = await api.handleQueryJson(body, context);
      else if (req.url === api.routes.read) out = await api.handleReadJson(body, context);
      else if (req.url === api.routes.mutate) out = await api.handleMutateJson(body, context);
      else throw Object.assign(new Error("not found"), { status: 404 });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(out));
    } catch (err) {
      const { status, message } = httpErrorOf(err);
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: message }));
    }
  })();
});

server.listen(PORT, () => {
  const address = server.address();
  const port = typeof address === "object" && address !== null ? address.port : PORT;
  console.log(`[api] listening on http://127.0.0.1:${port} (daemon: ${DAEMON_URL})`);
});
