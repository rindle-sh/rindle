// The app's API server — the SERVERLESS-shaped tier — as a local Node HTTP shell. The actual authority
// (named-query resolution, authoritative mutators, policy) lives in `app-api.ts`, which `worker.ts`
// reuses verbatim behind a Cloudflare `fetch` handler; this file just wires that factory to `node:http`
// for `pnpm dev`. Run: node server/api.ts (or via server/dev.ts).

import { createServer } from "node:http";
import type { IncomingMessage } from "node:http";

import type { ApiContext } from "@rindle/api-server";

import { createForumApi, httpErrorOf, resolveForumDaemon } from "./app-api.ts";
import type { User } from "./app-api.ts";
import { selectAuth } from "./select-auth.ts";

const PORT = Number(process.env.API_PORT ?? 7700);

// One topology (design 214): reads come off the follower (RINDLE_FOLLOWER_URL / RINDLE_DAEMON_URL) and
// writes go to the replicator write-master (RINDLE_REPLICATOR_URL) — config, not code.
const daemon = resolveForumDaemon(process.env, {
  daemonUrl: "http://127.0.0.1:7600",
  daemonToken: "dev-daemon-token",
});

const api = createForumApi(daemon);

// The identity provider (dev header by default; headwaters OIDC when FORUM_AUTH=oidc, §3.3). The
// AuthProvider seam takes a Web `Request`, so adapt the node:http IncomingMessage's headers to one.
const auth = selectAuth();

function toWebRequest(req: IncomingMessage): Request {
  const headers = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (typeof v === "string") headers.set(k, v);
    else if (Array.isArray(v)) headers.set(k, v.join(", "));
  }
  return new Request(`http://forum.local${req.url ?? "/"}`, { method: req.method, headers });
}

async function resolveUser(req: IncomingMessage): Promise<User> {
  return (await auth.verify(toWebRequest(req))) ?? undefined;
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
      const context: ApiContext<User> = { user: await resolveUser(req), request: req };
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
  const target = daemon.writeDaemon
    ? `reads ${daemon.daemonUrl} · writes ${daemon.writeDaemon.url}`
    : `daemon ${daemon.daemonUrl}`;
  console.log(`[api] listening on http://127.0.0.1:${port} (${target})`);
});
