import type { Mutation, MutationEnvelope, QueryId, RemoteQuery } from "@rindle/client";

import type { SubscribeClientMsg } from "./protocol.ts";

export type SubscribeMode = "flat" | "normalized";

export interface SubscribeRequest {
  queryId: QueryId;
  remote: RemoteQuery;
  mode: SubscribeMode;
}

export type SubscribeTarget =
  | { name: string; args: unknown; leaseToken?: never; wsEndpoint?: never }
  // A lease may carry the follower's public ws endpoint (READ-ROUTER-DESIGN.md §2.3): the source
  // connects (or migrates) its transport there before subscribing. Client-side routing only — it is
  // NOT put on the wire `subscribe` frame.
  | { leaseToken: string; wsEndpoint?: string; name?: never; args?: never };

export type SubscribeResolver = (request: SubscribeRequest) => SubscribeTarget | PromiseLike<SubscribeTarget>;

export type RawMutationSender = (mutations: Mutation[]) => void | PromiseLike<void>;

export type MutationEnvelopeSender = (envelope: MutationEnvelope) => void | PromiseLike<void>;

export function defaultSubscribeTarget(request: SubscribeRequest): SubscribeTarget {
  return { name: request.remote.name, args: request.remote.args };
}

export function subscribeMessage(request: SubscribeRequest, target: SubscribeTarget): SubscribeClientMsg {
  const mode = request.mode === "normalized" ? { mode: request.mode } : {};
  if (target.leaseToken !== undefined) {
    return { t: "subscribe", queryId: request.queryId, leaseToken: target.leaseToken, ...mode };
  }
  return { t: "subscribe", queryId: request.queryId, name: target.name, args: target.args, ...mode };
}

export function isThenable<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return value != null && typeof (value as { then?: unknown }).then === "function";
}
