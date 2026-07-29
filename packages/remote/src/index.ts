// @rindle/remote — the network backend for @rindle/client: a `RemoteBackend` over a transport,
// driving the SAME Store/ArrayView as the local (wasm/replica) backends. Also exports the
// wire protocol (frames + `Publisher`/`Subscriber` + `schemaFp`) a server uses to talk to it.
// Re-exports @rindle/client so an app can import everything from here.

export * from "@rindle/client";

export { RemoteBackend, createRemoteStore } from "./backend.ts";
export type { RemoteBackendOptions } from "./backend.ts";
export { WsTransport } from "./transport.ts";
export type { Transport } from "./transport.ts";

// Browser-side follower-affinity ticket transport (FOLLOWER-AFFINITY-DESIGN.md §3, §5).
export { WS_SUBPROTOCOL, createAffinityTicketStore, offerSubprotocols } from "./affinity.ts";
export type { AffinityTicketStore, TicketPersistence } from "./affinity.ts";
export type {
  MutationEnvelopeSender,
  RawMutationSender,
  SubscribeMode,
  SubscribeRequest,
  SubscribeResolver,
  SubscribeTarget,
} from "./subscribe.ts";

export { COMPARATOR_VERSION, Publisher, ProtocolError, Subscriber, schemaFp } from "./protocol.ts";
export type { Batch, ClientMsg, Hello, ProtocolErrorKind, ServerMsg, SubscribeClientMsg } from "./protocol.ts";

// The normalized subscription protocol (the path-free twin) + the client-side source.
export { NormalizedSubscriber, normalizedFp } from "./normalized.ts";
export type { NormalizedBatch, NormalizedHello } from "./normalized.ts";
export { RemoteNormalizedSource, createRemoteNormalizedSource } from "./remote-source.ts";
export type { RemoteNormalizedSourceOptions } from "./remote-source.ts";

// The optimistic-writes source (the normalized stream + mutation push + progress frames).
export { RemoteOptimisticSource, createRemoteOptimisticSource } from "./optimistic-source.ts";
export type {
  RemoteOptimisticConnection,
  RemoteOptimisticSourceOptions,
  TransportFactory,
} from "./optimistic-source.ts";

// The serverless mutation queue: confirmed, in-order batches over an unordered HTTP hop.
export { createQueuedMutationSender } from "./mutation-queue.ts";
export type { PushOutcome, QueuedMutationSenderOptions } from "./mutation-queue.ts";
