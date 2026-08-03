// The LM stream plane's SHARED contract (designs-implemented/LM-STREAM-CHECKPOINT-DESIGN.md): the frame shapes
// both ends speak, plus the two pure functions that reassemble a response from its two planes.
//
// This lives in `@rindle/client` rather than `@rindle/api-server` because BOTH tiers need it and the
// browser must never pull the server package (sql-client, daemon-client, …) to read a stream.
// `@rindle/api-server` re-exports every name here, so the server-side import path is unchanged.
//
// Nothing in this module has state, I/O, or a React dependency — the hook that drives a subscription
// is `useStreamedText` in `@rindle/react`; the transport is the app's choice.

/**
 * How a stream ended.
 * - `complete` — the model finished.
 * - `cancelled` — the reader asked it to stop and the producer honoured it.
 * - `error` — the generation threw.
 * - `interrupted` — the host went away mid-generation. The one status that implies the store may be
 *   short of what was produced.
 */
export type StreamStatus = "complete" | "cancelled" | "error" | "interrupted";

/** The value in the mapped status column while a stream is live. */
export const STREAM_STATUS_STREAMING = "streaming";

/**
 * One frame of a subscription. A subscription always begins with `open` and always ends with exactly
 * one terminal frame — `end`, `stale`, or `absent` — after which the iterator completes.
 *
 * `stale` and `absent` are the two "you are on the durable plane now" answers, and both are SAFE:
 * the store holds everything below `floorSeq` and everything through `durableSeq`, so the reader's
 * IVM view converges without the stream. Neither is an error.
 */
export type StreamFrame =
  /** Join accepted. `from` is the (clamped) offset the replay starts at. */
  | { type: "open"; streamId: string; from: number; seq: number; durableSeq: number; ended: boolean }
  /** PRODUCED text — not a durability claim. `text.length === seq - from`, always. */
  | { type: "chunk"; from: number; seq: number; text: string }
  /** The store now holds the prefix through `seq`. */
  | { type: "durable"; seq: number }
  /** Sealed. No further frames. */
  | { type: "end"; seq: number; status: StreamStatus; error?: string }
  /** `from` is below the producer's retained buffer floor (or the subscriber fell too far behind):
   *  read the store. (A raw `EventSource` rejoins automatically on its reconnect; `useStreamedText`
   *  deliberately stays on the durable plane instead — correct, at checkpoint granularity.) */
  | { type: "stale"; floorSeq: number; durableSeq: number }
  /** The process serving this subscribe is not hosting the stream (wrong instance, already evicted,
   *  or it never existed): the store is the whole truth. */
  | { type: "absent" };

/** The resume point a frame implies — what rides an SSE `id:` line so a reconnecting `EventSource`
 *  hands it straight back as `Last-Event-ID`. `undefined` for frames that are not a position. */
export function frameResumePoint(frame: StreamFrame): number | undefined {
  switch (frame.type) {
    case "open":
      return frame.from;
    case "chunk":
    case "durable":
    case "end":
      return frame.seq;
    default:
      return undefined;
  }
}

/**
 * Merge the durable plane with the live tail.
 *
 * `durable` is what the IVM view shows; `produced` is what a subscription has accumulated (the prefix
 * it joined at, plus every `chunk`). Both are prefixes of the same response, so the merge is "take
 * the longer" — no diffing, no overlap handling, no ranges.
 *
 * The length comparison is the whole algorithm, which is why a caller MUST seed its accumulator with
 * the text it joined at: a tail carrying only the chunks it received would read as shorter than the
 * durable text and be discarded. `useStreamedText` does that for you.
 */
export function spliceStreamText(durable: string, produced: string): string {
  return produced.length > durable.length ? durable + produced.slice(durable.length) : durable;
}

/**
 * The durable half of the splice for the mapped-table layout: the compacted `body` followed by
 * whatever chunk rows have not been folded into it yet.
 *
 * Chunks are ALWAYS the suffix after `body` — the closing checkpoint rewrites `body` and drops the
 * chunks it absorbed in ONE transaction — so a reader never observes a torn state where a chunk both
 * is and is not in the body.
 */
export function assembleDurableText(
  message: { body?: string | null } | null | undefined,
  chunks: ReadonlyArray<{ seq: number; text: string }> = [],
): string {
  const ordered = [...chunks].sort((a, b) => a.seq - b.seq);
  return (message?.body ?? "") + ordered.map((c) => c.text).join("");
}
