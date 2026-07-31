// `useStreamedText` — render an LM response that is arriving on two planes
// (designs/LM-STREAM-CHECKPOINT-DESIGN.md).
//
// The durable plane already flows through IVM like any other data: the app's chat query carries the
// message row and its un-compacted chunk rows, and `assembleDurableText` turns those into the text
// the store holds. This hook adds the LIVE plane — the tail that has not been checkpointed yet — and
// merges the two with `spliceStreamText`.
//
// It exists because the merge is one line but the SUBSCRIPTION has five traps, and every app would
// otherwise have to find them independently. Each is marked TRAP below:
//
//   1. the join offset must be read WITHOUT making the durable text a dependency (it advances on
//      every checkpoint, and a dependency would tear the connection down and rebuild it);
//   2. the accumulator must be SEEDED with the text it joined at (the splice compares lengths, so a
//      tail carrying only the frames it received reads as shorter than the durable text and is
//      discarded);
//   3. the reader must close its own `EventSource` on a terminal frame (`EventSource` reconnects on
//      ANY close, including a clean one);
//   4. the accumulated tail must be keyed to its stream (a tail left over from the previous message
//      is a PREFIX of nothing, and for one render it can be longer than the new durable text and win
//      the splice);
//   5. a chunk must be spliced at ITS OWN offset, never blindly appended: an `EventSource` reconnect
//      resumes at the last id it saw, and a `durable` frame's id sits at the STORE's position, which
//      trails the chunk progress — so a resumed replay can start BEHIND the accumulator, and an
//      append would duplicate the overlap into the rendered text.
//
// Losing the live plane is not an error: `absent` and `stale` mean "the store is the whole truth
// now", and the hook simply stops accumulating. The rendered text stays correct — it just advances at
// checkpoint granularity, which is what a reader on the wrong instance or an old runtime gets anyway.

import { useEffect, useRef, useState } from "react";
import { spliceStreamText } from "@rindle/client";
import type { StreamFrame } from "@rindle/client";

/** Where {@link useStreamedText} subscribes by default — `DEFAULT_RINDLE_API_ROUTES.stream`, mirrored
 *  here rather than imported so the browser never pulls `@rindle/api-server`. */
export const DEFAULT_STREAM_ENDPOINT = "/api/rindle/stream";

/**
 * How the live plane is reached. The default ({@link eventSourceTransport}) is SSE, which is what the
 * api-server's `streamFramesToSse` serves and what gets `Last-Event-ID` resume for free. Supply your
 * own for a WebSocket, a fetch-stream, or a test.
 */
export interface StreamTransport {
  /** Attach at `url` and call `onFrame` per decoded frame. MUST return a detach function; it may be
   *  called more than once and must tolerate that. `onFrame` may be called synchronously. */
  subscribe(url: string, onFrame: (frame: StreamFrame) => void): () => void;
}

export interface UseStreamedTextInput {
  /** The stream's id — the message row's key. Changing it drops the old tail and rejoins. */
  streamId: string;
  /** What the IVM view shows: `assembleDurableText(message, message.chunks)`. Read from a ref
   *  internally (TRAP 1), so it may change every checkpoint without disturbing the connection. */
  durable: string;
  /** Whether a producer is still running — the app's own read of its status column (typically
   *  `status === "streaming" || status === "pending"`). The live leg attaches only while true. */
  live: boolean;
}

export interface UseStreamedTextOptions {
  /** Default {@link DEFAULT_STREAM_ENDPOINT}. `streamId` and `from` are appended as query params. */
  endpoint?: string;
  /** Default {@link eventSourceTransport}. Read at subscribe time, NOT a dependency — an inline
   *  literal would otherwise reconnect on every render. */
  transport?: StreamTransport;
  /** A frame that could not be decoded, or a transport-level error. The durable plane is unaffected,
   *  so this is a diagnostic, not a failure. */
  onError?: (err: unknown) => void;
}

/** `<endpoint>?streamId=…&from=…`. `from` is the join offset; a reconnecting `EventSource` overrides
 *  it with its own `Last-Event-ID` header, which the server prefers. */
export function streamSubscribeUrl(endpoint: string, streamId: string, from: number): string {
  const sep = endpoint.includes("?") ? "&" : "?";
  return `${endpoint}${sep}streamId=${encodeURIComponent(streamId)}&from=${from}`;
}

/** The default SSE transport. Absent `EventSource` (SSR, an older runtime, a test without jsdom) it
 *  attaches nothing and the reader stays on the durable plane — correct, just chunkier. */
export function eventSourceTransport(onError?: (err: unknown) => void): StreamTransport {
  return {
    subscribe(url, onFrame) {
      const Ctor = (globalThis as { EventSource?: new (url: string) => EventSourceLike }).EventSource;
      if (!Ctor) return () => {};
      let es: EventSourceLike;
      try {
        es = new Ctor(url);
      } catch (err) {
        onError?.(err);
        return () => {};
      }
      es.onmessage = (event: { data: string }) => {
        try {
          onFrame(JSON.parse(event.data) as StreamFrame);
        } catch (err) {
          onError?.(err);
        }
      };
      // A transport-level error is not a stream error: `EventSource` retries on its own, and the
      // durable plane keeps the reader correct meanwhile.
      es.onerror = (event: unknown) => onError?.(event);
      return () => es.close();
    },
  };
}

interface EventSourceLike {
  onmessage: ((event: { data: string }) => void) | null;
  onerror: ((event: unknown) => void) | null;
  close(): void;
}

/**
 * The response text as it should be rendered right now: the durable prefix spliced with the live
 * tail.
 *
 * ```tsx
 * const data = useFragment(MessageFragment, message);
 * const text = useStreamedText({
 *   streamId: data.id,
 *   durable: assembleDurableText(data, data.chunks),
 *   live: data.status === "streaming" || data.status === "pending",
 * });
 * ```
 *
 * The value is monotone in practice — it only grows while a stream runs — and when the closing
 * checkpoint compacts the chunks into the body it returns the identical string, so there is no
 * flicker at the handoff.
 */
export function useStreamedText(
  { streamId, durable, live }: UseStreamedTextInput,
  options: UseStreamedTextOptions = {},
): string {
  const { endpoint = DEFAULT_STREAM_ENDPOINT } = options;

  // TRAP 1: the join offset is read from a ref at subscribe time. Making `durable` a dependency would
  // reconnect on every checkpoint — a fresh HTTP request every ~512 characters.
  const durableRef = useRef(durable);
  durableRef.current = durable;
  // Read at subscribe time for the same reason: callers pass these inline.
  const optionsRef = useRef(options);
  optionsRef.current = options;

  // TRAP 4: the tail is keyed to its stream and compared during RENDER, not reset in an effect.
  // Resetting in an effect would leave one render where the previous message's (longer) tail wins the
  // splice and briefly renders the wrong message's text.
  const [tail, setTail] = useState<{ streamId: string; text: string }>({ streamId, text: "" });
  const produced = tail.streamId === streamId ? tail.text : "";

  useEffect(() => {
    if (!live) return;
    const { transport, onError } = optionsRef.current;
    // TRAP 2: seed the accumulator with the text we are joining at, so its LENGTH is comparable to
    // the durable text's.
    let acc = durableRef.current;
    // `detach` is assigned by `subscribe` itself, and a transport may deliver frames (even terminal
    // ones) synchronously from inside that call — so completion is latched and applied after.
    let detach: (() => void) | undefined;
    let done = false;
    const finish = (): void => {
      done = true;
      detach?.();
    };

    const active = transport ?? eventSourceTransport(onError);
    detach = active.subscribe(streamSubscribeUrl(endpoint, streamId, acc.length), (frame) => {
      if (done) return;
      switch (frame.type) {
        case "chunk":
          // TRAP 5: splice at the frame's own offset. A resumed transport can replay a span that
          // starts BEHIND the accumulator (both are prefixes of one response, so cutting at `from`
          // and appending is exact); a span that starts AHEAD would be a gap — detach and let the
          // durable plane carry the reader, which stays correct at checkpoint granularity.
          if (frame.from > acc.length) {
            optionsRef.current.onError?.(
              new Error(`stream ${streamId}: chunk at ${frame.from} leaves a gap after ${acc.length}`),
            );
            finish();
            break;
          }
          acc = acc.slice(0, frame.from) + frame.text;
          setTail({ streamId, text: acc });
          break;
        case "end":
        case "stale":
        case "absent":
          // TRAP 3. On `stale`/`absent` the durable plane already carries everything, and on `end` the
          // stream is sealed — in all three cases another connection would be pure waste.
          finish();
          break;
        default:
          // `open` and `durable` carry no text. `durable` is a durability signal the renderer does
          // not need: the splice is length-based, so the handoff needs no announcement.
          break;
      }
    });
    if (done) detach();

    return finish;
    // `durable` is deliberately absent (TRAP 1); `transport`/`onError` are read from the ref.
  }, [streamId, live, endpoint]);

  return spliceStreamText(durable, produced);
}
