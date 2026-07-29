# @rindle/narrator-react

React integration for [`@rindle/narrator`](../narrator): subscribe to a query's live, netted change
stream and buffer semantic events without re-rendering the component on each change.

Full docs — the narration model and the observe→act loop:
**[rindle.sh/docs/agents](https://rindle.sh/docs/agents)** · markdown mirror:
[`agents.md`](https://rindle.sh/docs/agents.md) · for agents:
[llms.txt](https://rindle.sh/llms.txt)

```sh
pnpm add @rindle/narrator @rindle/narrator-react
```

```tsx
import { useNarration } from "@rindle/narrator-react";
import type { NarratorRegistry } from "@rindle/narrator";

const NARRATORS: NarratorRegistry = {
  unseatedGuests: {
    salience: "alert",
    root: { add: ({ row }) => `${row.name as string} needs a seat.` },
  },
};

const advisor = useNarration(unseatedGuestsQuery({ eventId }), NARRATORS, { ctx });

// When preparing the next agent prompt:
for (const event of advisor.take()) agentContext.append(event.text);
```

Wrap the component tree in `Rindle` from `@rindle/react`. The returned handle is stable; `take()`
drains events accumulated since its previous call, and `clear()` discards them. By default the hook
ignores the initial snapshot and buffers only later batches. Pass `phases: ["snapshot", "batch"]` to
include initial rows.
