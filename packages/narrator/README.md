# @rindle/narrator

Turn a Rindle view's netted change stream into agent-ready prose.

Full docs — the agent loop this package powers (named diffs in, salience-ranked
digests out, observe→act): **[rindle.sh/docs/agents](https://rindle.sh/docs/agents)** ·
markdown mirror: [`agents.md`](https://rindle.sh/docs/agents.md) · for agents:
[llms.txt](https://rindle.sh/llms.txt)

```ts
import { createNarrator, type NarratorRegistry } from "@rindle/narrator";

const NARRATORS: NarratorRegistry = {
  unseatedGuests: {
    salience: "alert",
    root: {
      add: ({ sub, context }) => `${sub("guest")?.name ?? "Someone"} is unseated for ${context.subject}.`,
      remove: ({ context }) => `A guest got a seat for ${context.subject}.`,
    },
  },
};

const narrator = createNarrator(NARRATORS);
store.materialize(unseatedGuestsQuery({ eventId }), {
  onChanges: (changes, phase, schema) => {
    const block = narrator.digest(narrator.narrate("unseatedGuests", schema, changes, phase, ctx));
    if (block) agentContext.append(block);
  },
});
```

In React, `useNarration(query, NARRATORS, { ctx })` from `@rindle/narrator-react`
wires the view lifecycle and change channel for you and returns a stable `Narration`
handle — drain it with `take()` when you prompt the model. See
[rindle.sh/docs/agents](https://rindle.sh/docs/agents) for the full walkthrough
(resolution, nested `related` templates, salience, and the observe→act loop).
