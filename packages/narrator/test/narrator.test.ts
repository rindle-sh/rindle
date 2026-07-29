// Self-contained unit tests over hand-crafted WireSchema + FlatChange fixtures (no wasm engine):
// resolution + template rendering + digest ordering. The live view-channel path (`view.onChanges`)
// is exercised end-to-end in `@rindle/client`'s `view-onchanges` tests, and against the real wasm
// engine in `@rindle/optimistic`'s `narration-engine` test.

import assert from "node:assert/strict";
import { test } from "node:test";

import type { FlatChange, WireSchema } from "@rindle/client";

import { createNarrator, type NarratorRegistry } from "../src/index.ts";

// ── fixtures ──────────────────────────────────────────────────────────────────

// A presence query: root rsvp rows, each with a nested `guest` sub (slot 0).
const guestSchema: WireSchema = { columns: ["id", "name"], primaryKey: [0], sort: [[0, true]], singular: false, relationships: [] };
const unseatedSchema: WireSchema = {
  columns: ["id", "status"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: false,
  relationships: [{ name: "guest", slot: 0, child: guestSchema, project: null }],
};
const unseatedAdd: FlatChange = {
  path: [],
  op: { tag: "add", node: { row: ["r-dana", "yes"], rels: [{ rel: 0, children: [{ row: ["g-dana", "Dana Lopez"], rels: [] }] }] } },
};

// An enum-fanned count query: one event row, a `yes` countAs slot (project col 1).
const yesCountSchema: WireSchema = { columns: ["status", "cnt"], primaryKey: [0], sort: [[0, true]], singular: true, relationships: [] };
const funnelSchema: WireSchema = {
  columns: ["id", "name"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: true,
  relationships: [{ name: "yes", slot: 0, child: yesCountSchema, project: { col: 1, identity: 0 } }],
};
const yesCountEdit: FlatChange = {
  path: [{ rel: 0, parentRow: ["evt", "Wedding"] }],
  op: { tag: "edit", old: ["yes", 1], new: ["yes", 2] },
};

const narrators: NarratorRegistry = {
  unseatedGuests: {
    salience: "alert",
    root: {
      add: ({ sub, context }) => `${sub("guest")?.name ?? "Someone"} is unseated for ${context.subject ?? "the event"}.`,
      remove: ({ context }) => `A guest got a seat for ${context.subject ?? "the event"}.`,
    },
  },
  rsvpFunnel: {
    salience: "ambient",
    counts: { yes: ({ aggregate }) => `yes count is now ${aggregate?.value}.` },
  },
};

// ── tests ─────────────────────────────────────────────────────────────────────

test("narrate: a root add resolves the nested sub-row name", () => {
  const n = createNarrator(narrators);
  const events = n.narrate("unseatedGuests", unseatedSchema, [unseatedAdd], "snapshot", { subject: "the wedding" });
  assert.equal(events.length, 1);
  assert.equal(events[0].salience, "alert");
  assert.equal(events[0].text, "Dana Lopez is unseated for the wedding.");
});

test("narrate: an aggregate edit reports the EXACT new value (and previous)", () => {
  const n = createNarrator(narrators);
  const events = n.narrate("rsvpFunnel", funnelSchema, [yesCountEdit], "batch", {});
  assert.equal(events.length, 1);
  assert.equal(events[0].text, "yes count is now 2.");
  assert.deepEqual(events[0].resolved.aggregate, { alias: "yes", value: 2, previous: 1 });
});

test("narrate: an unknown query yields events with null text (nothing rendered)", () => {
  const n = createNarrator(narrators);
  const events = n.narrate("notRegistered", unseatedSchema, [unseatedAdd], "batch", {});
  assert.equal(events.length, 1);
  assert.equal(events[0].text, null);
});

test("digest: salience-ranked, suppressions dropped, glyph-marked", () => {
  const n = createNarrator(narrators);
  const alert = n.narrate("unseatedGuests", unseatedSchema, [unseatedAdd], "batch", { subject: "the wedding" });
  const ambient = n.narrate("rsvpFunnel", funnelSchema, [yesCountEdit], "batch", {});
  const digest = n.digest([...ambient, ...alert]); // ambient first in input…
  const lines = digest.split("\n");
  assert.equal(lines.length, 2);
  assert.ok(lines[0].startsWith("⚠️"), "alert ranks above ambient");
  assert.ok(lines[1].startsWith("·"));
});

// ── nested / whole-tree narration (`related`) ───────────────────────────────────

// A deck (.one() root) → slides → components tree, narrated off ONE view.
const componentSchema: WireSchema = { columns: ["id", "kind"], primaryKey: [0], sort: [[0, true]], singular: false, relationships: [] };
const slideSchema: WireSchema = {
  columns: ["id", "title"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: false,
  relationships: [{ name: "components", slot: 0, child: componentSchema, project: null }],
};
const deckSchema: WireSchema = {
  columns: ["id", "name"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: true,
  relationships: [{ name: "slides", slot: 0, child: slideSchema, project: null }],
};

// A slide edit nested under the deck; a component add nested under a slide.
const slideEdit: FlatChange = {
  path: [{ rel: 0, parentRow: ["d1", "My Deck"] }],
  op: { tag: "edit", old: ["s3", "Old title"], new: ["s3", "New title"] },
};
const componentAdd: FlatChange = {
  path: [
    { rel: 0, parentRow: ["d1", "My Deck"] },
    { rel: 0, parentRow: ["s5", "Pricing"] },
  ],
  op: { tag: "add", node: { row: ["c9", "chart"], rels: [] } },
};

const deckNarrators: NarratorRegistry = {
  deck: {
    salience: "info",
    root: {
      edit: ({ row, old }) => (row.name !== old?.name ? `Deck renamed to "${row.name as string}".` : null),
    },
    related: {
      slides: {
        edit: ({ row, old, parent }) =>
          row.title !== old?.title ? `Slide "${row.title as string}" reworded in "${parent?.name as string}".` : null,
      },
      components: {
        salience: "ambient",
        add: ({ row, parent }) => `Added a ${row.kind as string} to slide "${parent?.title as string}".`,
      },
    },
  },
};

test("narrate: a nested relationship change matches a `related` template (whole-tree narration)", () => {
  const n = createNarrator(deckNarrators);
  const events = n.narrate("deck", deckSchema, [slideEdit], "batch", {});
  assert.equal(events.length, 1);
  assert.equal(events[0].text, 'Slide "New title" reworded in "My Deck".');
  assert.equal(events[0].salience, "info"); // inherits the query default
});

test("narrate: a deeply nested change resolves its IMMEDIATE parent + a related salience override", () => {
  const n = createNarrator(deckNarrators);
  const events = n.narrate("deck", deckSchema, [componentAdd], "batch", {});
  assert.equal(events.length, 1);
  assert.equal(events[0].text, 'Added a chart to slide "Pricing".'); // parent = the SLIDE, not the deck
  assert.equal(events[0].salience, "ambient"); // the `related` override wins over the query default
});

test("narrate: root + nested changes from one view narrate together", () => {
  const n = createNarrator(deckNarrators);
  const deckRename: FlatChange = { path: [], op: { tag: "edit", old: ["d1", "My Deck"], new: ["d1", "Q3 Review"] } };
  const events = n.narrate("deck", deckSchema, [deckRename, slideEdit], "batch", {}).filter((e) => e.text);
  assert.equal(events.length, 2);
  assert.equal(events[0].text, 'Deck renamed to "Q3 Review".'); // root template
  assert.equal(events[1].text, 'Slide "New title" reworded in "My Deck".'); // nested template
});

// ── same-leaf `related` disambiguation by dotted alias-chain ─────────────────────

// A deck with `components` under TWO branches: slides.components (visual) and handouts.components
// (a doc attachment with a different shape). Same leaf alias, different tree position.
const handoutCompSchema: WireSchema = { columns: ["id", "filename"], primaryKey: [0], sort: [[0, true]], singular: false, relationships: [] };
const handoutSchema: WireSchema = {
  columns: ["id", "label"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: false,
  relationships: [{ name: "components", slot: 0, child: handoutCompSchema, project: null }],
};
const twoBranchDeckSchema: WireSchema = {
  columns: ["id", "name"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: true,
  relationships: [
    { name: "slides", slot: 0, child: slideSchema, project: null },
    { name: "handouts", slot: 1, child: handoutSchema, project: null },
  ],
};
// A component add under slides.components, and one under handouts.components — both leaf `components`.
const slideCompAdd: FlatChange = {
  path: [
    { rel: 0, parentRow: ["d1", "My Deck"] },
    { rel: 0, parentRow: ["s5", "Pricing"] },
  ],
  op: { tag: "add", node: { row: ["c9", "chart"], rels: [] } },
};
const handoutCompAdd: FlatChange = {
  path: [
    { rel: 1, parentRow: ["d1", "My Deck"] },
    { rel: 0, parentRow: ["h2", "Appendix"] },
  ],
  op: { tag: "add", node: { row: ["f7", "budget.pdf"], rels: [] } },
};

test("narrate: `related` keyed by dotted alias-chain disambiguates two same-leaf relationships", () => {
  const dotted: NarratorRegistry = {
    deck: {
      salience: "info",
      related: {
        "slides.components": { add: ({ row, parent }) => `Added a ${row.kind as string} to slide "${parent?.title as string}".` },
        "handouts.components": { add: ({ row, parent }) => `Attached ${row.filename as string} to handout "${parent?.label as string}".` },
      },
    },
  };
  const n = createNarrator(dotted);
  const [slide] = n.narrate("deck", twoBranchDeckSchema, [slideCompAdd], "batch", {});
  const [handout] = n.narrate("deck", twoBranchDeckSchema, [handoutCompAdd], "batch", {});
  assert.equal(slide.text, 'Added a chart to slide "Pricing".');
  assert.equal(handout.text, "Attached budget.pdf to handout \"Appendix\".", "the second branch uses its OWN template, not the first's");
});

test("narrate: a bare leaf-alias `related` key still matches at any depth (back-compat)", () => {
  const leaf: NarratorRegistry = {
    deck: { salience: "info", related: { components: { add: ({ row }) => `component ${row.id as string}` } } },
  };
  const n = createNarrator(leaf);
  assert.equal(n.narrate("deck", twoBranchDeckSchema, [slideCompAdd], "batch", {})[0].text, "component c9");
  assert.equal(n.narrate("deck", twoBranchDeckSchema, [handoutCompAdd], "batch", {})[0].text, "component f7");
});
