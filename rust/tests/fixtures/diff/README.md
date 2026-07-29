# Rust-JS IVM Diff Fixtures

These JSON fixtures are recorded from the JS engine in
`/home/mlaw/workspace/mono/packages/zql` and replayed by
`tests/diff_fixtures.rs` against the Rust builder, operators, and production
`ArrayView`.

Regenerate from the mono checkout:

```sh
cd /home/mlaw/workspace/mono
GEN_FIXTURES=1 FIXTURE_OUT=/home/mlaw/workspace/rusty-ivm/tests/fixtures/diff \
  pnpm exec vitest --root /home/mlaw/workspace/rusty-ivm run tools/gen_diff_fixtures.test.ts
```

The passing corpus is intentionally JS-parity only: if Rust is deliberately more
correct than JS for a push path, that case belongs in local Rust tests and in the
divergence ledger below, not as an equality fixture.

## Current Coverage

| Contract area | Fixture stems |
| --- | --- |
| Source filters, ordering, start, limit | `issue_filter_order_start_limit_edit_enters_window` |
| Plain related joins and child pushes | `issue_comments_fetch_and_child_add` |
| Related `Take` boundary changes | `issue_comments_take_child_add_displaces_boundary` |
| Nested related joins | `issue_comments_reactions_nested_child_add` |
| Top-level non-flipped EXISTS | `issue_exists_child_add_flips_parent_on` |
| Top-level flipped EXISTS | `issue_flipped_exists_child_add_flips_parent_on` |
| OR leaf plus flipped EXISTS and alias suffixing | `issue_or_leaf_or_flipped_exists_child_add` |
| Same raw EXISTS alias under top-level AND/OR | `issue_same_alias_and_exists_child_add`, `issue_same_alias_or_exists_fetch` |
| Mixed flipped plus non-flipped EXISTS under OR | `issue_mixed_or_flipped_nonflipped_exists_fetch` |
| Nested AND/OR with flipped plus non-flipped EXISTS | `issue_nested_flipped_or_and_comment_add`, `issue_and_level_exists_with_flipped_or_fetch` |
| Root limit over a flipped OR fan | `issue_flipped_or_root_limit_fetch` |
| Materialized related over a flipped OR fan | `issue_related_over_flipped_or_label_add` |
| Duplicate related alias last-writer-wins | `issue_duplicate_related_alias_last_writer_child_add` |

Each `PushCase` generates both `fetch__...` and `push__...` fixtures. Fetch-only
cases generate only `fetch__...`.

## Intentional Divergences

### Non-Flipped EXISTS Under OR: Push Overlay

JS incremental pushes are lossy when a non-flipped EXISTS is inside an OR fan.
The JS source overlay can miss the in-flight child while re-counting the EXISTS
relationship, so some pushes produce no change even though a fresh fetch after
the same source update includes a different result.

Rust uses a re-materializing overlay and upholds:

```text
view after push == fresh fetch after the same source state
```

Known divergent push shapes are kept out of this JS-parity fixture corpus:

| Shape | JS push behavior | Rust behavior | Local coverage |
| --- | --- | --- | --- |
| `x = 1 OR EXISTS_flipped(comments) OR EXISTS(labels)`, add first `label` to a bare row | Drops the push | Adds the row | `tests/exists_or_mixed.rs::nonflipped_child_add_flips_a_bare_row_to_add` |
| Same query, add first `label` to a row already kept by the leaf branch | Drops the push | Emits a `Child(labels, Add)` | `tests/exists_or_mixed.rs::nonflipped_child_add_on_a_leaf_matched_row_emits_a_child` |
| Same query, add parent row with a pre-existing label | Drops the push | Adds the row | `tests/exists_or_mixed.rs::parent_add_matching_only_the_nonflipped_branch_via_a_preexisting_label` |
| Same query, remove the last non-flipped label branch child | May drop or under-report the membership/child change | Maintains consistency with fresh fetch | `tests/union_fan_consistency.rs` |
| Nested OR fan inside a larger AND/OR, remove the final non-flipped label after a flipped branch was removed | Drops the final remove | Removes the row | `tests/exists_or_nested.rs::e_nested_cross_exists_suppression_then_remove` |
| `(owner = 1 OR EXISTS_flipped(comments)) LIMIT 2`, add a new low-sort parent matching the leaf branch | Leaves `view.data` at the old window | Updates the window to match fresh fetch | `tests/exists_or_nested.rs::g_flip_or_with_root_limit_push_respects_take_boundary` |

Fetches for these shapes are still valid JS-parity coverage and may appear in
the corpus.

## JS Harness Notes

Two fetch-only cases use or require narrower oracle handling than the default
JS `runFetchTest`/`runPushTest` storage check:

| Shape | Fixture status | Reason |
| --- | --- | --- |
| Same raw alias under top-level OR | `issue_same_alias_or_exists_fetch` uses an `ArrayView` data-only JS oracle | JS `runFetchTest` reports a Catch-vs-View storage mismatch while the recorded artifact is only `ArrayView.data`. |
| `EXISTS(tags) AND (EXISTS_flipped(comments) OR EXISTS(labels))`, tag add push | Fetch-only as `issue_and_level_exists_with_flipped_or_fetch` | JS `runPushTest` reports a Catch-vs-View storage mismatch for the tag-add push. Local Rust coverage remains in `tests/exists_or_nested.rs::f_tag_add_keeps_the_and_level_relationship`. |
