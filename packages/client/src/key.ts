// A stable, canonical JSON string for a query AST — the `viewKey` shared by the React cache
// (one cached view per AST) and the SSR dehydrate/hydrate map (seed lookup by AST). Both sides
// MUST agree byte-for-byte, so the one canonical serializer lives here. Object keys are sorted;
// `undefined` is encoded distinctly (so `{a:undefined}` ≠ `{}`); cycles throw.

export function stableKey(value: unknown, seen = new WeakSet<object>()): string {
  if (value === undefined) return '{"$undefined":true}';
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (seen.has(value)) throw new TypeError("Rindle query keys must be acyclic JSON values");
  seen.add(value);
  if (Array.isArray(value)) {
    const out = `[${value.map((v) => stableKey(v, seen)).join(",")}]`;
    seen.delete(value);
    return out;
  }
  const obj = value as Record<string, unknown>;
  const out = `{${Object.keys(obj)
    .sort()
    .map((k) => `${JSON.stringify(k)}:${stableKey(obj[k], seen)}`)
    .join(",")}}`;
  seen.delete(value);
  return out;
}
