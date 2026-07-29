// Named value-operators (`eq`/`gt`/`like`/…) + boolean combinators (`or`/`and`) for the
// query builder. Operators are VALUE-ONLY — the value type `V` is inferred from the value,
// and the FIELD is bound separately (by `where.<field>(…)` or `table.<field>(…)`), so
// `or`/`exists` need no closure (WASM-CLIENT-DESIGN.md §6).

import type { Condition, LitValue, SimpleOp } from "./ast.ts";

const PRED: unique symbol = Symbol("rindle.pred");

/** A value predicate (an operator applied to a value). `V` is a phantom for type-checking. */
export interface Pred<V> {
  readonly [PRED]: true;
  readonly op: SimpleOp;
  readonly value: LitValue;
  readonly __v?: V;
}

function mk<V>(op: SimpleOp, value: LitValue): Pred<V> {
  return { [PRED]: true, op, value } as Pred<V>;
}

export function isPred(x: unknown): x is Pred<unknown> {
  return typeof x === "object" && x !== null && (x as Record<symbol, unknown>)[PRED] === true;
}

export const eq = <V>(v: V): Pred<V> => mk("=", v as LitValue);
export const ne = <V>(v: V): Pred<V> => mk("!=", v as LitValue);
export const gt = <V extends number | string>(v: V): Pred<V> => mk(">", v as LitValue);
export const ge = <V extends number | string>(v: V): Pred<V> => mk(">=", v as LitValue);
export const lt = <V extends number | string>(v: V): Pred<V> => mk("<", v as LitValue);
export const le = <V extends number | string>(v: V): Pred<V> => mk("<=", v as LitValue);
export const like = (pattern: string): Pred<string> => mk("LIKE", pattern);
export const notLike = (pattern: string): Pred<string> => mk("NOT LIKE", pattern);
export const ilike = (pattern: string): Pred<string> => mk("ILIKE", pattern);
export const notIlike = (pattern: string): Pred<string> => mk("NOT ILIKE", pattern);
export const inList = <V>(values: V[]): Pred<V> => mk("IN", values as LitValue);
export const notInList = <V>(values: V[]): Pred<V> => mk("NOT IN", values as LitValue);
export const is = <V>(v: V): Pred<V> => mk("IS", v as LitValue);
export const isNot = <V>(v: V): Pred<V> => mk("IS NOT", v as LitValue);
/** `IS NULL` — a field-agnostic null check. `V` is inferred from the field's expected `Arg<V>`
 *  (e.g. `where.signedAt(isNull())`), so it fits a column of any type without a cast. */
export const isNull = <V = never>(): Pred<V> => mk("IS", null);
/** `IS NOT NULL` — the negation of {@link isNull}; `V` is inferred from the field, like {@link isNull}. */
export const isNotNull = <V = never>(): Pred<V> => mk("IS NOT", null);

/** A field argument: its typed predicate, or a bare value (bare = `eq` sugar). */
export type Arg<V> = Pred<V> | V;

/** A condition over row `R`. The runtime value is the wire {@link Condition}; `R` is a
 *  phantom brand so a condition for one table can't be `.where()`d onto another. */
export type Cond<R> = Condition & { readonly __row?: R };

/** Build a `simple` condition from a field name + (predicate | bare value). */
export function fieldCondition(field: string, arg: unknown): Condition {
  const op: SimpleOp = isPred(arg) ? arg.op : "=";
  const value: LitValue = isPred(arg) ? arg.value : (arg as LitValue);
  return {
    type: "simple",
    op,
    left: { type: "column", name: field },
    right: { type: "literal", value },
  };
}

/** All children must hold. */
export function and<R>(...conds: Cond<R>[]): Cond<R> {
  return { type: "and", conditions: conds } as Cond<R>;
}

/** At least one child must hold. */
export function or<R>(...conds: Cond<R>[]): Cond<R> {
  return { type: "or", conditions: conds } as Cond<R>;
}
