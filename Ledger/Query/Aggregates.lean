/-
  Ledger.Query.Aggregates

  Aggregation functions for query results.
  Supports count, sum, avg, min, max over relation bindings.
-/

import Ledger.Query.AST
import Ledger.Query.Binding

namespace Ledger

/-- Convert Int to Float. -/
private def intToFloat (n : Int) : Float :=
  if n >= 0 then n.toNat.toFloat
  else -((-n).toNat.toFloat)

/-- Convert Float to Int (truncating). -/
private def floatToInt (f : Float) : Int :=
  if f >= 0 then f.toUInt64.toNat
  else -((-f).toUInt64.toNat)

/-- Aggregate function types. -/
inductive AggregateFunc where
  /-- Count of non-null values (or all rows if no variable specified). -/
  | count
  /-- Sum of numeric values. -/
  | sum
  /-- Average of numeric values. -/
  | avg
  /-- Minimum value. -/
  | min
  /-- Maximum value. -/
  | max
  deriving Repr, BEq, Inhabited

namespace AggregateFunc

instance : ToString AggregateFunc where
  toString
    | .count => "count"
    | .sum => "sum"
    | .avg => "avg"
    | .min => "min"
    | .max => "max"

end AggregateFunc

/-- An aggregate specification: function applied to a variable. -/
structure AggregateSpec where
  /-- The aggregate function to apply. -/
  func : AggregateFunc
  /-- The variable to aggregate (optional for count). -/
  var : Option Var
  /-- Name for the result column. -/
  resultName : Var
  deriving Repr, BEq, Inhabited

namespace AggregateSpec

/-- Create a count aggregate (counts all rows). -/
def count (name : String := "count") : AggregateSpec :=
  { func := .count, var := none, resultName := ⟨name⟩ }

/-- Create a count aggregate for a specific variable. -/
def countVar (v : String) (name : String := "count") : AggregateSpec :=
  { func := .count, var := some ⟨v⟩, resultName := ⟨name⟩ }

/-- Create a sum aggregate. -/
def sum (v : String) (name : String := "sum") : AggregateSpec :=
  { func := .sum, var := some ⟨v⟩, resultName := ⟨name⟩ }

/-- Create an avg aggregate. -/
def avg (v : String) (name : String := "avg") : AggregateSpec :=
  { func := .avg, var := some ⟨v⟩, resultName := ⟨name⟩ }

/-- Create a min aggregate. -/
def min (v : String) (name : String := "min") : AggregateSpec :=
  { func := .min, var := some ⟨v⟩, resultName := ⟨name⟩ }

/-- Create a max aggregate. -/
def max (v : String) (name : String := "max") : AggregateSpec :=
  { func := .max, var := some ⟨v⟩, resultName := ⟨name⟩ }

end AggregateSpec

/-- Result of an aggregation - a single value. -/
inductive AggregateValue where
  /-- Integer result (count, sum of ints). -/
  | int (v : Int)
  /-- Float result (avg, sum of floats). -/
  | float (v : Float)
  /-- Value result (min/max preserves original type). -/
  | value (v : Value)
  /-- Null result (empty input or all nulls). -/
  | null
  deriving Repr, BEq, Inhabited

namespace AggregateValue

instance : ToString AggregateValue where
  toString
    | .int n => toString n
    | .float f => toString f
    | .value v => toString v
    | .null => "null"

/-- Convert to a BoundValue for binding. -/
def toBoundValue : AggregateValue → Option BoundValue
  | .int n => some (.value (.int n))
  | .float f => some (.value (.float f))
  | .value v => some (.value v)
  | .null => none

end AggregateValue

namespace Aggregate

/-- Extract numeric value from a BoundValue for aggregation. -/
private def numericValue (bv : BoundValue) : Option Float :=
  match bv with
  | .value (.int n) => some (intToFloat n)
  | .value (.float f) => some f
  | _ => none

/-- Count values in a relation (optionally for a specific variable). -/
def count (rel : Relation) (v : Option Var) : AggregateValue :=
  match v with
  | none => .int rel.size
  | some var =>
    let nonNull := rel.bindings.filter fun b =>
      (b.lookup var).isSome
    .int nonNull.length

/-- Sum numeric values for a variable. -/
def sum (rel : Relation) (v : Var) : AggregateValue :=
  let values := rel.bindings.filterMap fun b =>
    b.lookup v >>= numericValue
  if values.isEmpty then .null
  else
    -- Check if all original values were ints
    let allInts := rel.bindings.all fun b =>
      match b.lookup v with
      | some (.value (.int _)) => true
      | some (.value (.float _)) => false
      | _ => true  -- Ignore non-numeric
    let total := values.foldl (· + ·) 0.0
    if allInts then .int (floatToInt total)
    else .float total

/-- Average numeric values for a variable. -/
def avg (rel : Relation) (v : Var) : AggregateValue :=
  let values := rel.bindings.filterMap fun b =>
    b.lookup v >>= numericValue
  if values.isEmpty then .null
  else
    let total := values.foldl (· + ·) 0.0
    let count := values.length.toFloat
    .float (total / count)

/-- Find minimum in a list using Ord instance. -/
private def listMin [Ord α] (xs : List α) : Option α :=
  xs.foldl (init := none) fun acc x =>
    match acc with
    | none => some x
    | some m => if compare x m == .lt then some x else some m

/-- Find maximum in a list using Ord instance. -/
private def listMax [Ord α] (xs : List α) : Option α :=
  xs.foldl (init := none) fun acc x =>
    match acc with
    | none => some x
    | some m => if compare x m == .gt then some x else some m

/-- Minimum value for a variable. -/
def min (rel : Relation) (v : Var) : AggregateValue :=
  let values := rel.bindings.filterMap fun b =>
    match b.lookup v with
    | some (.value val) => some val
    | _ => none
  match listMin values with
  | some val => .value val
  | none => .null

/-- Maximum value for a variable. -/
def max (rel : Relation) (v : Var) : AggregateValue :=
  let values := rel.bindings.filterMap fun b =>
    match b.lookup v with
    | some (.value val) => some val
    | _ => none
  match listMax values with
  | some val => .value val
  | none => .null

/-- Apply an aggregate function to a relation. -/
def apply (spec : AggregateSpec) (rel : Relation) : AggregateValue :=
  match spec.func, spec.var with
  | .count, v => count rel v
  | .sum, some v => sum rel v
  | .sum, none => .null  -- sum requires a variable
  | .avg, some v => avg rel v
  | .avg, none => .null  -- avg requires a variable
  | .min, some v => min rel v
  | .min, none => .null  -- min requires a variable
  | .max, some v => max rel v
  | .max, none => .null  -- max requires a variable

/-- Check if two bindings have equal values for a set of variables. -/
private def bindingsEqualOn (b1 b2 : Binding) (vars : List Var) : Bool :=
  vars.all fun v => b1.lookup v == b2.lookup v

/-- Group a relation by specified variables. -/
def groupBy (rel : Relation) (groupVars : List Var) : List (Binding × Relation) :=
  -- Project each binding to group key, collect unique keys
  let groups := rel.bindings.foldl (init := ([] : List (Binding × List Binding)))
    fun acc b =>
      let key := b.project groupVars
      match acc.find? fun (k, _) => bindingsEqualOn k key groupVars with
      | some _ =>
        acc.map fun (k, m) =>
          if bindingsEqualOn k key groupVars then (k, b :: m) else (k, m)
      | none => (key, [b]) :: acc
  groups.map fun (key, members) => (key, ⟨members⟩)

/-- Result of an aggregate query. -/
structure AggregateResult where
  /-- Group key columns. -/
  groupColumns : List Var
  /-- Aggregate result columns. -/
  aggregateColumns : List Var
  /-- Result rows: (group key binding, aggregate values). -/
  rows : List (Binding × List AggregateValue)
  deriving Repr, Inhabited

namespace AggregateResult

/-- Number of result rows. -/
def size (r : AggregateResult) : Nat := r.rows.length

/-- Check if empty. -/
def isEmpty (r : AggregateResult) : Bool := r.rows.isEmpty

/-- Convert to a relation with all columns. -/
def toRelation (r : AggregateResult) : Relation :=
  let bindings := r.rows.map fun (groupKey, aggValues) =>
    -- Start with group key binding, add aggregate values
    let aggBindings := (r.aggregateColumns.zip aggValues).foldl
      (init := groupKey) fun b (col, val) =>
        match val.toBoundValue with
        | some bv => b.bind col bv
        | none => b  -- Skip null values
    aggBindings
  ⟨bindings⟩

end AggregateResult

/-- Execute aggregation on a relation. -/
def execute (rel : Relation) (groupVars : List Var) (specs : List AggregateSpec)
    : AggregateResult :=
  if groupVars.isEmpty then
    -- No grouping - aggregate entire relation
    let values := specs.map (apply · rel)
    { groupColumns := []
    , aggregateColumns := specs.map (·.resultName)
    , rows := [(Binding.empty, values)] }
  else
    -- Group by specified variables
    let groups := groupBy rel groupVars
    let rows := groups.map fun (key, members) =>
      let values := specs.map (apply · members)
      (key, values)
    { groupColumns := groupVars
    , aggregateColumns := specs.map (·.resultName)
    , rows := rows }

end Aggregate

end Ledger
