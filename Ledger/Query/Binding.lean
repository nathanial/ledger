/-
  Ledger.Query.Binding

  Variable bindings and relations for query execution.
  A binding maps variables to values, and a relation is a set of bindings.
-/

import Std.Data.HashMap
import Ledger.Core.EntityId
import Ledger.Core.Value
import Ledger.Query.AST

namespace Ledger

/-- A bound value - what a variable can be bound to. -/
inductive BoundValue where
  /-- Bound to an entity ID. -/
  | entity (e : EntityId)
  /-- Bound to a value. -/
  | value (v : Value)
  /-- Bound to an attribute. -/
  | attr (a : Attribute)
  deriving Repr, BEq, Inhabited

namespace BoundValue

/-- Convert a Value to a BoundValue. -/
def fromValue (v : Value) : BoundValue := .value v

/-- Convert an EntityId to a BoundValue. -/
def fromEntity (e : EntityId) : BoundValue := .entity e

/-- Convert an Attribute to a BoundValue. -/
def fromAttr (a : Attribute) : BoundValue := .attr a

/-- Try to get as an EntityId. -/
def asEntity? : BoundValue → Option EntityId
  | .entity e => some e
  | .value (.ref e) => some e
  | _ => none

/-- Try to get as a Value. -/
def asValue? : BoundValue → Option Value
  | .value v => some v
  | .entity e => some (.ref e)
  | _ => none

end BoundValue

/-- A binding maps variables to bound values. -/
structure Binding where
  entries : Std.HashMap Var BoundValue := {}
  deriving Repr, Inhabited

instance : BEq Binding where
  beq a b :=
    if a.entries.size != b.entries.size then
      false
    else
      a.entries.toList.all fun (v, val) =>
        match b.entries[v]? with
        | some other => other == val
        | none => false

namespace Binding

/-- Empty binding. -/
def empty : Binding := ⟨{}⟩

/-- Look up a variable in the binding. -/
def lookup (b : Binding) (v : Var) : Option BoundValue :=
  b.entries[v]?

/-- Check if a variable is bound. -/
def isBound (b : Binding) (v : Var) : Bool :=
  b.entries.contains v

/-- Bind a variable to a value. -/
def bind (b : Binding) (v : Var) (val : BoundValue) : Binding :=
  ⟨b.entries.insert v val⟩

/-- Get all bound variables. -/
def vars (b : Binding) : List Var :=
  b.entries.toList.map Prod.fst

/-- Build a binding from a list of pairs. Later entries overwrite earlier ones. -/
def ofList (entries : List (Var × BoundValue)) : Binding :=
  let map := entries.foldl (init := ({} : Std.HashMap Var BoundValue)) fun acc (v, val) =>
    acc.insert v val
  ⟨map⟩

/-- Merge two bindings. Returns none if there's a conflict. -/
def merge (b1 b2 : Binding) : Option Binding :=
  b2.entries.toList.foldlM (init := b1) fun acc (v, val) =>
    match acc.lookup v with
    | none => some (acc.bind v val)
    | some existing => if existing == val then some acc else none

/-- Project binding to only include specified variables. -/
def project (b : Binding) (vs : List Var) : Binding :=
  let filtered := vs.foldl (init := ({} : Std.HashMap Var BoundValue)) fun acc v =>
    match b.lookup v with
    | some val => acc.insert v val
    | none => acc
  ⟨filtered⟩

/-- Convert binding to a list of values in variable order. -/
def toValues (b : Binding) (vs : List Var) : List (Option BoundValue) :=
  vs.map (b.lookup ·)

end Binding

/-- A relation is a set of bindings (like a table). -/
structure Relation where
  bindings : List Binding
  deriving Repr, Inhabited

namespace Relation

/-- Empty relation. -/
def empty : Relation := ⟨[]⟩

/-- Single-row relation. -/
def singleton (b : Binding) : Relation := ⟨[b]⟩

/-- Add a binding to the relation. -/
def add (r : Relation) (b : Binding) : Relation := ⟨b :: r.bindings⟩

/-- Number of rows. -/
def size (r : Relation) : Nat := r.bindings.length

/-- Check if empty. -/
def isEmpty (r : Relation) : Bool := r.bindings.isEmpty

/-- Join two relations on common variables. -/
def join (r1 r2 : Relation) : Relation :=
  let results := r1.bindings.flatMap fun b1 =>
    r2.bindings.filterMap fun b2 =>
      b1.merge b2
  ⟨results⟩

/-- Project relation to only include specified variables. -/
def project (r : Relation) (vs : List Var) : Relation :=
  ⟨r.bindings.map (·.project vs)⟩

/-- Remove duplicate bindings. -/
def distinct (r : Relation) : Relation :=
  let unique := r.bindings.foldl (init := ([] : List Binding)) fun acc b =>
    if acc.any (· == b) then acc else b :: acc
  ⟨unique⟩

/-- Filter relation by a predicate on bindings. -/
def filter (r : Relation) (p : Binding → Bool) : Relation :=
  ⟨r.bindings.filter p⟩

/-- Flat map over bindings. -/
def flatMap (r : Relation) (f : Binding → List Binding) : Relation :=
  ⟨r.bindings.flatMap f⟩

end Relation

end Ledger
