/-
  Ledger.Query.Unify

  Unification of patterns against datoms.
  Matches patterns to datoms and produces variable bindings.
-/

import Ledger.Core.Datom
import Ledger.Query.AST
import Ledger.Query.Binding

namespace Ledger

namespace Unify

/-- Try to unify a term with an entity ID, given existing bindings.
    Returns updated bindings if successful. -/
def unifyEntity (term : Term) (e : EntityId) (b : Binding) : Option Binding :=
  match term with
  | .blank => some b  -- Wildcard matches anything
  | .entity e' => if e == e' then some b else none
  | .var v =>
    match b.lookup v with
    | none => some (b.bind v (.entity e))  -- Bind the variable
    | some (.entity e') => if e == e' then some b else none  -- Check consistency
    | some (.value (.ref e')) => if e == e' then some b else none
    | _ => none
  | _ => none  -- Type mismatch (e.g., value term for entity position)

/-- Try to unify a term with an attribute, given existing bindings. -/
def unifyAttr (term : Term) (a : Attribute) (b : Binding) : Option Binding :=
  match term with
  | .blank => some b
  | .attr a' => if a == a' then some b else none
  | .var v =>
    match b.lookup v with
    | none => some (b.bind v (.attr a))
    | some (.attr a') => if a == a' then some b else none
    | _ => none
  | _ => none

/-- Try to unify a term with a value, given existing bindings. -/
def unifyValue (term : Term) (v : Value) (b : Binding) : Option Binding :=
  match term with
  | .blank => some b
  | .value v' => if v == v' then some b else none
  | .entity e =>
    -- Entity term in value position matches ref values
    match v with
    | .ref e' => if e == e' then some b else none
    | _ => none
  | .var var =>
    match b.lookup var with
    | none => some (b.bind var (.value v))
    | some (.value v') => if v == v' then some b else none
    | some (.entity e) =>
      -- Check if value is a ref to the same entity
      match v with
      | .ref e' => if e == e' then some b else none
      | _ => none
    | _ => none
  | _ => none

/-- Try to unify a pattern with a datom, given existing bindings.
    Returns updated bindings if the pattern matches. -/
def unifyDatom (pattern : Pattern) (d : Datom) (b : Binding) : Option Binding := do
  -- Only match asserted datoms (not retractions)
  if !d.added then none
  else
    let b ← unifyEntity pattern.entity d.entity b
    let b ← unifyAttr pattern.attr d.attr b
    unifyValue pattern.value d.value b

/-- Match a pattern against a list of datoms, producing all valid bindings. -/
def matchPattern (pattern : Pattern) (datoms : List Datom) (b : Binding) : Relation :=
  ⟨datoms.filterMap fun d => unifyDatom pattern d b⟩

/-- Match a pattern against datoms starting from empty bindings. -/
def matchPatternFresh (pattern : Pattern) (datoms : List Datom) : Relation :=
  matchPattern pattern datoms Binding.empty

end Unify

end Ledger
