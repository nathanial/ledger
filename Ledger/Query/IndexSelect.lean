/-
  Ledger.Query.IndexSelect

  Index selection for query patterns.
  Chooses the most selective index based on bound terms in a pattern.
-/

import Ledger.Core.Datom
import Ledger.Index.Manager
import Ledger.Query.AST
import Ledger.Query.Binding

namespace Ledger

/-- Which index to use for a pattern. -/
inductive IndexChoice where
  /-- Use EAVT (entity is bound). -/
  | eavt
  /-- Use AEVT (attribute is bound). -/
  | aevt
  /-- Use AVET (attribute and value are bound). -/
  | avet
  /-- Use VAET (value is a ref). -/
  | vaet
  /-- Full scan (nothing bound). -/
  | scan
  deriving Repr, BEq

namespace IndexSelect

/-- Check if a term is effectively bound (constant or already bound variable). -/
def isBound (term : Term) (b : Binding) : Bool :=
  match term with
  | .entity _ | .attr _ | .value _ => true
  | .var v => b.isBound v
  | .blank => false

/-- Get the entity ID from a bound term. -/
def getEntity (term : Term) (b : Binding) : Option EntityId :=
  match term with
  | .entity e => some e
  | .var v =>
    match b.lookup v with
    | some (.entity e) => some e
    | some (.value (.ref e)) => some e
    | _ => none
  | _ => none

/-- Get the attribute from a bound term. -/
def getAttr (term : Term) (b : Binding) : Option Attribute :=
  match term with
  | .attr a => some a
  | .var v =>
    match b.lookup v with
    | some (.attr a) => some a
    | _ => none
  | _ => none

/-- Get the value from a bound term. -/
def getValue (term : Term) (b : Binding) : Option Value :=
  match term with
  | .value v => some v
  | .entity e => some (.ref e)
  | .var v =>
    match b.lookup v with
    | some (.value v) => some v
    | some (.entity e) => some (.ref e)
    | _ => none
  | _ => none

/-- Choose the best index for a pattern given current bindings.
    Priority: EAVT (entity bound) > AVET (attr+value) > AEVT (attr) > VAET (ref value) > scan -/
def chooseIndex (pattern : Pattern) (b : Binding) : IndexChoice :=
  let eBound := isBound pattern.entity b
  let aBound := isBound pattern.attr b
  let vBound := isBound pattern.value b

  if eBound then
    .eavt  -- Entity bound: EAVT is most selective
  else if aBound && vBound then
    .avet  -- Attr + value bound: AVET for value lookup
  else if aBound then
    .aevt  -- Just attribute bound: AEVT
  else if vBound then
    -- Check if value is a ref for VAET
    match getValue pattern.value b with
    | some (.ref _) => .vaet
    | _ => .scan
  else
    .scan  -- Nothing bound, full scan

/-- Fetch candidate datoms using the selected index. -/
def fetchCandidates (pattern : Pattern) (b : Binding) (idx : Indexes) : List Datom :=
  match chooseIndex pattern b with
  | .eavt =>
    match getEntity pattern.entity b with
    | some e =>
      match getAttr pattern.attr b with
      | some a => idx.datomsForEntityAttr e a
      | none => idx.datomsForEntity e
    | none => idx.allDatoms  -- Shouldn't happen
  | .aevt =>
    match getAttr pattern.attr b with
    | some a => idx.datomsForAttr a
    | none => idx.allDatoms
  | .avet =>
    match getAttr pattern.attr b, getValue pattern.value b with
    | some a, some v => idx.datomsForAttrValue a v
    | some a, none => idx.datomsForAttr a
    | _, _ => idx.allDatoms
  | .vaet =>
    match getValue pattern.value b with
    | some (.ref e) => idx.datomsReferencingEntity e
    | _ => idx.allDatoms
  | .scan =>
    idx.allDatoms

/-- Estimate selectivity of a pattern (lower is more selective).
    Used for query optimization. -/
def selectivity (pattern : Pattern) (b : Binding) : Nat :=
  let eBound := if isBound pattern.entity b then 1 else 0
  let aBound := if isBound pattern.attr b then 1 else 0
  let vBound := if isBound pattern.value b then 1 else 0
  -- More bound terms = more selective (lower score)
  3 - (eBound + aBound + vBound)

end IndexSelect

end Ledger
