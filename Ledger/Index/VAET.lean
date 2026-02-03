/-
  Ledger.Index.VAET

  Value-Attribute-Entity-Transaction index.
  Primary index for reverse reference lookups.
  Only indexes datoms where the value is a reference (Value.ref).
-/

import Batteries.Data.RBMap
import Batteries.Data.HashMap
import Ledger.Core.Datom
import Ledger.Index.Types
import Ledger.Index.RBRange

namespace Ledger

/-- VAET index using RBMap for ordered access.
    Only contains datoms where value is a reference. -/
abbrev VAETIndex := Batteries.RBMap VAETKey Datom compare

namespace VAETIndex

/-- Create an empty VAET index. -/
def empty : VAETIndex := Batteries.RBMap.empty

/-- Create key from a datom. -/
def keyOf (d : Datom) : VAETKey :=
  { value := d.value
  , attr := d.attr
  , entity := d.entity
  , tx := d.tx }

/-- Insert a datom into the index.
    Only inserts if the value is a reference. -/
def insertDatom (idx : VAETIndex) (d : Datom) : VAETIndex :=
  if d.value.isRef then
    Batteries.RBMap.insert idx (keyOf d) d
  else
    idx

/-- Remove a datom from the index.
    Only removes if the value is a reference. -/
def removeDatom (idx : VAETIndex) (d : Datom) : VAETIndex :=
  if d.value.isRef then
    Batteries.RBMap.erase idx (keyOf d)
  else
    idx

/-- Get all datoms that reference a specific entity.
    This is the primary use case for VAET - finding "who points to me".
    Uses early termination to avoid full index scan. -/
def datomsReferencingEntity (target : EntityId) (idx : VAETIndex) : List Datom :=
  let refValue := Value.ref target
  RBRange.collectFromWhile idx (VAETKey.minForValue refValue) (VAETKey.matchesValue refValue)

/-- Get all entities that reference a specific entity.
    Implementation: O(n) using HashMap instead of O(n²) eraseDups. -/
def entitiesReferencing (target : EntityId) (idx : VAETIndex) : List EntityId :=
  let datoms := datomsReferencingEntity target idx
  -- Use HashMap as a set for O(n) deduplication instead of O(n²) eraseDups
  let seen : Std.HashMap EntityId Unit := {}
  let (_, result) := datoms.foldl (init := (seen, #[])) fun (seen, acc) d =>
    if seen.contains d.entity then (seen, acc)
    else (seen.insert d.entity (), acc.push d.entity)
  result.toList

/-- Get all datoms that reference a specific entity via a specific attribute.
    E.g., "find all entities where :person/friend points to entity X"
    Uses early termination to avoid full index scan. -/
def datomsReferencingViaAttr (target : EntityId) (a : Attribute) (idx : VAETIndex) : List Datom :=
  let refValue := Value.ref target
  RBRange.collectFromWhile idx (VAETKey.minForValueAttr refValue a) (VAETKey.matchesValueAttr refValue a)

/-- Get entities that reference a target via a specific attribute. -/
def entitiesReferencingViaAttr (target : EntityId) (a : Attribute) (idx : VAETIndex) : List EntityId :=
  (datomsReferencingViaAttr target a idx).map (·.entity)

/-- Get all datoms in the index. -/
def allDatoms (idx : VAETIndex) : List Datom :=
  (Batteries.RBMap.toList idx).map Prod.snd

/-- Count of datoms in the index (only ref datoms). -/
def count (idx : VAETIndex) : Nat :=
  Batteries.RBMap.size idx

end VAETIndex

end Ledger
