/-
  Ledger.Index.AVET

  Attribute-Value-Entity-Transaction index.
  Primary index for value lookups (e.g., "find entity where :person/email = 'foo@bar.com'").
  Especially useful for unique attributes.
-/

import Batteries.Data.RBMap
import Ledger.Core.Datom
import Ledger.Index.Types

namespace Ledger

/-- AVET index using RBMap for ordered access. -/
abbrev AVETIndex := Batteries.RBMap AVETKey Datom compare

namespace AVETIndex

/-- Create an empty AVET index. -/
def empty : AVETIndex := Batteries.RBMap.empty

/-- Create key from a datom. -/
def keyOf (d : Datom) : AVETKey :=
  { attr := d.attr
  , value := d.value
  , entity := d.entity
  , tx := d.tx }

/-- Insert a datom into the index. -/
def insertDatom (idx : AVETIndex) (d : Datom) : AVETIndex :=
  Batteries.RBMap.insert idx (keyOf d) d

/-- Get all datoms for an attribute and value (range scan).
    Useful for finding entities with a specific attribute value.
    Only returns assertions (added = true), not retractions. -/
def datomsForAttrValue (a : Attribute) (v : Value) (idx : AVETIndex) : List Datom :=
  (Batteries.RBMap.toList idx).filterMap fun (k, d) =>
    if k.attr == a && k.value == v && d.added then some d else none

/-- Get entities with a specific attribute value.
    Primary use case: lookup by unique attribute.
    Filters out entities where the fact has been retracted. -/
def entitiesWithAttrValue (a : Attribute) (v : Value) (idx : AVETIndex) : List EntityId :=
  let datoms := datomsForAttrValue a v idx
  -- Group by entity and check if the latest tx is an assertion
  let entities := datoms.map (·.entity) |>.eraseDups
  entities.filter fun e =>
    -- Get all datoms for this entity/attr/value and find the one with highest tx
    let entityDatoms := (Batteries.RBMap.toList idx).filterMap fun (k, d) =>
      if k.attr == a && k.value == v && d.entity == e then some d else none
    match entityDatoms.foldl (init := none) (fun acc d =>
      match acc with
      | none => some d
      | some prev => if d.tx.id > prev.tx.id then some d else some prev) with
    | some latest => latest.added
    | none => false

/-- Get the first entity with a specific attribute value.
    Useful for unique attributes where only one entity should match. -/
def entityWithAttrValue (a : Attribute) (v : Value) (idx : AVETIndex) : Option EntityId :=
  (entitiesWithAttrValue a v idx).head?

/-- Get all datoms for an attribute (less efficient than AEVT for this). -/
def datomsForAttr (a : Attribute) (idx : AVETIndex) : List Datom :=
  (Batteries.RBMap.toList idx).filterMap fun (k, d) =>
    if k.attr == a then some d else none

/-- Get all datoms in the index. -/
def allDatoms (idx : AVETIndex) : List Datom :=
  (Batteries.RBMap.toList idx).map Prod.snd

/-- Count of datoms in the index. -/
def count (idx : AVETIndex) : Nat :=
  Batteries.RBMap.size idx

end AVETIndex

end Ledger
