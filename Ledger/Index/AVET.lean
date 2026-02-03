/-
  Ledger.Index.AVET

  Attribute-Value-Entity-Transaction index.
  Primary index for value lookups (e.g., "find entity where :person/email = 'foo@bar.com'").
  Especially useful for unique attributes.
-/

import Batteries.Data.RBMap
import Batteries.Data.HashMap
import Ledger.Core.Datom
import Ledger.Index.Types
import Ledger.Index.RBRange

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

/-- Remove a datom from the index. -/
def removeDatom (idx : AVETIndex) (d : Datom) : AVETIndex :=
  Batteries.RBMap.erase idx (keyOf d)

/-- Get all datoms for an attribute and value (range scan).
    Useful for finding entities with a specific attribute value.
    Only returns assertions (added = true), not retractions.
    Uses early termination to avoid full index scan. -/
def datomsForAttrValue (a : Attribute) (v : Value) (idx : AVETIndex) : List Datom :=
  RBRange.collectFromWhile idx (AVETKey.minForAttrValue a v) (AVETKey.matchesAttrValue a v)
  |>.filter (·.added)

/-- Get entities with a specific attribute value.
    Primary use case: lookup by unique attribute.
    Filters out entities where the fact has been retracted.

    Implementation: Uses range query for O(s + k) range access, then
    HashMap to track each entity's latest transaction state. -/
def entitiesWithAttrValue (a : Attribute) (v : Value) (idx : AVETIndex) : List EntityId :=
  -- Get only datoms in range using early termination
  let datoms := RBRange.collectPairsWhile idx
    (fun k => AVETKey.matchesAttrValue a v k)

  -- Build HashMap of entity -> (latestTxId, isAdded)
  let entityState : Std.HashMap EntityId (Nat × Bool) :=
    datoms.foldl (init := {}) fun acc (_, d) =>
      match acc[d.entity]? with
      | none => acc.insert d.entity (d.tx.id, d.added)
      | some (prevTxId, _) =>
        if d.tx.id > prevTxId
        then acc.insert d.entity (d.tx.id, d.added)
        else acc

  -- Filter to entities where latest transaction was an assertion
  entityState.toList.filterMap fun (e, (_, added)) =>
    if added then some e else none

/-- Get the first entity with a specific attribute value.
    Useful for unique attributes where only one entity should match. -/
def entityWithAttrValue (a : Attribute) (v : Value) (idx : AVETIndex) : Option EntityId :=
  (entitiesWithAttrValue a v idx).head?

/-- Get all datoms for an attribute (less efficient than AEVT for this).
    Uses early termination but still needs to scan all values for the attribute. -/
def datomsForAttr (a : Attribute) (idx : AVETIndex) : List Datom :=
  RBRange.collectWhile idx (fun k => k.attr == a)

/-- Get all datoms in the index. -/
def allDatoms (idx : AVETIndex) : List Datom :=
  (Batteries.RBMap.toList idx).map Prod.snd

/-- Count of datoms in the index. -/
def count (idx : AVETIndex) : Nat :=
  Batteries.RBMap.size idx

end AVETIndex

end Ledger
