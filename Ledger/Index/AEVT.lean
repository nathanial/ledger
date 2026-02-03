/-
  Ledger.Index.AEVT

  Attribute-Entity-Value-Transaction index.
  Primary index for attribute-based queries (e.g., "all entities with :person/name").
-/

import Batteries.Data.RBMap
import Batteries.Data.HashMap
import Ledger.Core.Datom
import Ledger.Index.Types
import Ledger.Index.RBRange

namespace Ledger

/-- AEVT index using RBMap for ordered access. -/
abbrev AEVTIndex := Batteries.RBMap AEVTKey Datom compare

namespace AEVTIndex

/-- Create an empty AEVT index. -/
def empty : AEVTIndex := Batteries.RBMap.empty

/-- Create key from a datom. -/
def keyOf (d : Datom) : AEVTKey :=
  { attr := d.attr
  , entity := d.entity
  , value := d.value
  , tx := d.tx }

/-- Insert a datom into the index. -/
def insertDatom (idx : AEVTIndex) (d : Datom) : AEVTIndex :=
  Batteries.RBMap.insert idx (keyOf d) d

/-- Remove a datom from the index. -/
def removeDatom (idx : AEVTIndex) (d : Datom) : AEVTIndex :=
  Batteries.RBMap.erase idx (keyOf d)

/-- Get all datoms for an attribute (range scan).
    Returns all entities that have this attribute.
    Uses early termination to avoid full index scan. -/
def datomsForAttr (a : Attribute) (idx : AEVTIndex) : List Datom :=
  RBRange.collectFromWhile idx (AEVTKey.minForAttr a) (AEVTKey.matchesAttr a)

/-- Get all datoms for an attribute and entity.
    Uses early termination to avoid full index scan. -/
def datomsForAttrEntity (a : Attribute) (e : EntityId) (idx : AEVTIndex) : List Datom :=
  RBRange.collectFromWhile idx (AEVTKey.minForAttrEntity a e) (AEVTKey.matchesAttrEntity a e)

/-- Get all entities that have a specific attribute.
    Implementation: O(n) using HashMap instead of O(n²) eraseDups. -/
def entitiesWithAttr (a : Attribute) (idx : AEVTIndex) : List EntityId :=
  let datoms := datomsForAttr a idx
  -- Use HashMap as a set for O(n) deduplication instead of O(n²) eraseDups
  let seen : Std.HashMap EntityId Unit := {}
  let (_, result) := datoms.foldl (init := (seen, #[])) fun (seen, acc) d =>
    if seen.contains d.entity then (seen, acc)
    else (seen.insert d.entity (), acc.push d.entity)
  result.toList

/-- Get all datoms in the index. -/
def allDatoms (idx : AEVTIndex) : List Datom :=
  (Batteries.RBMap.toList idx).map Prod.snd

/-- Count of datoms in the index. -/
def count (idx : AEVTIndex) : Nat :=
  Batteries.RBMap.size idx

end AEVTIndex

end Ledger
