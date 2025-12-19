/-
  Ledger.Index.AEVT

  Attribute-Entity-Value-Transaction index.
  Primary index for attribute-based queries (e.g., "all entities with :person/name").
-/

import Batteries.Data.RBMap
import Ledger.Core.Datom
import Ledger.Index.Types

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

/-- Get all datoms for an attribute (range scan).
    Returns all entities that have this attribute. -/
def datomsForAttr (a : Attribute) (idx : AEVTIndex) : List Datom :=
  (Batteries.RBMap.toList idx).filterMap fun (k, d) =>
    if k.attr == a then some d else none

/-- Get all datoms for an attribute and entity. -/
def datomsForAttrEntity (a : Attribute) (e : EntityId) (idx : AEVTIndex) : List Datom :=
  (Batteries.RBMap.toList idx).filterMap fun (k, d) =>
    if k.attr == a && k.entity == e then some d else none

/-- Get all entities that have a specific attribute. -/
def entitiesWithAttr (a : Attribute) (idx : AEVTIndex) : List EntityId :=
  (datomsForAttr a idx).map (·.entity) |>.eraseDups

/-- Get all datoms in the index. -/
def allDatoms (idx : AEVTIndex) : List Datom :=
  (Batteries.RBMap.toList idx).map Prod.snd

/-- Count of datoms in the index. -/
def count (idx : AEVTIndex) : Nat :=
  Batteries.RBMap.size idx

end AEVTIndex

end Ledger
