/-
  Ledger.Index.EAVT

  Entity-Attribute-Value-Transaction index.
  Primary index for entity lookups.
-/

import Batteries.Data.RBMap
import Ledger.Core.Datom
import Ledger.Index.Types

namespace Ledger

/-- EAVT index using RBMap for ordered access. -/
abbrev EAVTIndex := Batteries.RBMap EAVTKey Datom compare

namespace EAVTIndex

/-- Create an empty EAVT index. -/
def empty : EAVTIndex := Batteries.RBMap.empty

/-- Create key from a datom. -/
def keyOf (d : Datom) : EAVTKey :=
  { entity := d.entity
  , attr := d.attr
  , value := d.value
  , tx := d.tx }

/-- Insert a datom into the index. -/
def insertDatom (idx : EAVTIndex) (d : Datom) : EAVTIndex :=
  Batteries.RBMap.insert idx (keyOf d) d

/-- Look up a specific datom by its full key. -/
def findByKey (idx : EAVTIndex) (key : EAVTKey) : Option Datom :=
  Batteries.RBMap.find? idx key

/-- Get all datoms for an entity (range scan).
    Returns datoms in EAVT order. -/
def datomsForEntity (e : EntityId) (idx : EAVTIndex) : List Datom :=
  -- We need to find all entries where entity = e
  -- Since RBMap doesn't have direct range queries, we filter
  (Batteries.RBMap.toList idx).filterMap fun (k, d) =>
    if k.entity == e then some d else none

/-- Get all datoms for an entity and attribute (range scan). -/
def datomsForEntityAttr (e : EntityId) (a : Attribute) (idx : EAVTIndex) : List Datom :=
  (Batteries.RBMap.toList idx).filterMap fun (k, d) =>
    if k.entity == e && k.attr == a then some d else none

/-- Get a specific value for an entity and attribute (most recent assertion).
    Note: This returns all matching datoms, caller should filter by tx for current value. -/
def valuesForEntityAttr (e : EntityId) (a : Attribute) (idx : EAVTIndex) : List Value :=
  (datomsForEntityAttr e a idx).map (·.value)

/-- Get all datoms in the index. -/
def allDatoms (idx : EAVTIndex) : List Datom :=
  (Batteries.RBMap.toList idx).map Prod.snd

/-- Count of datoms in the index. -/
def count (idx : EAVTIndex) : Nat :=
  Batteries.RBMap.size idx

end EAVTIndex

end Ledger
