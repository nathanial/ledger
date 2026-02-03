/-
  Ledger.Index.EAVT

  Entity-Attribute-Value-Transaction index.
  Primary index for entity lookups.
-/

import Batteries.Data.RBMap
import Ledger.Core.Datom
import Ledger.Index.Types
import Ledger.Index.RBRange

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

/-- Remove a datom from the index. -/
def removeDatom (idx : EAVTIndex) (d : Datom) : EAVTIndex :=
  Batteries.RBMap.erase idx (keyOf d)

/-- Look up a specific datom by its full key. -/
def findByKey (idx : EAVTIndex) (key : EAVTKey) : Option Datom :=
  Batteries.RBMap.find? idx key

/-- Get all datoms for an entity (range scan).
    Returns datoms in EAVT order.
    Uses early termination for O(s + k) complexity where s = elements before range,
    k = elements in range. Avoids full O(n) list allocation. -/
def datomsForEntity (e : EntityId) (idx : EAVTIndex) : List Datom :=
  RBRange.collectFromWhile idx (EAVTKey.minForEntity e) (EAVTKey.matchesEntity e)

/-- Get all datoms for an entity and attribute (range scan).
    Uses early termination to avoid full index scan. -/
def datomsForEntityAttr (e : EntityId) (a : Attribute) (idx : EAVTIndex) : List Datom :=
  RBRange.collectFromWhile idx (EAVTKey.minForEntityAttr e a) (EAVTKey.matchesEntityAttr e a)

/-- Get all datoms for an entity, attribute, and value (range scan).
    Uses early termination to avoid full index scan. -/
def datomsForEntityAttrValue (e : EntityId) (a : Attribute) (v : Value) (idx : EAVTIndex) : List Datom :=
  RBRange.collectFromWhile idx
    (EAVTKey.minForEntityAttrValue e a v)
    (EAVTKey.matchesEntityAttrValue e a v)

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
