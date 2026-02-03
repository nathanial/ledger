/-
  Ledger.Index.Manager

  Unified management of all four database indexes.
  Provides atomic operations across all indexes.
-/

import Ledger.Index.EAVT
import Ledger.Index.AEVT
import Ledger.Index.AVET
import Ledger.Index.VAET

namespace Ledger

/-- All database indexes bundled together. -/
structure Indexes where
  /-- Entity-Attribute-Value-Transaction (primary for entity lookup) -/
  eavt : EAVTIndex
  /-- Attribute-Entity-Value-Transaction (for attribute queries) -/
  aevt : AEVTIndex
  /-- Attribute-Value-Entity-Transaction (for value lookups) -/
  avet : AVETIndex
  /-- Value-Attribute-Entity-Transaction (for reverse references) -/
  vaet : VAETIndex
  deriving Inhabited

namespace Indexes

/-- Create empty indexes. -/
def empty : Indexes :=
  { eavt := EAVTIndex.empty
  , aevt := AEVTIndex.empty
  , avet := AVETIndex.empty
  , vaet := VAETIndex.empty }

/-- Insert a datom into all indexes atomically. -/
def insertDatom (idx : Indexes) (d : Datom) : Indexes :=
  { eavt := idx.eavt.insertDatom d
  , aevt := idx.aevt.insertDatom d
  , avet := idx.avet.insertDatom d
  , vaet := idx.vaet.insertDatom d }

/-- Remove a datom from all indexes atomically. -/
def removeDatom (idx : Indexes) (d : Datom) : Indexes :=
  { eavt := idx.eavt.removeDatom d
  , aevt := idx.aevt.removeDatom d
  , avet := idx.avet.removeDatom d
  , vaet := idx.vaet.removeDatom d }

/-- Insert multiple datoms into all indexes. -/
def insertDatoms (idx : Indexes) (ds : List Datom) : Indexes :=
  ds.foldl insertDatom idx

/-- Remove multiple datoms from all indexes. -/
def removeDatoms (idx : Indexes) (ds : List Datom) : Indexes :=
  ds.foldl removeDatom idx

/-- Count of datoms (from EAVT, which contains all datoms). -/
def count (idx : Indexes) : Nat :=
  idx.eavt.count

/-- Get all datoms (from EAVT). -/
def allDatoms (idx : Indexes) : List Datom :=
  idx.eavt.allDatoms

-- ============================================================
-- Entity-based queries (use EAVT)
-- ============================================================

/-- Get all datoms for an entity. -/
def datomsForEntity (e : EntityId) (idx : Indexes) : List Datom :=
  idx.eavt.datomsForEntity e

/-- Get all datoms for an entity and attribute. -/
def datomsForEntityAttr (e : EntityId) (a : Attribute) (idx : Indexes) : List Datom :=
  idx.eavt.datomsForEntityAttr e a

/-- Get all datoms for an entity, attribute, and value. -/
def datomsForEntityAttrValue (e : EntityId) (a : Attribute) (v : Value) (idx : Indexes) : List Datom :=
  idx.eavt.datomsForEntityAttrValue e a v

/-- Remove all datoms for a specific fact (entity, attribute, value). -/
def removeFact (e : EntityId) (a : Attribute) (v : Value) (idx : Indexes) : Indexes :=
  removeDatoms idx (datomsForEntityAttrValue e a v idx)

/-- Get values for an entity's attribute. -/
def valuesForEntityAttr (e : EntityId) (a : Attribute) (idx : Indexes) : List Value :=
  idx.eavt.valuesForEntityAttr e a

-- ============================================================
-- Attribute-based queries (use AEVT)
-- ============================================================

/-- Get all datoms with a specific attribute. -/
def datomsForAttr (a : Attribute) (idx : Indexes) : List Datom :=
  idx.aevt.datomsForAttr a

/-- Get all entities that have a specific attribute. -/
def entitiesWithAttr (a : Attribute) (idx : Indexes) : List EntityId :=
  idx.aevt.entitiesWithAttr a

-- ============================================================
-- Value-based queries (use AVET)
-- ============================================================

/-- Get all datoms with a specific attribute and value. -/
def datomsForAttrValue (a : Attribute) (v : Value) (idx : Indexes) : List Datom :=
  idx.avet.datomsForAttrValue a v

/-- Get entities with a specific attribute value. -/
def entitiesWithAttrValue (a : Attribute) (v : Value) (idx : Indexes) : List EntityId :=
  idx.avet.entitiesWithAttrValue a v

/-- Get the first entity with a specific attribute value (for unique attrs). -/
def entityWithAttrValue (a : Attribute) (v : Value) (idx : Indexes) : Option EntityId :=
  idx.avet.entityWithAttrValue a v

-- ============================================================
-- Reverse reference queries (use VAET)
-- ============================================================

/-- Get all datoms that reference a specific entity. -/
def datomsReferencingEntity (target : EntityId) (idx : Indexes) : List Datom :=
  idx.vaet.datomsReferencingEntity target

/-- Get all entities that reference a specific entity. -/
def entitiesReferencing (target : EntityId) (idx : Indexes) : List EntityId :=
  idx.vaet.entitiesReferencing target

/-- Get datoms referencing an entity via a specific attribute. -/
def datomsReferencingViaAttr (target : EntityId) (a : Attribute) (idx : Indexes) : List Datom :=
  idx.vaet.datomsReferencingViaAttr target a

/-- Get entities referencing a target via a specific attribute. -/
def entitiesReferencingViaAttr (target : EntityId) (a : Attribute) (idx : Indexes) : List EntityId :=
  idx.vaet.entitiesReferencingViaAttr target a

end Indexes

end Ledger
