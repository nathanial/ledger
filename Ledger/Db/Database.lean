/-
  Ledger.Db.Database

  Immutable database snapshot - the core "database as a value" abstraction.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Index.Manager
import Ledger.Tx.Types

namespace Ledger

/-- Immutable database snapshot.
    Each Db value represents the state of the database at a specific point in time.
    Queries run against a Db always see a consistent view. -/
structure Db where
  /-- The basis transaction - the most recent transaction in this snapshot. -/
  basisT : TxId
  /-- All four indexes for efficient querying. -/
  indexes : Indexes
  /-- Next available entity ID for new entities. -/
  nextEntityId : EntityId
  deriving Inhabited

namespace Db

/-- Create an empty database. -/
def empty : Db :=
  { basisT := TxId.genesis
  , indexes := Indexes.empty
  , nextEntityId := ⟨1⟩ }

/-- Get the number of datoms in the database. -/
def size (db : Db) : Nat := db.indexes.count

/-- Allocate a new entity ID. Returns the ID and updated database. -/
def allocEntityId (db : Db) : EntityId × Db :=
  let eid := db.nextEntityId
  let db' := { db with nextEntityId := ⟨db.nextEntityId.id + 1⟩ }
  (eid, db')

/-- Allocate multiple entity IDs. -/
def allocEntityIds (db : Db) (n : Nat) : List EntityId × Db :=
  let startId := db.nextEntityId.id
  let ids := (List.range n).map fun (i : Nat) => EntityId.mk (startId + i)
  let db' := { db with nextEntityId := ⟨startId + n⟩ }
  (ids, db')

/-- Process a transaction, producing a new database snapshot.
    This is a pure function - the original database is unchanged. -/
def transact (db : Db) (tx : Transaction) (instant : Nat := 0) : Except TxError (Db × TxReport) := do
  let newTxId := db.basisT.next

  -- Convert transaction operations to datoms
  let mut datoms : Array Datom := #[]
  let mut indexes := db.indexes

  for op in tx do
    match op with
    | .add entity attr value =>
      let datom := Datom.assert entity attr value newTxId
      datoms := datoms.push datom
      indexes := indexes.insertDatom datom

    | .retract entity attr value =>
      let datom := Datom.retract entity attr value newTxId
      datoms := datoms.push datom
      indexes := indexes.insertDatom datom

  let db' : Db :=
    { db with
      basisT := newTxId
      indexes := indexes }

  let report : TxReport :=
    { txId := newTxId
    , txData := datoms
    , txInstant := instant }

  return (db', report)

-- ============================================================
-- Entity-based queries (use EAVT index)
-- ============================================================

/-- Get all datoms for an entity. -/
def entity (db : Db) (e : EntityId) : List Datom :=
  db.indexes.datomsForEntity e

/-- Get all values for a specific attribute of an entity. -/
def get (db : Db) (e : EntityId) (a : Attribute) : List Value :=
  db.indexes.valuesForEntityAttr e a

/-- Get a single value for an attribute (assumes cardinality one).
    Returns the most recently asserted value. -/
def getOne (db : Db) (e : EntityId) (a : Attribute) : Option Value :=
  (db.indexes.valuesForEntityAttr e a).head?

-- ============================================================
-- Attribute-based queries (use AEVT index)
-- ============================================================

/-- Get all datoms with a specific attribute across all entities. -/
def datomsWithAttr (db : Db) (a : Attribute) : List Datom :=
  db.indexes.datomsForAttr a

/-- Get all entities that have a specific attribute. -/
def entitiesWithAttr (db : Db) (a : Attribute) : List EntityId :=
  db.indexes.entitiesWithAttr a

-- ============================================================
-- Value-based queries (use AVET index)
-- ============================================================

/-- Find entities where attribute equals a specific value. -/
def findByAttrValue (db : Db) (a : Attribute) (v : Value) : List EntityId :=
  db.indexes.entitiesWithAttrValue a v

/-- Find a single entity by unique attribute value. -/
def findOneByAttrValue (db : Db) (a : Attribute) (v : Value) : Option EntityId :=
  db.indexes.entityWithAttrValue a v

-- ============================================================
-- Reverse reference queries (use VAET index)
-- ============================================================

/-- Get all entities that reference a specific entity. -/
def referencingEntities (db : Db) (target : EntityId) : List EntityId :=
  db.indexes.entitiesReferencing target

/-- Get all datoms that reference a specific entity. -/
def referencingDatoms (db : Db) (target : EntityId) : List Datom :=
  db.indexes.datomsReferencingEntity target

/-- Get entities referencing target via a specific attribute. -/
def referencingViaAttr (db : Db) (target : EntityId) (a : Attribute) : List EntityId :=
  db.indexes.entitiesReferencingViaAttr target a

-- ============================================================
-- General queries
-- ============================================================

/-- Get all datoms in the database. -/
def datoms (db : Db) : List Datom :=
  db.indexes.allDatoms

end Db

end Ledger
