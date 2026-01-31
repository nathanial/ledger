/-
  Ledger.Db.Database

  Immutable database snapshot - the core "database as a value" abstraction.
-/

import Std.Data.HashMap
import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Index.Manager
import Ledger.Tx.Types
import Ledger.Schema.Types
import Ledger.Schema.Validation

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
  /-- Optional schema for validation (none = schema-free mode). -/
  schemaConfig : Option SchemaConfig := none
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

/-- Check if a specific fact is currently asserted (not retracted).
    Used to validate retractions. -/
private def isFactAsserted (indexes : Indexes) (e : EntityId) (a : Attribute) (v : Value) : Bool :=
  let datoms := indexes.datomsForEntityAttr e a |>.filter (·.value == v)
  match datoms.foldl (fun acc d => match acc with
    | none => some d
    | some prev => if d.tx.id > prev.tx.id then some d else acc) none with
  | some latest => latest.added
  | none => false

/-- Process a transaction, producing a new database snapshot.
    This is a pure function - the original database is unchanged. -/
def transact (db : Db) (tx : Transaction) (instant : Nat := 0) : Except TxError (Db × TxReport) := do
  -- Schema validation (if schema is configured)
  if let some config := db.schemaConfig then
    match SchemaValidation.validateTransaction config db.indexes tx with
    | .error schemaErr => throw (.schemaViolation (toString schemaErr))
    | .ok () => pure ()

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
      -- Validate that the fact exists in the pre-transaction state
      if !isFactAsserted db.indexes entity attr value then
        throw (.factNotFound entity attr value)
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

/-- Filter datoms to get only visible (not retracted) values.
    For each (entity, attr, value), check if the most recent datom is an assertion.
    Returns values sorted by their latest txId descending (most recent first).

    Implementation: O(n) using HashMap for grouping, then O(m log m) sort. -/
private def filterVisible (datoms : List Datom) : List Value :=
  -- Group datoms by value using HashMap: O(n)
  let grouped : Std.HashMap Value (Array Datom) := datoms.foldl
    (fun acc d =>
      match acc[d.value]? with
      | none => acc.insert d.value #[d]
      | some ds => acc.insert d.value (ds.push d))
    {}
  -- For each group, find latest and check if added, return (value, txId) pairs: O(n total)
  let visibleWithTx : List (Value × Nat) := grouped.toList.filterMap fun (v, ds) =>
    if h : ds.size > 0 then
      -- Find datom with max tx
      let latest := ds.foldl (init := ds[0]) fun best d =>
        if d.tx.id > best.tx.id then d else best
      if latest.added then some (v, latest.tx.id) else none
    else none
  -- Sort by txId descending (most recent first): O(m log m)
  let sorted := visibleWithTx.toArray.qsort (fun a b => a.2 > b.2)
  sorted.toList.map (·.1)

/-- Get all values for a specific attribute of an entity.
    Only returns values that are currently asserted (not retracted). -/
def get (db : Db) (e : EntityId) (a : Attribute) : List Value :=
  let datoms := db.indexes.datomsForEntityAttr e a
  filterVisible datoms

/-- Get a single value for an attribute (assumes cardinality one).
    Returns the most recently asserted visible value. A value is visible
    if its latest datom is an assertion (not retracted).

    Implementation: O(n) using HashMap for grouping instead of O(n²) list-based. -/
def getOne (db : Db) (e : EntityId) (a : Attribute) : Option Value :=
  let datoms := db.indexes.datomsForEntityAttr e a
  -- Group datoms by value using HashMap: O(n)
  let grouped : Std.HashMap Value (Array Datom) := datoms.foldl
    (fun acc d =>
      match acc[d.value]? with
      | none => acc.insert d.value #[d]
      | some ds => acc.insert d.value (ds.push d))
    {}
  -- For each value, check if visible (latest datom is assertion)
  -- If visible, return (value, txId of that assertion)
  let visibleWithTx : List (Value × Nat) := grouped.toList.filterMap fun (v, ds) =>
    if h : ds.size > 0 then
      -- Find datom with max tx
      let latest := ds.foldl (init := ds[0]) fun best d =>
        if d.tx.id > best.tx.id then d else best
      if latest.added then some (v, latest.tx.id) else none
    else none
  -- Return the visible value with highest txId (most recently asserted)
  let sortedVisible := visibleWithTx.toArray.qsort (fun a b => a.2 > b.2)
  if h : sortedVisible.size > 0 then some sortedVisible[0].1 else none

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

-- ============================================================
-- Schema configuration
-- ============================================================

/-- Enable schema validation on this database. -/
def withSchema (db : Db) (schema : Schema) (strict : Bool := false) : Db :=
  { db with schemaConfig := some { schema := schema, strictMode := strict } }

/-- Disable schema validation. -/
def withoutSchema (db : Db) : Db :=
  { db with schemaConfig := none }

/-- Get the current schema (if any). -/
def getSchema (db : Db) : Option Schema :=
  db.schemaConfig.map (·.schema)

/-- Check if schema validation is enabled. -/
def hasSchema (db : Db) : Bool :=
  db.schemaConfig.isSome

end Db

end Ledger
