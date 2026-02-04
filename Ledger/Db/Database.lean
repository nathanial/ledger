/-
  Ledger.Db.Database

  Immutable database snapshot - the core "database as a value" abstraction.
-/

import Std.Data.HashMap
import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Core.Util
import Ledger.Index.Manager
import Ledger.Tx.Types
import Ledger.Tx.Functions
import Ledger.Schema.Types
import Ledger.Schema.Validation
import Ledger.Schema.Install

namespace Ledger

/-- Fact key for current fact map. -/
structure FactKey where
  entity : EntityId
  attr : Attribute
  value : Value
  deriving Repr, BEq, Hashable, Inhabited

namespace FactKey

def of (e : EntityId) (a : Attribute) (v : Value) : FactKey :=
  { entity := e, attr := a, value := v }

def ofDatom (d : Datom) : FactKey :=
  { entity := d.entity, attr := d.attr, value := d.value }

end FactKey

/-- Immutable database snapshot.
    Each Db value represents the state of the database at a specific point in time.
    Queries run against a Db always see a consistent view. -/
structure Db where
  /-- The basis transaction - the most recent transaction in this snapshot. -/
  basisT : TxId
  /-- All four indexes for efficient querying (current visible facts). -/
  indexes : Indexes
  /-- Full history indexes (including retractions). -/
  historyIndexes : Indexes := Indexes.empty
  /-- Current visible facts keyed by (entity, attribute, value). -/
  currentFacts : Std.HashMap FactKey Datom := {}
  /-- Next available entity ID for new entities. -/
  nextEntityId : EntityId
  /-- Optional schema for validation (none = schema-free mode). -/
  schemaConfig : Option SchemaConfig := none
  deriving Inhabited

namespace Db

/-- Apply a datom to the current fact map. -/
private def applyCurrentFact (facts : Std.HashMap FactKey Datom) (d : Datom) :
    Std.HashMap FactKey Datom :=
  let key := FactKey.ofDatom d
  if d.added then facts.insert key d else facts.erase key

/-- Set of entity IDs (using HashMap for membership). -/
private abbrev EntitySet := Std.HashMap EntityId Unit

/-- Set of fact keys (using HashMap for membership). -/
private abbrev FactSet := Std.HashMap FactKey Unit

/-- Choose schema for component resolution. Prefers configured schema, falls back to stored schema. -/
private def schemaForComponents (db : Db) : Schema :=
  match db.schemaConfig with
  | some config => config.schema
  | none => Schema.loadFromIndexes db.indexes

/-- Check if an attribute is a component ref in the schema. -/
private def isComponentAttr (schema : Schema) (attr : Attribute) : Bool :=
  match schema.get? attr with
  | some attrSchema => attrSchema.component && attrSchema.valueType == .ref
  | none => false

/-- Resolve a lookup ref to an entity ID. Errors if not unique or not found. -/
private def resolveLookupRef (schema : Schema) (db : Db) (attr : Attribute) (value : Value)
    : Except TxError EntityId := do
  match schema.get? attr with
  | none =>
    throw (.custom s!"Lookup ref attribute not in schema: {attr}")
  | some attrSchema =>
    match attrSchema.unique with
    | none =>
      throw (.custom s!"Lookup ref attribute is not unique: {attr}")
    | some _ =>
      let candidates := db.indexes.entitiesWithAttrValue attr value
      match candidates with
      | [] => throw (.custom s!"Lookup ref not found: [{attr} {value}]")
      | [e] => return e
      | _ => throw (.custom s!"Lookup ref not unique: [{attr} {value}]")

/-- Resolve an entity ref to an entity ID (or none for no-op). -/
private def resolveEntityRef (schema : Schema) (db : Db) (eref : EntityRef) :
    Except TxError (Option EntityId) := do
  match eref with
  | .id e => return some e
  | .lookup attr value =>
    let eid ← resolveLookupRef schema db attr value
    return some eid

/-- Collect all fact keys to retract for an entity (including inbound refs and component cascade). -/
private partial def collectRetractFacts (db : Db) (schema : Schema) (entity : EntityId)
    (visited : EntitySet) (facts : FactSet) : EntitySet × FactSet :=
  if visited.contains entity then
    (visited, facts)
  else
    let visited := visited.insert entity ()
    let ownDatoms := db.indexes.datomsForEntity entity
    let inboundDatoms := db.indexes.datomsReferencingEntity entity
    let facts := (ownDatoms ++ inboundDatoms).foldl (init := facts) fun acc d =>
      acc.insert (FactKey.ofDatom d) ()
    let componentChildren := ownDatoms.filterMap fun d =>
      match d.value with
      | .ref child => if isComponentAttr schema d.attr then some child else none
      | _ => none
    componentChildren.foldl (init := (visited, facts)) fun (v, f) child =>
      collectRetractFacts db schema child v f

/-- Expand retractEntity operations into concrete retract operations. -/
private def expandRetractEntities (db : Db) (tx : Transaction) : Except TxError Transaction := do
  let schema := schemaForComponents db
  let mut expanded : Transaction := []
  let mut retracted : FactSet := {}

  for op in tx do
    match op with
    | .add _ _ _ =>
      expanded := expanded ++ [op]
    | .retract entity attr value =>
      let key := FactKey.of entity attr value
      if !retracted.contains key then
        retracted := retracted.insert key ()
        expanded := expanded ++ [op]
    | .retractEntity eref =>
      let eid? ← resolveEntityRef schema db eref
      match eid? with
      | none => pure ()
      | some eid =>
        let oldFacts := retracted
        let (_, retracted') := collectRetractFacts db schema eid {} retracted
        let newKeys := retracted'.toList
          |>.filter (fun (k, _) => !oldFacts.contains k)
          |>.map (·.1)
        expanded := expanded ++ newKeys.map (fun k => .retract k.entity k.attr k.value)
        retracted := retracted'
    | .call _ _ =>
      expanded := expanded ++ [op]

  return expanded

/-- Build current fact map from a list of datoms (in transaction order). -/
def currentFactsFromDatoms (datoms : DatomSeq) : Std.HashMap FactKey Datom :=
  datoms.foldl (init := {}) fun acc d => applyCurrentFact acc d

/-- Read-only view for tx functions. -/
private def txFuncView (db : Db) : DbView :=
  let getOne := fun e a =>
    let datoms := db.indexes.datomsForEntityAttr e a
    match datoms with
    | [] => none
    | d :: rest =>
      let latest := rest.foldl (init := d) fun best cand =>
        if cand.tx.id > best.tx.id then cand else best
      some latest.value
  let get := fun e a =>
    let datoms := db.indexes.datomsForEntityAttr e a
    let sorted := datoms.toArray.qsort (fun a b => a.tx.id > b.tx.id)
    sorted.toList.map (·.value)
  { getOne := getOne
  , get := get
  , entity := fun e => db.indexes.datomsForEntity e
  , findByAttrValue := fun a v => db.indexes.entitiesWithAttrValue a v
  , findOneByAttrValue := fun a v => db.indexes.entityWithAttrValue a v }

/-- Create an empty database. -/
def empty : Db :=
  { basisT := TxId.genesis
  , indexes := Indexes.empty
  , historyIndexes := Indexes.empty
  , currentFacts := {}
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
private def isFactAsserted (facts : Std.HashMap FactKey Datom)
    (e : EntityId) (a : Attribute) (v : Value) : Bool :=
  facts.contains (FactKey.of e a v)

/-- Process a transaction, producing a new database snapshot.
    This is a pure function - the original database is unchanged. -/
def transactWith (db : Db) (registry : TxFuncRegistry) (tx : Transaction) (instant : Nat := 0)
    : Except TxError (Db × TxReport) := do
  let ctx : TxFuncContext := { db := txFuncView db, tx := tx }
  let expandedCalls ← expandTxFunctions ctx registry TxFunctions.defaultMaxDepth tx
  let expandedTx ← expandRetractEntities db expandedCalls

  -- Schema validation (if schema is configured)
  if let some config := db.schemaConfig then
    match SchemaValidation.validateTransaction config db.indexes expandedTx with
    | .error schemaErr => throw (.schemaViolation (toString schemaErr))
    | .ok () => pure ()

  let newTxId := db.basisT.next

  -- Convert transaction operations to datoms
  let mut datoms : Array Datom := #[]
  let mut indexes := db.indexes
  let mut historyIndexes := db.historyIndexes
  let mut currentFacts := db.currentFacts

  for op in expandedTx do
    match op with
    | .add entity attr value =>
      let datom := Datom.assert entity attr value newTxId
      datoms := datoms.push datom
      historyIndexes := historyIndexes.insertDatom datom
      let key := FactKey.of entity attr value
      if let some prev := currentFacts[key]? then
        indexes := indexes.removeDatom prev
      indexes := indexes.insertDatom datom
      currentFacts := currentFacts.insert key datom

    | .retract entity attr value =>
      -- Validate that the fact exists in the pre-transaction state
      if !isFactAsserted currentFacts entity attr value then
        throw (.factNotFound entity attr value)
      let datom := Datom.retract entity attr value newTxId
      datoms := datoms.push datom
      historyIndexes := historyIndexes.insertDatom datom
      let key := FactKey.of entity attr value
      if let some prev := currentFacts[key]? then
        indexes := indexes.removeDatom prev
      currentFacts := currentFacts.erase key
    | .retractEntity _ =>
      -- Should have been expanded away
      pure ()
    | .call _ _ =>
      -- Should have been expanded away
      pure ()

  let db' : Db :=
    { db with
      basisT := newTxId
      indexes := indexes
      historyIndexes := historyIndexes
      currentFacts := currentFacts }

  let report : TxReport :=
    { txId := newTxId
    , txData := datoms
    , txInstant := instant }

  return (db', report)

/-- Process a transaction with the default tx function registry. -/
def transact (db : Db) (tx : Transaction) (instant : Nat := 0) : Except TxError (Db × TxReport) :=
  db.transactWith TxFunctions.defaultRegistry tx instant

-- ============================================================
-- Entity-based queries (use EAVT index)
-- ============================================================

/-- Get all datoms for an entity. -/
def entity (db : Db) (e : EntityId) : DatomSeq :=
  db.indexes.datomsForEntity e

/-- Filter datoms to get only visible (not retracted) values.
    For each (entity, attr, value), check if the most recent datom is an assertion.
    Returns values sorted by their latest txId descending (most recent first).

    Implementation: O(n) using HashMap for grouping, then O(m log m) sort. -/
private def filterVisible (datoms : List Datom) : List Value :=
  let visible := Util.filterVisibleDatoms datoms
  let sorted := visible.toArray.qsort (fun a b => a.tx.id > b.tx.id)
  sorted.toList.map (·.value)

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
def datomsWithAttr (db : Db) (a : Attribute) : DatomSeq :=
  db.indexes.datomsForAttr a

/-- Get all entities that have a specific attribute. -/
def entitiesWithAttr (db : Db) (a : Attribute) : List EntityId :=
  db.indexes.entitiesWithAttr a

-- ============================================================
-- Value-based queries (use AVET index)
-- ============================================================

/-- Find entities where attribute equals a specific value. -/
def entitiesWithAttrValue (db : Db) (a : Attribute) (v : Value) : List EntityId :=
  db.indexes.entitiesWithAttrValue a v

/-- Find a single entity by unique attribute value. -/
def entityWithAttrValue (db : Db) (a : Attribute) (v : Value) : Option EntityId :=
  db.indexes.entityWithAttrValue a v

@[deprecated "use entitiesWithAttrValue" (since := "2026-02-04")]
def findByAttrValue (db : Db) (a : Attribute) (v : Value) : List EntityId :=
  db.entitiesWithAttrValue a v

@[deprecated "use entityWithAttrValue" (since := "2026-02-04")]
def findOneByAttrValue (db : Db) (a : Attribute) (v : Value) : Option EntityId :=
  db.entityWithAttrValue a v

-- ============================================================
-- Reverse reference queries (use VAET index)
-- ============================================================

/-- Get all entities that reference a specific entity. -/
def referencingEntities (db : Db) (target : EntityId) : List EntityId :=
  db.indexes.entitiesReferencing target

/-- Get all datoms that reference a specific entity. -/
def referencingDatoms (db : Db) (target : EntityId) : DatomSeq :=
  db.indexes.datomsReferencingEntity target

/-- Get entities referencing target via a specific attribute. -/
def referencingViaAttr (db : Db) (target : EntityId) (a : Attribute) : List EntityId :=
  db.indexes.entitiesReferencingViaAttr target a

-- ============================================================
-- General queries
-- ============================================================

/-- Get all datoms in the database. -/
def datoms (db : Db) : DatomSeq :=
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
