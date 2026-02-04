/-
  Ledger.DSL.Combinators

  High-level combinators for common database operations.
  Provides convenient shortcuts for typical use cases.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Tx.Types
import Ledger.Db.Database
import Ledger.Db.Connection
import Ledger.Query.AST
import Ledger.Query.Executor
import Ledger.DSL.QueryBuilder
import Ledger.Pull.Executor

namespace Ledger

namespace DSL

-- ============================================================
-- Entity Operations
-- ============================================================

/-- Create a new entity with a single attribute. -/
def createEntity (db : Db) (attr : String) (v : Value) : Except TxError (Db × EntityId) := do
  let (e, db) := db.allocEntityId
  let tx : Transaction := [.add e (Attribute.mk attr) v]
  let (db', _) ← db.transact tx
  return (db', e)

/-- Create a new entity with multiple attributes. -/
def createEntityWith (db : Db) (attrs : List (String × Value)) : Except TxError (Db × EntityId) := do
  let (e, db) := db.allocEntityId
  let tx : Transaction := attrs.map fun (attr, v) => .add e (Attribute.mk attr) v
  let (db', _) ← db.transact tx
  return (db', e)

/-- Update an entity's attribute (retract old, add new). -/
def updateAttr (db : Db) (e : EntityId) (attr : String) (oldVal newVal : Value)
    : Except TxError Db := do
  let tx : Transaction := [
    .retract e (Attribute.mk attr) oldVal,
    .add e (Attribute.mk attr) newVal
  ]
  let (db', _) ← db.transact tx
  return db'

/-- Set an attribute (add without retraction - for new values). -/
def setAttr (db : Db) (e : EntityId) (attr : String) (v : Value) : Except TxError Db := do
  let tx : Transaction := [.add e (Attribute.mk attr) v]
  let (db', _) ← db.transact tx
  return db'

/-- Remove an attribute value. -/
def removeAttr (db : Db) (e : EntityId) (attr : String) (v : Value) : Except TxError Db := do
  let tx : Transaction := [.retract e (Attribute.mk attr) v]
  let (db', _) ← db.transact tx
  return db'

-- ============================================================
-- Query Shortcuts
-- ============================================================

@[deprecated "use entitiesWithAttr" (since := "2026-02-04")]
def allWith (db : Db) (attr : String) : List EntityId :=
  entitiesWithAttr db attr

/-- Find entities where attr = string value. -/
@[deprecated "use entitiesWithAttrValueStr" (since := "2026-02-04")]
def findByStr (db : Db) (attr : String) (v : String) : List EntityId :=
  entitiesWithAttrValueStr db attr v

/-- Find entities where attr = int value. -/
@[deprecated "use entitiesWithAttrValueInt" (since := "2026-02-04")]
def findByInt (db : Db) (attr : String) (v : Int) : List EntityId :=
  entitiesWithAttrValueInt db attr v

/-- Find single entity where attr = string (unique lookup). -/
@[deprecated "use entityWithAttrValueStr" (since := "2026-02-04")]
def findOneByStr (db : Db) (attr : String) (v : String) : Option EntityId :=
  entityWithAttrValueStr db attr v

/-- Get string value of an attribute. -/
def attrStr (db : Db) (e : EntityId) (attr : String) : Option String :=
  match db.getOne e (Attribute.mk attr) with
  | some (.string s) => some s
  | _ => none

/-- Get int value of an attribute. -/
def attrInt (db : Db) (e : EntityId) (attr : String) : Option Int :=
  match db.getOne e (Attribute.mk attr) with
  | some (.int i) => some i
  | _ => none

/-- Get reference value of an attribute. -/
def attrRef (db : Db) (e : EntityId) (attr : String) : Option EntityId :=
  match db.getOne e (Attribute.mk attr) with
  | some (.ref ref) => some ref
  | _ => none

/-- Get all values for an attribute. -/
def attrAll (db : Db) (e : EntityId) (attr : String) : List Value :=
  db.get e (Attribute.mk attr)

-- ============================================================
-- Reference Traversal
-- ============================================================

/-- Follow a reference attribute to get the target entity. -/
def follow (db : Db) (e : EntityId) (attr : String) : Option EntityId :=
  attrRef db e attr

/-- Follow a reference and get an attribute of the target. -/
def followAndGet (db : Db) (e : EntityId) (refAttr getAttr : String) : Option Value :=
  match follow db e refAttr with
  | some target => db.getOne target (Attribute.mk getAttr)
  | none => none

/-- Get all entities that reference this entity. -/
def referencedBy (db : Db) (e : EntityId) : List EntityId :=
  db.referencingEntities e

/-- Get all entities that reference this entity via a specific attribute. -/
def referencedByVia (db : Db) (e : EntityId) (attr : String) : List EntityId :=
  db.referencingViaAttr e (Attribute.mk attr)

-- ============================================================
-- Connection Operations
-- ============================================================

/-- Create entity on a connection. -/
def createEntityConn (conn : Connection) (attrs : List (String × Value))
    : Except TxError (Connection × EntityId) := do
  let (e, conn) := conn.allocEntityId
  let tx : Transaction := attrs.map fun (attr, v) => .add e (Attribute.mk attr) v
  let (conn', _) ← conn.transact tx
  return (conn', e)

/-- Update attribute on a connection. -/
def updateAttrConn (conn : Connection) (e : EntityId) (attr : String) (oldVal newVal : Value)
    : Except TxError Connection := do
  let tx : Transaction := [
    .retract e (Attribute.mk attr) oldVal,
    .add e (Attribute.mk attr) newVal
  ]
  let (conn', _) ← conn.transact tx
  return conn'

-- ============================================================
-- Batch Operations
-- ============================================================

/-- Create multiple entities with the same schema. -/
def createMany (db : Db) (attrValuesList : List (List (String × Value)))
    : Except TxError (Db × List EntityId) := do
  let (entities, db) := db.allocEntityIds attrValuesList.length
  let allOps := entities.zip attrValuesList |>.flatMap fun (e, attrs) =>
    attrs.map fun (attr, v) => TxOp.add e (Attribute.mk attr) v
  let (db', _) ← db.transact allOps
  return (db', entities)

/-- Apply a function to each entity in a list and collect results. -/
def mapEntities (db : Db) (entities : List EntityId) (f : Db → EntityId → α) : List α :=
  entities.map (f db)

/-- Filter entities by a predicate. -/
def filterEntities (db : Db) (entities : List EntityId) (p : Db → EntityId → Bool) : List EntityId :=
  entities.filter (p db)

end DSL

end Ledger
