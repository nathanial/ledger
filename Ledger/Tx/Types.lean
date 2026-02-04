/-
  Ledger.Tx.Types

  Transaction types for database mutations.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom

namespace Ledger

/-- Reference to an entity, either by ID or lookup ref. -/
inductive EntityRef where
  | id (entity : EntityId)
  | lookup (attr : Attribute) (value : Value)
  deriving Repr, Inhabited

namespace EntityRef

def ofId (entity : EntityId) : EntityRef := .id entity
def ofLookup (attr : Attribute) (value : Value) : EntityRef := .lookup attr value

instance : ToString EntityRef where
  toString ref := match ref with
    | .id e => toString e
    | .lookup a v => s!"[{a} {v}]"

end EntityRef

/-- A single operation in a transaction. -/
inductive TxOp where
  /-- Assert a fact: the entity has this attribute with this value. -/
  | add (entity : EntityId) (attr : Attribute) (value : Value)
  /-- Retract a fact: remove this specific attribute-value from the entity. -/
  | retract (entity : EntityId) (attr : Attribute) (value : Value)
  /-- Retract an entity (and its references), optionally via lookup ref. -/
  | retractEntity (entity : EntityRef)
  /-- Call a transaction function (expanded before application). -/
  | call (fn : String) (args : List Value)
  deriving Repr, Inhabited

namespace TxOp

instance : ToString TxOp where
  toString op := match op with
    | .add e a v => s!"[:db/add {e} {a} {v}]"
    | .retract e a v => s!"[:db/retract {e} {a} {v}]"
    | .retractEntity e => s!"[:db/retractEntity {e}]"
    | .call fn args => s!"[:db/call {fn} {args}]"

end TxOp


/-- A transaction is a list of operations to apply atomically. -/
abbrev Transaction := List TxOp


/-- Error that can occur during transaction processing. -/
inductive TxError where
  /-- Attempted to retract a fact that doesn't exist. -/
  | factNotFound (entity : EntityId) (attr : Attribute) (value : Value)
  /-- Generic error with message. -/
  | custom (msg : String)
  /-- Schema validation failed. -/
  | schemaViolation (msg : String)
  deriving Repr, Inhabited

namespace TxError

instance : ToString TxError where
  toString err := match err with
    | .factNotFound e a v => s!"Fact not found: [{e} {a} {v}]"
    | .custom msg => msg
    | .schemaViolation msg => s!"Schema violation: {msg}"

end TxError


/-- Result of a successful transaction. -/
structure TxReport where
  /-- The transaction ID assigned to this transaction. -/
  txId : TxId
  /-- All datoms created by this transaction. -/
  txData : Array Datom
  /-- Unix timestamp when the transaction was processed. -/
  txInstant : Nat
  deriving Repr, Inhabited

end Ledger
