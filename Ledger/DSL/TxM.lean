/-
  Ledger.DSL.TxM - Monadic transaction builder

  TxM is a pure monad for building database transactions.
  It accumulates transaction operations while having read access to the database
  for cardinality-one enforcement.

  ## Usage

  ```lean
  let (_, ops) := TxM.build db do
    TxM.addStr eid attr "value"
    TxM.setInt eid attr2 42
  ```
-/
import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Tx.Types
import Ledger.Tx.Functions
import Ledger.Db.Database

namespace Ledger

/-- State for transaction builder: accumulated ops -/
structure TxState where
  ops : Array TxOp := #[]
  deriving Repr, Inhabited

/-- Transaction monad: builds ops while having read access to database snapshot -/
abbrev TxM := StateT TxState (ReaderT Db Id)

namespace TxM

/-! ## Core Operations -/

/-- Add an assertion (add fact) -/
def add (e : EntityId) (attr : Attribute) (v : Value) : TxM Unit :=
  modify fun s => { s with ops := s.ops.push (.add e attr v) }

/-- Add a retraction (remove fact) -/
def retract (e : EntityId) (attr : Attribute) (v : Value) : TxM Unit :=
  modify fun s => { s with ops := s.ops.push (.retract e attr v) }

/-- Retract an entire entity (cascading component delete) -/
def retractEntity (e : EntityId) : TxM Unit :=
  modify fun s => { s with ops := s.ops.push (.retractEntity (.id e)) }

/-- Retract an entity by lookup ref (attribute + value) -/
def retractEntityLookup (attr : Attribute) (v : Value) : TxM Unit :=
  modify fun s => { s with ops := s.ops.push (.retractEntity (.lookup attr v)) }

/-- Call a transaction function by name. -/
def call (fn : String) (args : List Value) : TxM Unit :=
  modify fun s => { s with ops := s.ops.push (.call fn args) }

/-- Get the database snapshot (for reading current values) -/
def getDb : TxM Db := read

/-- Get the accumulated ops -/
def getOps : TxM (Array TxOp) := do
  let s ← get
  pure s.ops

/-! ## Typed Helpers -/

/-- Add a string value -/
def addStr (e : EntityId) (attr : Attribute) (v : String) : TxM Unit :=
  add e attr (.string v)

/-- Add an int value -/
def addInt (e : EntityId) (attr : Attribute) (v : Int) : TxM Unit :=
  add e attr (.int v)

/-- Add a nat value (as int) -/
def addNat (e : EntityId) (attr : Attribute) (v : Nat) : TxM Unit :=
  add e attr (.int (Int.ofNat v))

/-- Add a bool value -/
def addBool (e : EntityId) (attr : Attribute) (v : Bool) : TxM Unit :=
  add e attr (.bool v)

/-- Add a reference (EntityId) -/
def addRef (e : EntityId) (attr : Attribute) (ref : EntityId) : TxM Unit :=
  add e attr (.ref ref)

/-- Add a float value -/
def addFloat (e : EntityId) (attr : Attribute) (v : Float) : TxM Unit :=
  add e attr (.float v)

/-- Retract a string value -/
def retractStr (e : EntityId) (attr : Attribute) (v : String) : TxM Unit :=
  retract e attr (.string v)

/-- Retract an int value -/
def retractInt (e : EntityId) (attr : Attribute) (v : Int) : TxM Unit :=
  retract e attr (.int v)

/-- Retract a bool value -/
def retractBool (e : EntityId) (attr : Attribute) (v : Bool) : TxM Unit :=
  retract e attr (.bool v)

/-- Retract a reference -/
def retractRef (e : EntityId) (attr : Attribute) (ref : EntityId) : TxM Unit :=
  retract e attr (.ref ref)

/-- Retract an entity by lookup ref (string value). -/
def retractEntityLookupStr (attr : Attribute) (v : String) : TxM Unit :=
  retractEntityLookup attr (.string v)

/-- Retract an entity by lookup ref (int value). -/
def retractEntityLookupInt (attr : Attribute) (v : Int) : TxM Unit :=
  retractEntityLookup attr (.int v)

/-! ## Cardinality-One Helpers -/

/-- Set a string value with cardinality-one enforcement (retracts old value if different) -/
def setStr (e : EntityId) (attr : Attribute) (v : String) : TxM Unit := do
  let db ← getDb
  let newVal := Value.string v
  match db.getOne e attr with
  | some oldVal =>
    if oldVal != newVal then do
      retract e attr oldVal
      add e attr newVal
  | none => add e attr newVal

/-- Set an int value with cardinality-one enforcement -/
def setInt (e : EntityId) (attr : Attribute) (v : Int) : TxM Unit := do
  let db ← getDb
  let newVal := Value.int v
  match db.getOne e attr with
  | some oldVal =>
    if oldVal != newVal then do
      retract e attr oldVal
      add e attr newVal
  | none => add e attr newVal

/-- Set a nat value with cardinality-one enforcement -/
def setNat (e : EntityId) (attr : Attribute) (v : Nat) : TxM Unit :=
  setInt e attr (Int.ofNat v)

/-- Set a bool value with cardinality-one enforcement -/
def setBool (e : EntityId) (attr : Attribute) (v : Bool) : TxM Unit := do
  let db ← getDb
  let newVal := Value.bool v
  match db.getOne e attr with
  | some oldVal =>
    if oldVal != newVal then do
      retract e attr oldVal
      add e attr newVal
  | none => add e attr newVal

/-- Set a reference with cardinality-one enforcement -/
def setRef (e : EntityId) (attr : Attribute) (ref : EntityId) : TxM Unit := do
  let db ← getDb
  let newVal := Value.ref ref
  match db.getOne e attr with
  | some oldVal =>
    if oldVal != newVal then do
      retract e attr oldVal
      add e attr newVal
  | none => add e attr newVal

/-- Set a float value with cardinality-one enforcement -/
def setFloat (e : EntityId) (attr : Attribute) (v : Float) : TxM Unit := do
  let db ← getDb
  let newVal := Value.float v
  match db.getOne e attr with
  | some oldVal =>
    if oldVal != newVal then do
      retract e attr oldVal
      add e attr newVal
  | none => add e attr newVal

/-! ## Entity Operations -/

/-- Retract all values for an attribute (for deletion) -/
def retractAttr (e : EntityId) (attr : Attribute) : TxM Unit := do
  let db ← getDb
  match db.getOne e attr with
  | some v => retract e attr v
  | none => pure ()

/-- Retract all values for a list of attributes -/
def retractAttrs (e : EntityId) (attrs : List Attribute) : TxM Unit := do
  for attr in attrs do
    retractAttr e attr

/-! ## Running -/

/-- Build the transaction ops from a TxM computation -/
def build (m : TxM α) (db : Db) : α × Array TxOp :=
  -- TxM α = StateT TxState (ReaderT Db Id) α
  -- StateT.run returns (α × TxState), ReaderT.run returns that directly
  let (result, state) := m.run {} |>.run db
  (result, state.ops)

/-- Build just the ops list -/
def buildOps (m : TxM α) (db : Db) : List TxOp :=
  (build m db).2.toList

/-- Run a TxM and commit to the database -/
def run (m : TxM α) (db : Db) : Except TxError (α × Db × TxReport) :=
  let (result, ops) := build m db
  if ops.isEmpty then
    .ok (result, db, { txId := TxId.genesis, txData := #[], txInstant := 0 })
  else
    match db.transact ops.toList with
    | .ok (db', report) => .ok (result, db', report)
    | .error e => .error e

/-- Run a TxM and commit with a custom function registry. -/
def runWith (m : TxM α) (db : Db) (registry : TxFuncRegistry) :
    Except TxError (α × Db × TxReport) :=
  let (result, ops) := build m db
  if ops.isEmpty then
    .ok (result, db, { txId := TxId.genesis, txData := #[], txInstant := 0 })
  else
    match db.transactWith registry ops.toList with
    | .ok (db', report) => .ok (result, db', report)
    | .error e => .error e

end TxM

end Ledger
