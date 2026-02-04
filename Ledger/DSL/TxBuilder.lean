/-
  Ledger.DSL.TxBuilder

  Fluent builder API for constructing transactions.
  Provides a more ergonomic way to build and execute transactions.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Tx.Types
import Ledger.Tx.Functions
import Ledger.Db.Database
import Ledger.Db.Connection

namespace Ledger

namespace DSL

/-- Transaction builder for fluent construction. -/
structure TxBuilder where
  ops : List TxOp := []
  deriving Repr, Inhabited

namespace TxBuilder

/-- Create a new transaction builder. -/
def new : TxBuilder := {}

/-- Add an assertion (add fact). -/
def add (tb : TxBuilder) (e : EntityId) (attr : String) (v : Value) : TxBuilder :=
  { tb with ops := tb.ops ++ [.add e (Attribute.mk attr) v] }

/-- Add a string value. -/
def addStr (tb : TxBuilder) (e : EntityId) (attr : String) (v : String) : TxBuilder :=
  tb.add e attr (.string v)

/-- Add an int value. -/
def addInt (tb : TxBuilder) (e : EntityId) (attr : String) (v : Int) : TxBuilder :=
  tb.add e attr (.int v)

/-- Add a bool value. -/
def addBool (tb : TxBuilder) (e : EntityId) (attr : String) (v : Bool) : TxBuilder :=
  tb.add e attr (.bool v)

/-- Add a reference. -/
def addRef (tb : TxBuilder) (e : EntityId) (attr : String) (ref : EntityId) : TxBuilder :=
  tb.add e attr (.ref ref)

/-- Add a float value. -/
def addFloat (tb : TxBuilder) (e : EntityId) (attr : String) (v : Float) : TxBuilder :=
  tb.add e attr (.float v)

/-- Add a keyword value. -/
def addKeyword (tb : TxBuilder) (e : EntityId) (attr : String) (v : String) : TxBuilder :=
  tb.add e attr (.keyword v)

/-- Add a retraction. -/
def retract (tb : TxBuilder) (e : EntityId) (attr : String) (v : Value) : TxBuilder :=
  { tb with ops := tb.ops ++ [.retract e (Attribute.mk attr) v] }

/-- Retract a string value. -/
def retractStr (tb : TxBuilder) (e : EntityId) (attr : String) (v : String) : TxBuilder :=
  tb.retract e attr (.string v)

/-- Retract an int value. -/
def retractInt (tb : TxBuilder) (e : EntityId) (attr : String) (v : Int) : TxBuilder :=
  tb.retract e attr (.int v)

/-- Retract a reference. -/
def retractRef (tb : TxBuilder) (e : EntityId) (attr : String) (ref : EntityId) : TxBuilder :=
  tb.retract e attr (.ref ref)

/-- Retract an entire entity (cascading component delete). -/
def retractEntity (tb : TxBuilder) (e : EntityId) : TxBuilder :=
  { tb with ops := tb.ops ++ [.retractEntity (.id e)] }

/-- Retract an entity by lookup ref (attribute + value). -/
def retractEntityLookup (tb : TxBuilder) (attr : String) (v : Value) : TxBuilder :=
  { tb with ops := tb.ops ++ [.retractEntity (.lookup (Attribute.mk attr) v)] }

/-- Retract an entity by lookup ref (string value). -/
def retractEntityLookupStr (tb : TxBuilder) (attr : String) (v : String) : TxBuilder :=
  tb.retractEntityLookup attr (.string v)

/-- Retract an entity by lookup ref (int value). -/
def retractEntityLookupInt (tb : TxBuilder) (attr : String) (v : Int) : TxBuilder :=
  tb.retractEntityLookup attr (.int v)

/-- Call a transaction function by name. -/
def call (tb : TxBuilder) (fn : String) (args : List Value) : TxBuilder :=
  { tb with ops := tb.ops ++ [.call fn args] }

/-- Build the transaction. -/
def build (tb : TxBuilder) : Transaction := tb.ops

/-- Execute the transaction on a database. -/
def run (tb : TxBuilder) (db : Db) : Except TxError (Db × TxReport) :=
  db.transact tb.ops

/-- Execute the transaction on a database with a custom function registry. -/
def runWith (tb : TxBuilder) (db : Db) (registry : TxFuncRegistry) :
    Except TxError (Db × TxReport) :=
  db.transactWith registry tb.ops

/-- Execute the transaction on a connection. -/
def runOn (tb : TxBuilder) (conn : Connection) : Except TxError (Connection × TxReport) :=
  conn.transact tb.ops

/-- Execute the transaction on a connection with a custom function registry. -/
def runOnWith (tb : TxBuilder) (conn : Connection) (registry : TxFuncRegistry) :
    Except TxError (Connection × TxReport) :=
  conn.transactWith registry tb.ops

end TxBuilder

/-- Shorthand for creating a transaction builder. -/
def tx : TxBuilder := TxBuilder.new

/-- Entity builder for creating entities with multiple attributes. -/
structure EntityBuilder where
  entity : EntityId
  txBuilder : TxBuilder
  deriving Repr, Inhabited

namespace EntityBuilder

/-- Set a string attribute. -/
def str (eb : EntityBuilder) (attr : String) (v : String) : EntityBuilder :=
  { eb with txBuilder := eb.txBuilder.addStr eb.entity attr v }

/-- Set an int attribute. -/
def int (eb : EntityBuilder) (attr : String) (v : Int) : EntityBuilder :=
  { eb with txBuilder := eb.txBuilder.addInt eb.entity attr v }

/-- Set a bool attribute. -/
def bool (eb : EntityBuilder) (attr : String) (v : Bool) : EntityBuilder :=
  { eb with txBuilder := eb.txBuilder.addBool eb.entity attr v }

/-- Set a reference attribute. -/
def ref (eb : EntityBuilder) (attr : String) (ref : EntityId) : EntityBuilder :=
  { eb with txBuilder := eb.txBuilder.addRef eb.entity attr ref }

/-- Set a float attribute. -/
def float (eb : EntityBuilder) (attr : String) (v : Float) : EntityBuilder :=
  { eb with txBuilder := eb.txBuilder.addFloat eb.entity attr v }

/-- Set a keyword attribute. -/
def keyword (eb : EntityBuilder) (attr : String) (v : String) : EntityBuilder :=
  { eb with txBuilder := eb.txBuilder.addKeyword eb.entity attr v }

/-- Build the transaction from this entity builder. -/
def build (eb : EntityBuilder) : Transaction := eb.txBuilder.build

/-- Get the transaction builder to add more entities. -/
def done (eb : EntityBuilder) : TxBuilder := eb.txBuilder

end EntityBuilder

/-- Start building an entity with a transaction builder. -/
def TxBuilder.entity (tb : TxBuilder) (e : EntityId) : EntityBuilder :=
  { entity := e, txBuilder := tb }

/-- Convenience: create a new entity and add attributes in one go. -/
def withNewEntity (db : Db) (f : EntityId → TxBuilder → TxBuilder) :
    Except TxError (Db × EntityId × TxReport) := do
  let (e, db) := db.allocEntityId
  let tb := f e TxBuilder.new
  match db.transact tb.ops with
  | .ok (db', report) => return (db', e, report)
  | .error err => throw err

end DSL

end Ledger
