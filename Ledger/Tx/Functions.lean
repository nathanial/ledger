/-
  Ledger.Tx.Functions

  Transaction function registry and expansion.
-/

import Std.Data.HashMap
import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Tx.Types

namespace Ledger

/-- Read-only view of database operations available to tx functions. -/
structure DbView where
  getOne : EntityId → Attribute → Option Value
  get : EntityId → Attribute → List Value
  entity : EntityId → List Datom
  findByAttrValue : Attribute → Value → List EntityId
  findOneByAttrValue : Attribute → Value → Option EntityId

/-- Context passed to transaction functions. -/
structure TxFuncContext where
  db : DbView
  tx : Transaction

abbrev TxFunction := TxFuncContext → List Value → Except TxError Transaction

structure TxFuncRegistry where
  fns : Std.HashMap String TxFunction
  deriving Inhabited

namespace TxFuncRegistry

def empty : TxFuncRegistry := ⟨{}⟩

def register (name : String) (fn : TxFunction) (r : TxFuncRegistry) : TxFuncRegistry :=
  ⟨r.fns.insert name fn⟩

def get? (name : String) (r : TxFuncRegistry) : Option TxFunction :=
  r.fns[name]?

end TxFuncRegistry

namespace TxFunctions

private def valueToEntity? : Value → Option EntityId
  | .ref e => some e
  | _ => none

private def valueToAttr? : Value → Option Attribute
  | .keyword k => some (Attribute.mk k)
  | .string s => some (Attribute.mk s)
  | _ => none

private def asInt? : Value → Option Int
  | .int n => some n
  | _ => none

private def casFn : TxFunction := fun ctx args => do
  match args with
  | [eVal, aVal, oldVal, newVal] =>
    let some e := valueToEntity? eVal
      | throw (.custom "cas: expected entity ref as first arg")
    let some a := valueToAttr? aVal
      | throw (.custom "cas: expected attribute keyword/string as second arg")
    match ctx.db.getOne e a with
    | none => throw (.custom "cas: attribute not present")
    | some current =>
      if current == oldVal then
        return [.retract e a oldVal, .add e a newVal]
      else
        throw (.custom "cas: compare failed")
  | _ => throw (.custom "cas: expected 4 args [entity attr old new]")

private def incFn : TxFunction := fun ctx args => do
  match args with
  | [eVal, aVal, deltaVal] =>
    let some e := valueToEntity? eVal
      | throw (.custom "inc: expected entity ref as first arg")
    let some a := valueToAttr? aVal
      | throw (.custom "inc: expected attribute keyword/string as second arg")
    let some delta := asInt? deltaVal
      | throw (.custom "inc: expected int delta")
    match ctx.db.getOne e a with
    | some (.int current) =>
      let newVal := Value.int (current + delta)
      return [.retract e a (.int current), .add e a newVal]
    | some _ => throw (.custom "inc: expected int value")
    | none => throw (.custom "inc: attribute not present")
  | _ => throw (.custom "inc: expected 3 args [entity attr delta]")

def defaultRegistry : TxFuncRegistry :=
  TxFuncRegistry.empty
    |>.register ":db.fn/cas" casFn
    |>.register ":db.fn/inc" incFn

def defaultMaxDepth : Nat := 8

end TxFunctions

/-- Expand transaction functions (call ops) into concrete operations. -/
def expandTxFunctions (ctx : TxFuncContext) (registry : TxFuncRegistry) (maxDepth : Nat)
    (tx : Transaction) : Except TxError Transaction := do
  let rec go (depth : Nat) (tx : Transaction) : Except TxError Transaction := do
    match depth with
    | 0 => throw (.custom "transaction function expansion exceeded max depth")
    | depth + 1 =>
      let mut expanded : Transaction := []
      for op in tx do
        match op with
        | .call fn args =>
          match registry.get? fn with
          | none => throw (.custom s!"Unknown transaction function: {fn}")
          | some f =>
            let nested ← f ctx args
            let more ← go depth nested
            expanded := expanded ++ more
        | _ => expanded := expanded ++ [op]
      return expanded
  go maxDepth tx

end Ledger
