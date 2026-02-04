/-
  Ledger.Tests.TxFunctions - Transaction function tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.TxFunctions

open Crucible
open Ledger

testSuite "Transaction Functions"

private def seedDbWithInt (v : Int) : Db × EntityId := Id.run do
  let db := Db.empty
  let (e, db) := db.allocEntityId
  let tx : Transaction := [
    .add e (Attribute.mk ":counter/value") (Value.int v)
  ]
  let .ok (db, _) := db.transact tx | panic! "Seed tx failed"
  return (db, e)

test "Custom tx function adds value" := do
  let db := Db.empty
  let (e, db) := db.allocEntityId
  let addName : TxFunction := fun _ args => do
    match args with
    | [Value.ref eid, Value.string name] =>
      return [.add eid (Attribute.mk ":person/name") (Value.string name)]
    | _ => throw (.custom "add-name: expected [ref name]")
  let registry :=
    TxFuncRegistry.empty
      |>.register ":test/add-name" addName
  let tx : Transaction := [
    .call ":test/add-name" [.ref e, .string "Alice"]
  ]
  match db.transactWith registry tx with
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"
  | .ok (db', _) =>
    db'.getOne e (Attribute.mk ":person/name") ≡ some (.string "Alice")

test "Nested tx function expansion" := do
  let db := Db.empty
  let (e, db) := db.allocEntityId
  let addName : TxFunction := fun _ args => do
    match args with
    | [Value.ref eid, Value.string name] =>
      return [.add eid (Attribute.mk ":person/name") (Value.string name)]
    | _ => throw (.custom "add-name: expected [ref name]")
  let wrap : TxFunction := fun _ args => do
    return [.call ":test/add-name" args]
  let registry :=
    TxFuncRegistry.empty
      |>.register ":test/add-name" addName
      |>.register ":test/wrap" wrap
  let tx : Transaction := [
    .call ":test/wrap" [.ref e, .string "Bob"]
  ]
  match db.transactWith registry tx with
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"
  | .ok (db', _) =>
    db'.getOne e (Attribute.mk ":person/name") ≡ some (.string "Bob")

test "Unknown tx function errors" := do
  let db := Db.empty
  let tx : Transaction := [
    .call ":test/missing" []
  ]
  match db.transactWith TxFuncRegistry.empty tx with
  | .error err => toString err ≡ "Unknown transaction function: :test/missing"
  | .ok _ => throw <| IO.userError "Expected tx function error"

test "Unknown tx function is TxError.custom" := do
  let db := Db.empty
  let tx : Transaction := [
    .call ":test/missing" []
  ]
  match db.transactWith TxFuncRegistry.empty tx with
  | .error (.custom msg) => msg ≡ "Unknown transaction function: :test/missing"
  | .error _ => throw <| IO.userError "Expected TxError.custom"
  | .ok _ => throw <| IO.userError "Expected tx function error"

test "Tx function expansion depth limit" := do
  let db := Db.empty
  let loopFn : TxFunction := fun _ _ => do
    return [.call ":test/loop" []]
  let registry :=
    TxFuncRegistry.empty
      |>.register ":test/loop" loopFn
  let tx : Transaction := [
    .call ":test/loop" []
  ]
  match db.transactWith registry tx with
  | .error err => toString err ≡ "transaction function expansion exceeded max depth"
  | .ok _ => throw <| IO.userError "Expected depth error"

test "Built-in inc increments value" := do
  let (db, e) := seedDbWithInt 10
  let tx : Transaction := [
    .call ":db.fn/inc" [.ref e, .keyword ":counter/value", .int 5]
  ]
  match db.transact tx with
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"
  | .ok (db', _) =>
    db'.getOne e (Attribute.mk ":counter/value") ≡ some (.int 15)

test "Built-in inc errors on missing attr" := do
  let db := Db.empty
  let (e, db) := db.allocEntityId
  let tx : Transaction := [
    .call ":db.fn/inc" [.ref e, .keyword ":counter/value", .int 1]
  ]
  match db.transact tx with
  | .error err => toString err ≡ "inc: attribute not present"
  | .ok _ => throw <| IO.userError "Expected inc error"

test "Built-in inc errors on non-int value" := do
  let db := Db.empty
  let (e, db) := db.allocEntityId
  let tx1 : Transaction := [
    .add e (Attribute.mk ":counter/value") (Value.string "ten")
  ]
  let .ok (db, _) := db.transact tx1 | panic! "Seed tx failed"
  let tx2 : Transaction := [
    .call ":db.fn/inc" [.ref e, .keyword ":counter/value", .int 1]
  ]
  match db.transact tx2 with
  | .error err => toString err ≡ "inc: expected int value"
  | .ok _ => throw <| IO.userError "Expected inc type error"

test "Built-in cas success" := do
  let db := Db.empty
  let (e, db) := db.allocEntityId
  let tx1 : Transaction := [
    .add e (Attribute.mk ":person/status") (Value.string "new")
  ]
  let .ok (db, _) := db.transact tx1 | panic! "Seed tx failed"
  let tx2 : Transaction := [
    .call ":db.fn/cas" [.ref e, .keyword ":person/status", .string "new", .string "active"]
  ]
  match db.transact tx2 with
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"
  | .ok (db', _) =>
    db'.getOne e (Attribute.mk ":person/status") ≡ some (.string "active")

test "Built-in cas compare failed" := do
  let db := Db.empty
  let (e, db) := db.allocEntityId
  let tx1 : Transaction := [
    .add e (Attribute.mk ":person/status") (Value.string "new")
  ]
  let .ok (db, _) := db.transact tx1 | panic! "Seed tx failed"
  let tx2 : Transaction := [
    .call ":db.fn/cas" [.ref e, .keyword ":person/status", .string "old", .string "active"]
  ]
  match db.transact tx2 with
  | .error err => toString err ≡ "cas: compare failed"
  | .ok _ => throw <| IO.userError "Expected cas compare error"

end Ledger.Tests.TxFunctions
