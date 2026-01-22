/-
  Ledger.Tests.Pull - Pull API tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.Pull

open Crucible
open Ledger

testSuite "Pull API"

test "Pull: single attribute" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result1 := Pull.pullOne db alice ":person/name"
  result1 ≡ some (Value.string "Alice")

test "Pull: multiple attrs size" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result2 := Pull.pullAttrs db alice [":person/name", ":person/age"]
  result2.size ≡ 2

test "Pull: wildcard has name" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result3 := Pull.pull db alice [.wildcard]
  ensure (result3.get? (Attribute.mk ":person/name")).isSome "Should have name"

test "Pull: nested has friend" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add alice (Attribute.mk ":person/friend") (Value.ref bob)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result4 := Pull.pull db alice [
    .attr (Attribute.mk ":person/name"),
    .nested (Attribute.mk ":person/friend") [
      .attr (Attribute.mk ":person/name")
    ]
  ]
  ensure (result4.get? (Attribute.mk ":person/friend")).isSome "Should have friend"

test "Pull: nested friend name" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add alice (Attribute.mk ":person/friend") (Value.ref bob)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result4 := Pull.pull db alice [
    .attr (Attribute.mk ":person/name"),
    .nested (Attribute.mk ":person/friend") [
      .attr (Attribute.mk ":person/name")
    ]
  ]
  match result4.get? (Attribute.mk ":person/friend") with
  | some (.entity data) =>
    let friendName := data.find? fun (a, _) => a == Attribute.mk ":person/name"
    match friendName with
    | some (_, .scalar (.string name)) => name ≡ "Bob"
    | _ => throw <| IO.userError "Expected friend name"
  | _ => throw <| IO.userError "Expected entity"

test "Pull: reverse worker count" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (project, db) := db.allocEntityId
  let tx : Transaction := [
    .add project (Attribute.mk ":project/name") (Value.string "Ledger"),
    .add alice (Attribute.mk ":person/works-on") (Value.ref project),
    .add bob (Attribute.mk ":person/works-on") (Value.ref project)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result5 := Pull.pull db project [
    .attr (Attribute.mk ":project/name"),
    .reverse (Attribute.mk ":person/works-on") [
      .attr (Attribute.mk ":person/name")
    ]
  ]
  match result5.get? (Attribute.mk ":person/works-on") with
  | some (.many workers) => workers.length ≡ 2
  | _ => throw <| IO.userError "Expected many workers"

test "Pull: default value" := do
  let db := Db.empty
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add charlie (Attribute.mk ":person/name") (Value.string "Charlie")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result6 := Pull.pull db charlie [
    .attr (Attribute.mk ":person/name"),
    .withDefault (Attribute.mk ":person/email") "no-email@example.com"
  ]
  match result6.get? (Attribute.mk ":person/email") with
  | some (.scalar (.string email)) => email ≡ "no-email@example.com"
  | _ => throw <| IO.userError "Expected default email"

test "Pull: many entities count" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add charlie (Attribute.mk ":person/name") (Value.string "Charlie")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let results7 := Pull.pullMany db [alice, bob, charlie] [.attr (Attribute.mk ":person/name")]
  results7.length ≡ 3

/-! ## Pull with Multiple Values (Bug Reproduction) -/

test "Pull: multiple values returns many" := do
  -- When an entity has multiple values for an attribute, pull returns .many
  let db := Db.empty
  let (card, db) := db.allocEntityId
  let tx1 : Transaction := [.add card (Attribute.mk ":card/title") (Value.string "Magic")]
  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [.add card (Attribute.mk ":card/title") (Value.string "Magic Gun")]
  let .ok (db, _) := db.transact tx2 | throw <| IO.userError "Tx2 failed"
  let result := Pull.pull db card [.attr (Attribute.mk ":card/title")]
  match result.get? (Attribute.mk ":card/title") with
  | some (.many vs) => vs.length ≡ 2
  | some (.scalar _) => throw <| IO.userError "Expected .many, got .scalar (only one value)"
  | _ => throw <| IO.userError "Expected title in result"

test "Pull: multiple values - first should be most recent" := do
  -- BUG TEST: When there are multiple values, the first one in .many should be
  -- the most recently asserted value (highest txId). This test may fail if the
  -- order is arbitrary (HashMap iteration order).
  let db := Db.empty
  let (card, db) := db.allocEntityId
  let tx1 : Transaction := [.add card (Attribute.mk ":card/title") (Value.string "Magic")]
  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [.add card (Attribute.mk ":card/title") (Value.string "Magic Gun")]
  let .ok (db, _) := db.transact tx2 | throw <| IO.userError "Tx2 failed"
  let result := Pull.pull db card [.attr (Attribute.mk ":card/title")]
  match result.get? (Attribute.mk ":card/title") with
  | some (.many ((PullValue.scalar (.string firstVal)) :: _)) =>
    -- The first value should be "Magic Gun" (most recent)
    -- If this fails, it means the order is wrong!
    firstVal ≡ "Magic Gun"
  | some (.scalar (.string v)) =>
    -- If only one value returned, verify it's the most recent
    v ≡ "Magic Gun"
  | other => throw <| IO.userError s!"Expected string value, got {repr other}"

test "Pull: pullOne with multiple values" := do
  -- pullOne should return the most recent value via db.getOne
  -- (But currently Pull uses db.get which returns all values)
  let db := Db.empty
  let (card, db) := db.allocEntityId
  let tx1 : Transaction := [.add card (Attribute.mk ":card/title") (Value.string "Magic")]
  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [.add card (Attribute.mk ":card/title") (Value.string "Magic Gun")]
  let .ok (db, _) := db.transact tx2 | throw <| IO.userError "Tx2 failed"
  -- pullOne uses pull internally, so it may have the same bug
  let titleOpt := Pull.pullOne db card ":card/title"
  match titleOpt with
  | some (.string title) => title ≡ "Magic Gun"
  | _ => throw <| IO.userError "Expected string value"

end Ledger.Tests.Pull
