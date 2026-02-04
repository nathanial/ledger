/-
  Ledger.Tests.Retraction - Retraction, cardinality-one, time travel, and history tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.Retraction

open Crucible
open Ledger

testSuite "Retraction & Time Travel"

/-! ## Retraction Validation Tests -/

test "Retract non-existent fact fails with factNotFound" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  -- Try to retract a fact that was never added
  let tx : Transaction := [
    .retract alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  match conn.transact tx with
  | .ok _ => throw <| IO.userError "Should have failed with factNotFound"
  | .error (.factNotFound e a v) =>
    ensure (e == alice) "Error should reference correct entity"
    ensure (a == Attribute.mk ":person/name") "Error should reference correct attribute"
    ensure (v == Value.string "Alice") "Error should reference correct value"
  | .error err => throw <| IO.userError s!"Wrong error type: {err}"

test "Double retraction fails with factNotFound" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  -- Add a fact
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  -- Retract it
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  -- Try to retract again - should fail
  let tx3 : Transaction := [
    .retract alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  match conn.transact tx3 with
  | .ok _ => throw <| IO.userError "Double retraction should fail"
  | .error (.factNotFound _ _ _) => pure ()
  | .error err => throw <| IO.userError s!"Wrong error type: {err}"

test "Retract wrong value fails with factNotFound" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  -- Add a fact
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  -- Try to retract with wrong value
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/name") (Value.string "Bob")
  ]
  match conn.transact tx2 with
  | .ok _ => throw <| IO.userError "Retract wrong value should fail"
  | .error (.factNotFound _ _ _) => pure ()
  | .error err => throw <| IO.userError s!"Wrong error type: {err}"

test "Valid retraction still works" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Valid retraction should succeed"
  let name := conn.db.getOne alice (Attribute.mk ":person/name")
  ensure name.isNone "Name should be retracted"

/-! ## Retraction Filtering in Value Queries (AVET) Tests -/

test "entitiesWithAttrValue excludes retracted entity" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let (bob, conn) := conn.allocEntityId
  -- Add both entities with same tag
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/tag") (Value.string "active"),
    .add bob (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  -- Retract Alice's tag
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  -- Should only find Bob
  let active := conn.db.entitiesWithAttrValue (Attribute.mk ":person/tag") (Value.string "active")
  active.length ≡ 1

test "entitiesWithAttrValue excludes retracted entity - contains check" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let (bob, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/tag") (Value.string "active"),
    .add bob (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let active := conn.db.entitiesWithAttrValue (Attribute.mk ":person/tag") (Value.string "active")
  ensure (active.contains bob) "Should contain bob"

test "entitiesWithAttrValue excludes retracted entity - alice not present" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let (bob, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/tag") (Value.string "active"),
    .add bob (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let active := conn.db.entitiesWithAttrValue (Attribute.mk ":person/tag") (Value.string "active")
  ensure (!active.contains alice) "Should not contain retracted alice"

test "entityWithAttrValue returns none for retracted" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let found := conn.db.entityWithAttrValue
    (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ensure found.isNone "Retracted email should not be found"

test "Re-assertion after retraction is visible" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  -- Add tag
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  -- Retract tag
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  -- Re-add tag
  let tx3 : Transaction := [
    .add alice (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  -- Should find Alice again
  let active := conn.db.entitiesWithAttrValue (Attribute.mk ":person/tag") (Value.string "active")
  active.length ≡ 1

test "Re-assertion after retraction contains entity" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx3 : Transaction := [
    .add alice (Attribute.mk ":person/tag") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  let active := conn.db.entitiesWithAttrValue (Attribute.mk ":person/tag") (Value.string "active")
  ensure (active.contains alice) "Re-asserted alice should be found"

test "Multiple entities with mixed retractions" := do
  let conn := Connection.create
  let (e1, conn) := conn.allocEntityId
  let (e2, conn) := conn.allocEntityId
  let (e3, conn) := conn.allocEntityId
  let (e4, conn) := conn.allocEntityId
  -- Add all with same owner
  let tx1 : Transaction := [
    .add e1 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .add e2 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .add e3 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .add e4 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  -- Retract e2 and e4
  let tx2 : Transaction := [
    .retract e2 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .retract e4 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  -- Should find only e1 and e3
  let owned := conn.db.entitiesWithAttrValue (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
  owned.length ≡ 2

test "Multiple entities with mixed retractions - correct entities" := do
  let conn := Connection.create
  let (e1, conn) := conn.allocEntityId
  let (e2, conn) := conn.allocEntityId
  let (e3, conn) := conn.allocEntityId
  let (e4, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add e1 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .add e2 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .add e3 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .add e4 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract e2 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .retract e4 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let owned := conn.db.entitiesWithAttrValue (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
  ensure (owned.contains e1 && owned.contains e3) "Should contain e1 and e3"

test "Multiple entities with mixed retractions - excluded entities" := do
  let conn := Connection.create
  let (e1, conn) := conn.allocEntityId
  let (e2, conn) := conn.allocEntityId
  let (e3, conn) := conn.allocEntityId
  let (e4, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add e1 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .add e2 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .add e3 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .add e4 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract e2 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩),
    .retract e4 (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let owned := conn.db.entitiesWithAttrValue (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
  ensure (!owned.contains e2 && !owned.contains e4) "Should not contain e2 and e4"

/-! ## Time Travel Tests -/

test "Current age is 27" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 25)
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/age") (Value.int 25),
    .add alice (Attribute.mk ":person/age") (Value.int 26)
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx3 : Transaction := [
    .retract alice (Attribute.mk ":person/age") (Value.int 26),
    .add alice (Attribute.mk ":person/age") (Value.int 27)
  ]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  let currentAge := conn.current.getOne alice (Attribute.mk ":person/age")
  currentAge ≡ some (Value.int 27)

test "Age at tx1 is 25" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 25)
  ]
  let .ok (conn, report1) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx1Id := report1.txId
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/age") (Value.int 25),
    .add alice (Attribute.mk ":person/age") (Value.int 26)
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let dbAtTx1 := conn.asOf tx1Id
  let ageAtTx1 := dbAtTx1.getOne alice (Attribute.mk ":person/age")
  ageAtTx1 ≡ some (Value.int 25)

test "Age at tx2 is 26" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 25)
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/age") (Value.int 25),
    .add alice (Attribute.mk ":person/age") (Value.int 26)
  ]
  let .ok (conn, report2) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx2Id := report2.txId
  let dbAtTx2 := conn.asOf tx2Id
  let ageAtTx2 := dbAtTx2.getOne alice (Attribute.mk ":person/age")
  ageAtTx2 ≡ some (Value.int 26)

test "Changes since tx1" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 25)
  ]
  let .ok (conn, report1) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx1Id := report1.txId
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/age") (Value.int 25),
    .add alice (Attribute.mk ":person/age") (Value.int 26)
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx3 : Transaction := [
    .retract alice (Attribute.mk ":person/age") (Value.int 26),
    .add alice (Attribute.mk ":person/age") (Value.int 27)
  ]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  let changesSinceTx1 := conn.since tx1Id
  changesSinceTx1.length ≡ 4

test "History has 5 entries" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 25)
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/age") (Value.int 25),
    .add alice (Attribute.mk ":person/age") (Value.int 26)
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx3 : Transaction := [
    .retract alice (Attribute.mk ":person/age") (Value.int 26),
    .add alice (Attribute.mk ":person/age") (Value.int 27)
  ]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  let history := conn.attrHistory alice (Attribute.mk ":person/age")
  history.length ≡ 5

/-! ## Basic Retraction Tests -/

test "Has name after add" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  conn.current.getOne alice (Attribute.mk ":person/name") ≡ some (Value.string "Alice")

test "Has email after add" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  conn.current.getOne alice (Attribute.mk ":person/email") ≡ some (Value.string "alice@example.com")

test "Still has name after email retract" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  conn.current.getOne alice (Attribute.mk ":person/name") ≡ some (Value.string "Alice")

test "Email retracted" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let email := conn.current.getOne alice (Attribute.mk ":person/email")
  ensure email.isNone "Email should be retracted"

/-! ## Cardinality-One (getOne) Tests -/

test "getOne: single assertion returns value" := do
  let conn := Connection.create
  let (e, conn) := conn.allocEntityId
  let tx : Transaction := [
    .add e (Attribute.mk ":item/name") (Value.string "A")
  ]
  let .ok (conn, _) := conn.transact tx | throw <| IO.userError "Tx failed"
  conn.db.getOne e (Attribute.mk ":item/name") ≡ some (Value.string "A")

test "getOne: most recent assertion wins" := do
  let conn := Connection.create
  let (e, conn) := conn.allocEntityId
  let tx1 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "B")]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  conn.db.getOne e (Attribute.mk ":item/name") ≡ some (Value.string "B")

test "getOne: retraction reveals previous value" := do
  let conn := Connection.create
  let (e, conn) := conn.allocEntityId
  -- Assert "A"
  let tx1 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  -- Assert "B"
  let tx2 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "B")]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  -- Retract "B"
  let tx3 : Transaction := [.retract e (Attribute.mk ":item/name") (Value.string "B")]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  -- Should return "A" (the most recent visible value)
  conn.db.getOne e (Attribute.mk ":item/name") ≡ some (Value.string "A")

test "getOne: retract only value returns none" := do
  let conn := Connection.create
  let (e, conn) := conn.allocEntityId
  let tx1 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [.retract e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let result := conn.db.getOne e (Attribute.mk ":item/name")
  ensure result.isNone "Should return none after retracting only value"

test "getOne: new assertion after retraction" := do
  let conn := Connection.create
  let (e, conn) := conn.allocEntityId
  let tx1 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [.retract e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx3 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "B")]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  conn.db.getOne e (Attribute.mk ":item/name") ≡ some (Value.string "B")

test "getOne: retract all values returns none" := do
  let conn := Connection.create
  let (e, conn) := conn.allocEntityId
  let tx1 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "B")]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx3 : Transaction := [.retract e (Attribute.mk ":item/name") (Value.string "B")]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  let tx4 : Transaction := [.retract e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx4 | throw <| IO.userError "Tx4 failed"
  let result := conn.db.getOne e (Attribute.mk ":item/name")
  ensure result.isNone "Should return none after retracting all values"

test "getOne: re-assertion after retraction" := do
  let conn := Connection.create
  let (e, conn) := conn.allocEntityId
  -- Assert "A"
  let tx1 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  -- Retract "A"
  let tx2 : Transaction := [.retract e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  -- Re-assert "A"
  let tx3 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  conn.db.getOne e (Attribute.mk ":item/name") ≡ some (Value.string "A")

test "getOne: multiple values with mixed retractions" := do
  let conn := Connection.create
  let (e, conn) := conn.allocEntityId
  -- Add "A", "B", "C" in sequence
  let tx1 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "A")]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "B")]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx3 : Transaction := [.add e (Attribute.mk ":item/name") (Value.string "C")]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  -- Retract "C" and "B"
  let tx4 : Transaction := [.retract e (Attribute.mk ":item/name") (Value.string "C")]
  let .ok (conn, _) := conn.transact tx4 | throw <| IO.userError "Tx4 failed"
  let tx5 : Transaction := [.retract e (Attribute.mk ":item/name") (Value.string "B")]
  let .ok (conn, _) := conn.transact tx5 | throw <| IO.userError "Tx5 failed"
  -- Should return "A" (the only remaining visible value)
  conn.db.getOne e (Attribute.mk ":item/name") ≡ some (Value.string "A")

/-! ## Entity History Tests -/

test "Entity history count" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [.add alice (Attribute.mk ":person/status") (Value.string "new")]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/status") (Value.string "new"),
    .add alice (Attribute.mk ":person/status") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx3 : Transaction := [
    .retract alice (Attribute.mk ":person/status") (Value.string "active"),
    .add alice (Attribute.mk ":person/status") (Value.string "inactive")
  ]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  let history := conn.entityHistory alice
  history.length ≡ 5

test "Status history count" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [.add alice (Attribute.mk ":person/status") (Value.string "new")]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/status") (Value.string "new"),
    .add alice (Attribute.mk ":person/status") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx3 : Transaction := [
    .retract alice (Attribute.mk ":person/status") (Value.string "active"),
    .add alice (Attribute.mk ":person/status") (Value.string "inactive")
  ]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"
  let statusHistory := conn.attrHistory alice (Attribute.mk ":person/status")
  statusHistory.length ≡ 5

test "History is sorted" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx1 : Transaction := [.add alice (Attribute.mk ":person/status") (Value.string "new")]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/status") (Value.string "new"),
    .add alice (Attribute.mk ":person/status") (Value.string "active")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let statusHistory := conn.attrHistory alice (Attribute.mk ":person/status")
  let txIds := statusHistory.map (·.tx.id)
  let sortedIds := (txIds.toArray.qsort (· < ·)).toList
  ensure (txIds == sortedIds) "History should be sorted by tx"

/-! ## Entity Retraction Tests -/

test "Retract entity removes own facts and inbound refs" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let (bob, conn) := conn.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/tag") (Value.string "active"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add bob (Attribute.mk ":person/friend") (Value.ref alice)
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [.retractEntity (.id alice)]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  ensure (conn.db.getOne alice (Attribute.mk ":person/name")).isNone "Alice name should be removed"
  ensure (conn.db.getOne alice (Attribute.mk ":person/tag")).isNone "Alice tag should be removed"
  ensure (conn.db.getOne bob (Attribute.mk ":person/friend")).isNone "Inbound ref should be removed"
  ensure (conn.db.getOne bob (Attribute.mk ":person/name")).isSome "Bob should remain"

test "Retract entity cascades through component refs" := do
  let schema := DSL.schema
    |>.string ":person/name"
    |>.ref ":person/address" |>.component
    |>.string ":address/street"
    |>.ref ":company/location"
    |>.build
  let db := Db.empty.withSchema schema
  let (person, db) := db.allocEntityId
  let (address, db) := db.allocEntityId
  let (company, db) := db.allocEntityId
  let tx1 : Transaction := [
    .add person (Attribute.mk ":person/name") (Value.string "Alice"),
    .add address (Attribute.mk ":address/street") (Value.string "Main"),
    .add person (Attribute.mk ":person/address") (Value.ref address),
    .add company (Attribute.mk ":company/location") (Value.ref address)
  ]
  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [.retractEntity (.id person)]
  let .ok (db, _) := db.transact tx2 | throw <| IO.userError "Tx2 failed"
  ensure (db.getOne person (Attribute.mk ":person/name")).isNone "Person should be removed"
  ensure (db.getOne address (Attribute.mk ":address/street")).isNone "Component address should be removed"
  ensure (db.getOne company (Attribute.mk ":company/location")).isNone "Inbound ref to component should be removed"

test "Retract entity is a no-op when entity has no facts" := do
  let db := Db.empty
  let (eid, db) := db.allocEntityId
  let tx : Transaction := [.retractEntity (.id eid)]
  match db.transact tx with
  | .ok (db', _) => db'.size ≡ 0
  | .error err => throw <| IO.userError s!"Retract entity should not fail: {err}"

test "Retract entity by lookup ref" := do
  let schema := DSL.schema
    |>.string ":person/email" |>.uniqueIdentity
    |>.build
  let db := Db.empty.withSchema schema
  let (alice, db) := db.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/email") (Value.string "alice@example.com"),
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retractEntity (.lookup (Attribute.mk ":person/email") (Value.string "alice@example.com"))
  ]
  let .ok (db, _) := db.transact tx2 | throw <| IO.userError "Tx2 failed"
  ensure (db.getOne alice (Attribute.mk ":person/name")).isNone "Lookup ref should retract entity"

end Ledger.Tests.Retraction
