/-
  Ledger Tests

  Basic tests for the Ledger database.
-/

import Crucible
import Ledger

namespace Ledger.Tests

open Crucible
open Ledger

testSuite "Ledger Tests"

/-! ## Core Types Tests -/

test "EntityId equality" := do
  let e1 := EntityId.mk 1
  ensure (e1 == e1) "EntityId should equal itself"

test "EntityId inequality" := do
  let e1 := EntityId.mk 1
  let e2 := EntityId.mk 2
  ensure (e1 != e2) "Different EntityIds should not be equal"

test "EntityId ordering" := do
  let e1 := EntityId.mk 1
  let e2 := EntityId.mk 2
  ensure (compare e1 e2 == .lt) "EntityId 1 should be less than 2"

test "EntityId temp check" := do
  ensure (EntityId.mk (-1) |>.isTemp) "Negative EntityId should be temp"

test "TxId next" := do
  let t1 := TxId.mk 1
  let t2 := t1.next
  t2.id ≡ 2

test "Attribute equality" := do
  let a1 := Attribute.mk ":person/name"
  ensure (a1 == a1) "Attribute should equal itself"

test "Attribute keyword" := do
  let a2 := Attribute.keyword "person" "age"
  a2.name ≡ ":person/age"

test "Value int equality" := do
  ensure (Value.int 42 == Value.int 42) "Same int values should be equal"

test "Value string equality" := do
  ensure (Value.string "hello" == Value.string "hello") "Same string values should be equal"

test "Value ordering same type" := do
  ensure (compare (Value.int 1) (Value.int 2) == .lt) "Int 1 should be less than 2"

test "Value ordering diff type" := do
  ensure (compare (Value.int 0) (Value.string "") == .lt) "Int should be less than string"

/-! ## Datom Tests -/

test "Datom added flag" := do
  let e := EntityId.mk 1
  let a := Attribute.mk ":person/name"
  let v := Value.string "Alice"
  let t := TxId.mk 1
  let d1 := Datom.assert e a v t
  ensure d1.added "Asserted datom should have added=true"

test "Datom retracted flag" := do
  let e := EntityId.mk 1
  let a := Attribute.mk ":person/name"
  let v := Value.string "Alice"
  let t := TxId.mk 1
  let d2 := Datom.retract e a v t
  ensure (!d2.added) "Retracted datom should have added=false"

test "Datom equality" := do
  let e := EntityId.mk 1
  let a := Attribute.mk ":person/name"
  let v := Value.string "Alice"
  let t := TxId.mk 1
  let d1 := Datom.assert e a v t
  ensure (d1.entity == e && d1.attr == a && d1.value == v) "Datom fields should match"

/-! ## Database Tests -/

test "Empty db size" := do
  let db := Db.empty
  db.size ≡ 0

test "Empty db basisT" := do
  let db := Db.empty
  db.basisT ≡ TxId.genesis

test "Allocated entity" := do
  let db := Db.empty
  let (e1, _) := db.allocEntityId
  e1.id ≡ 1

test "Transaction ID" := do
  let db := Db.empty
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 (Attribute.mk ":person/name") (Value.string "Alice"),
    .add e1 (Attribute.mk ":person/age") (Value.int 30)
  ]
  match db.transact tx with
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"
  | .ok (_, report) => report.txId.id ≡ 1

test "Transaction datoms count" := do
  let db := Db.empty
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 (Attribute.mk ":person/name") (Value.string "Alice"),
    .add e1 (Attribute.mk ":person/age") (Value.int 30)
  ]
  match db.transact tx with
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"
  | .ok (_, report) => report.txData.size ≡ 2

test "Database size after tx" := do
  let db := Db.empty
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 (Attribute.mk ":person/name") (Value.string "Alice"),
    .add e1 (Attribute.mk ":person/age") (Value.int 30)
  ]
  match db.transact tx with
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"
  | .ok (db', _) => db'.size ≡ 2

test "Get name" := do
  let db := Db.empty
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 (Attribute.mk ":person/name") (Value.string "Alice"),
    .add e1 (Attribute.mk ":person/age") (Value.int 30)
  ]
  match db.transact tx with
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"
  | .ok (db', _) =>
    let name := db'.getOne e1 (Attribute.mk ":person/name")
    name ≡ some (Value.string "Alice")

test "Get age" := do
  let db := Db.empty
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 (Attribute.mk ":person/name") (Value.string "Alice"),
    .add e1 (Attribute.mk ":person/age") (Value.int 30)
  ]
  match db.transact tx with
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"
  | .ok (db', _) =>
    let age := db'.getOne e1 (Attribute.mk ":person/age")
    age ≡ some (Value.int 30)

test "Original db unchanged" := do
  let db := Db.empty
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  match db.transact tx with
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"
  | .ok _ => db.size ≡ 0

/-! ## Multiple Transactions Tests -/

test "Second tx ID" := do
  let db := Db.empty
  let (e1, db) := db.allocEntityId
  let (e2, db) := db.allocEntityId
  let tx1 : Transaction := [.add e1 (Attribute.mk ":person/name") (Value.string "Alice")]
  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .add e2 (Attribute.mk ":person/name") (Value.string "Bob"),
    .add e1 (Attribute.mk ":person/friend") (Value.ref e2)
  ]
  let .ok (_, report) := db.transact tx2 | throw <| IO.userError "Tx2 failed"
  report.txId.id ≡ 2

test "Database size after multiple tx" := do
  let db := Db.empty
  let (e1, db) := db.allocEntityId
  let (e2, db) := db.allocEntityId
  let tx1 : Transaction := [.add e1 (Attribute.mk ":person/name") (Value.string "Alice")]
  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .add e2 (Attribute.mk ":person/name") (Value.string "Bob"),
    .add e1 (Attribute.mk ":person/friend") (Value.ref e2)
  ]
  let .ok (db, _) := db.transact tx2 | throw <| IO.userError "Tx2 failed"
  db.size ≡ 3

test "Reference value" := do
  let db := Db.empty
  let (e1, db) := db.allocEntityId
  let (e2, db) := db.allocEntityId
  let tx1 : Transaction := [.add e1 (Attribute.mk ":person/name") (Value.string "Alice")]
  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .add e2 (Attribute.mk ":person/name") (Value.string "Bob"),
    .add e1 (Attribute.mk ":person/friend") (Value.ref e2)
  ]
  let .ok (db, _) := db.transact tx2 | throw <| IO.userError "Tx2 failed"
  let friend := db.getOne e1 (Attribute.mk ":person/friend")
  friend ≡ some (Value.ref e2)

/-! ## Attribute Queries (AEVT) Tests -/

test "Entities with name" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add bob (Attribute.mk ":person/age") (Value.int 25),
    .add charlie (Attribute.mk ":person/name") (Value.string "Charlie")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let withName := db.entitiesWithAttr (Attribute.mk ":person/name")
  withName.length ≡ 3

test "Entities with age" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add bob (Attribute.mk ":person/age") (Value.int 25),
    .add charlie (Attribute.mk ":person/name") (Value.string "Charlie")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let withAge := db.entitiesWithAttr (Attribute.mk ":person/age")
  withAge.length ≡ 2

test "Name datoms count" := do
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
  let nameDatoms := db.datomsWithAttr (Attribute.mk ":person/name")
  nameDatoms.length ≡ 3

/-! ## Value Queries (AVET) Tests -/

test "Find by email" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/email") (Value.string "alice@example.com"),
    .add bob (Attribute.mk ":person/email") (Value.string "bob@example.com")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let aliceByEmail := db.findOneByAttrValue
    (Attribute.mk ":person/email") (Value.string "alice@example.com")
  aliceByEmail ≡ some alice

test "Find by age 30" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add charlie (Attribute.mk ":person/age") (Value.int 30),
    .add alice (Attribute.mk ":person/age") (Value.int 30)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let age30 := db.findByAttrValue (Attribute.mk ":person/age") (Value.int 30)
  age30.length ≡ 2

test "Not found returns none" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let notFound := db.findOneByAttrValue
    (Attribute.mk ":person/email") (Value.string "nobody@example.com")
  ensure notFound.isNone "Non-existent email should return none"

/-! ## Retraction Filtering in Value Queries (AVET) Tests -/

test "findByAttrValue excludes retracted entity" := do
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
  let active := conn.db.findByAttrValue (Attribute.mk ":person/tag") (Value.string "active")
  active.length ≡ 1

test "findByAttrValue excludes retracted entity - contains check" := do
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
  let active := conn.db.findByAttrValue (Attribute.mk ":person/tag") (Value.string "active")
  ensure (active.contains bob) "Should contain bob"

test "findByAttrValue excludes retracted entity - alice not present" := do
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
  let active := conn.db.findByAttrValue (Attribute.mk ":person/tag") (Value.string "active")
  ensure (!active.contains alice) "Should not contain retracted alice"

test "findOneByAttrValue returns none for retracted" := do
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
  let found := conn.db.findOneByAttrValue
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
  let active := conn.db.findByAttrValue (Attribute.mk ":person/tag") (Value.string "active")
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
  let active := conn.db.findByAttrValue (Attribute.mk ":person/tag") (Value.string "active")
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
  let owned := conn.db.findByAttrValue (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
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
  let owned := conn.db.findByAttrValue (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
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
  let owned := conn.db.findByAttrValue (Attribute.mk ":todo/owner") (Value.ref ⟨100⟩)
  ensure (!owned.contains e2 && !owned.contains e4) "Should not contain e2 and e4"

/-! ## Reverse References (VAET) Tests -/

test "Project refs count" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (project, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/works-on") (Value.ref project),
    .add bob (Attribute.mk ":person/works-on") (Value.ref project)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let projectRefs := db.referencingEntities project
  projectRefs.length ≡ 2

test "Project refs contains alice" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (project, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/works-on") (Value.ref project),
    .add bob (Attribute.mk ":person/works-on") (Value.ref project)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let projectRefs := db.referencingEntities project
  ensure (projectRefs.contains alice) "Project refs should contain alice"

test "Bob refs count" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/friend") (Value.ref bob)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let bobRefs := db.referencingEntities bob
  bobRefs.length ≡ 1

test "Workers count" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (project, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/works-on") (Value.ref project),
    .add bob (Attribute.mk ":person/works-on") (Value.ref project)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let workers := db.referencingViaAttr project (Attribute.mk ":person/works-on")
  workers.length ≡ 2

test "Charlie has no refs" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let charlieRefs := db.referencingEntities charlie
  ensure charlieRefs.isEmpty "Charlie should have no refs"

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

/-! ## Retraction Tests -/

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

/-! ## Datalog Query Tests -/

test "Query: entities with name" := do
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
  let pattern1 : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr (Attribute.mk ":person/name")
    value := .var ⟨"name"⟩
  }
  let result1 := Query.findEntities pattern1 db
  result1.length ≡ 3

test "Query: entities with age 30" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/age") (Value.int 25),
    .add charlie (Attribute.mk ":person/age") (Value.int 30)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let pattern2 : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr (Attribute.mk ":person/age")
    value := .value (Value.int 30)
  }
  let result2 := Query.findEntities pattern2 db
  result2.length ≡ 2

test "Query: multi-pattern result count" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add bob (Attribute.mk ":person/age") (Value.int 25)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query3 : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/age")
        value := .value (Value.int 30)
      },
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"name"⟩
      }
    ]
  }
  let result3 := Query.execute query3 db
  result3.size ≡ 1

test "Query: entities with friends" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/friend") (Value.ref bob)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let pattern4 : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr (Attribute.mk ":person/friend")
    value := .var ⟨"friend"⟩
  }
  let result4 := Query.findEntities pattern4 db
  result4.length ≡ 1

test "Query: alice's name binding" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let pattern5 : Pattern := {
    entity := .entity alice
    attr := .attr (Attribute.mk ":person/name")
    value := .var ⟨"name"⟩
  }
  let result5 := Query.executePattern pattern5 Binding.empty db.indexes
  result5.size ≡ 1

/-! ## Pull API Tests -/

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

/-! ## DSL Tests -/

test "DSL: TxBuilder name" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
    |>.addInt alice ":person/age" 30
    |>.addStr bob ":person/name" "Bob"
    |>.addRef alice ":person/friend" bob
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  DSL.attrStr db alice ":person/name" ≡ some "Alice"

test "DSL: TxBuilder age" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
    |>.addInt alice ":person/age" 30
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  DSL.attrInt db alice ":person/age" ≡ some 30

test "DSL: TxBuilder ref" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
    |>.addStr bob ":person/name" "Bob"
    |>.addRef alice ":person/friend" bob
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  DSL.attrRef db alice ":person/friend" ≡ some bob

test "DSL: QueryBuilder result" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
    |>.addInt alice ":person/age" 30
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  let qb := DSL.query
    |>.find "name"
    |>.where_ "e" ":person/name" "name"
    |>.whereInt "e" ":person/age" 30
  let result := qb.run db
  result.size ≡ 1

test "DSL: PullBuilder has name" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
    |>.addInt alice ":person/age" 30
    |>.addRef alice ":person/friend" bob
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  let pb := DSL.pull
    |>.attr ":person/name"
    |>.attr ":person/age"
  let pullResult := pb.run db alice
  ensure (pullResult.get? (Attribute.mk ":person/name")).isSome "Should have name"

test "DSL: findByStr" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  (DSL.findByStr db ":person/name" "Alice").length ≡ 1

test "DSL: findOneByStr" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  DSL.findOneByStr db ":person/name" "Alice" ≡ some alice

test "DSL: follow" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
    |>.addStr bob ":person/name" "Bob"
    |>.addRef alice ":person/friend" bob
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  DSL.follow db alice ":person/friend" ≡ some bob

test "DSL: followAndGet" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
    |>.addStr bob ":person/name" "Bob"
    |>.addRef alice ":person/friend" bob
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  let friendName := DSL.followAndGet db alice ":person/friend" ":person/name"
  friendName ≡ some (Value.string "Bob")

test "DSL: allWith" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
    |>.addStr bob ":person/name" "Bob"
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  let withName := DSL.allWith db ":person/name"
  withName.length ≡ 2

test "DSL: EntityBuilder email" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let eb := DSL.tx
    |>.entity alice
    |>.str ":person/email" "alice@example.com"
    |>.int ":person/score" 100
  let .ok (db, _) := eb.done.run db | throw <| IO.userError "EntityBuilder failed"
  DSL.attrStr db alice ":person/email" ≡ some "alice@example.com"

test "DSL: EntityBuilder score" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let eb := DSL.tx
    |>.entity alice
    |>.str ":person/email" "alice@example.com"
    |>.int ":person/score" 100
  let .ok (db, _) := eb.done.run db | throw <| IO.userError "EntityBuilder failed"
  DSL.attrInt db alice ":person/score" ≡ some 100

/-! ## Persistence Tests -/

test "JSON: Value int roundtrip" := do
  let v := Value.int 42
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value float roundtrip" := do
  let v := Value.float 3.14
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  ensure (v' == some v) "Float should roundtrip"

test "JSON: Value string roundtrip" := do
  let v := Value.string "hello world"
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value bool true roundtrip" := do
  let v := Value.bool true
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value bool false roundtrip" := do
  let v := Value.bool false
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value instant roundtrip" := do
  let v := Value.instant 1703347200000
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value ref roundtrip" := do
  let v := Value.ref ⟨123⟩
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value keyword roundtrip" := do
  let v := Value.keyword ":status/active"
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value bytes roundtrip" := do
  let v := Value.bytes (ByteArray.mk #[1, 2, 3, 4, 5])
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Datom roundtrip" := do
  let d : Datom := {
    entity := ⟨1⟩
    attr := ⟨":person/name"⟩
    value := .string "Alice"
    tx := ⟨5⟩
    added := true
  }
  let json := Persist.JSON.datomToJson d
  match Persist.JSON.datomFromJson json with
  | some d' =>
    ensure (d'.entity == d.entity && d'.attr == d.attr && d'.value == d.value) "Datom fields should match"
  | none => throw <| IO.userError "Datom parse failed"

test "JSON: TxLogEntry roundtrip" := do
  let entry : TxLogEntry := {
    txId := ⟨1⟩
    txInstant := 1703347200000
    datoms := #[
      { entity := ⟨1⟩, attr := ⟨":person/name"⟩, value := .string "Alice", tx := ⟨1⟩, added := true },
      { entity := ⟨1⟩, attr := ⟨":person/age"⟩, value := .int 30, tx := ⟨1⟩, added := true }
    ]
  }
  let json := Persist.JSON.txLogEntryToJson entry
  match Persist.JSON.txLogEntryFromJson json with
  | some entry' => entry'.txId.id ≡ entry.txId.id
  | none => throw <| IO.userError "TxLogEntry parse failed"

test "JSON: Base64 empty roundtrip" := do
  let data := ByteArray.empty
  let encoded := Persist.JSON.base64Encode data
  match Persist.JSON.base64Decode encoded with
  | some decoded => decoded.size ≡ 0
  | none => throw <| IO.userError "Base64 decode failed"

test "JSON: Base64 roundtrip" := do
  let data := ByteArray.mk #[72, 101, 108, 108, 111]  -- "Hello"
  let encoded := Persist.JSON.base64Encode data
  match Persist.JSON.base64Decode encoded with
  | some decoded => decoded ≡ data
  | none => throw <| IO.userError "Base64 decode failed"

test "JSON: String with special chars" := do
  let v := Value.string "Hello\nWorld\t\"quoted\""
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

#generate_tests

end Ledger.Tests

def main : IO Unit := do
  IO.println "╔══════════════════════════════════════╗"
  IO.println "║     Ledger Database Tests            ║"
  IO.println "╚══════════════════════════════════════╝"
  IO.println ""

  let exitCode ← runAllSuites

  IO.println ""
  if exitCode == 0 then
    IO.println "All tests passed!"
  else
    IO.println "Some tests failed"
    IO.Process.exit 1
