/-
  Ledger.Tests.Database - Database operations, transactions, and index queries
-/

import Crucible
import Ledger

namespace Ledger.Tests.Database

open Crucible
open Ledger

testSuite "Database Operations"

/-! ## Basic Database Tests -/

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

/-! ## Transaction Tests -/

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

#generate_tests

end Ledger.Tests.Database
