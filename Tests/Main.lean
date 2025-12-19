/-
  Ledger Tests

  Basic tests for the Ledger database.
-/

import Ledger

open Ledger

/-- Simple test helper. -/
def test (name : String) (cond : Bool) : IO Unit := do
  if cond then
    IO.println s!"✓ {name}"
  else
    IO.println s!"✗ {name}"
    throw <| IO.userError s!"Test failed: {name}"

/-- Test core types. -/
def testCoreTypes : IO Unit := do
  IO.println "Testing Core Types..."

  -- EntityId tests
  let e1 := EntityId.mk 1
  let e2 := EntityId.mk 2
  test "EntityId equality" (e1 == e1)
  test "EntityId inequality" (e1 != e2)
  test "EntityId ordering" (compare e1 e2 == .lt)
  test "EntityId temp check" (EntityId.mk (-1) |>.isTemp)

  -- TxId tests
  let t1 := TxId.mk 1
  let t2 := t1.next
  test "TxId next" (t2.id == 2)

  -- Attribute tests
  let a1 := Attribute.mk ":person/name"
  let a2 := Attribute.keyword "person" "age"
  test "Attribute equality" (a1 == a1)
  test "Attribute keyword" (a2.name == ":person/age")

  -- Value tests
  test "Value int" (Value.int 42 == Value.int 42)
  test "Value string" (Value.string "hello" == Value.string "hello")
  test "Value ordering same type" (compare (Value.int 1) (Value.int 2) == .lt)
  test "Value ordering diff type" (compare (Value.int 0) (Value.string "") == .lt)

  IO.println ""

/-- Test datom creation and comparison. -/
def testDatoms : IO Unit := do
  IO.println "Testing Datoms..."

  let e := EntityId.mk 1
  let a := Attribute.mk ":person/name"
  let v := Value.string "Alice"
  let t := TxId.mk 1

  let d1 := Datom.assert e a v t
  test "Datom added flag" d1.added

  let d2 := Datom.retract e a v t
  test "Datom retracted flag" (!d2.added)

  test "Datom equality" (d1.entity == e && d1.attr == a && d1.value == v)

  IO.println ""

/-- Test basic database operations. -/
def testDatabase : IO Unit := do
  IO.println "Testing Database..."

  -- Create empty database
  let db := Db.empty
  test "Empty db size" (db.size == 0)
  test "Empty db basisT" (db.basisT == TxId.genesis)

  -- Allocate entity ID
  let (e1, db) := db.allocEntityId
  test "Allocated entity" (e1.id == 1)

  -- Create a transaction
  let tx : Transaction := [
    .add e1 (Attribute.mk ":person/name") (Value.string "Alice"),
    .add e1 (Attribute.mk ":person/age") (Value.int 30)
  ]

  -- Process transaction
  match db.transact tx with
  | .error err =>
    throw <| IO.userError s!"Transaction failed: {err}"
  | .ok (db', report) =>
    test "Transaction ID" (report.txId.id == 1)
    test "Transaction datoms count" (report.txData.size == 2)
    test "Database size after tx" (db'.size == 2)

    -- Query the entity
    let name := db'.getOne e1 (Attribute.mk ":person/name")
    test "Get name" (name == some (Value.string "Alice"))

    let age := db'.getOne e1 (Attribute.mk ":person/age")
    test "Get age" (age == some (Value.int 30))

    -- Original database unchanged
    test "Original db unchanged" (db.size == 0)

  IO.println ""

/-- Test multiple transactions. -/
def testMultipleTransactions : IO Unit := do
  IO.println "Testing Multiple Transactions..."

  let db := Db.empty
  let (e1, db) := db.allocEntityId
  let (e2, db) := db.allocEntityId

  -- First transaction
  let tx1 : Transaction := [
    .add e1 (Attribute.mk ":person/name") (Value.string "Alice")
  ]

  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"

  -- Second transaction
  let tx2 : Transaction := [
    .add e2 (Attribute.mk ":person/name") (Value.string "Bob"),
    .add e1 (Attribute.mk ":person/friend") (Value.ref e2)
  ]

  let .ok (db, report) := db.transact tx2 | throw <| IO.userError "Tx2 failed"

  test "Second tx ID" (report.txId.id == 2)
  test "Database size" (db.size == 3)  -- 1 from tx1 + 2 from tx2

  -- Check reference
  let friend := db.getOne e1 (Attribute.mk ":person/friend")
  test "Reference value" (friend == some (Value.ref e2))

  IO.println ""

/-- Test attribute-based queries (AEVT index). -/
def testAttributeQueries : IO Unit := do
  IO.println "Testing Attribute Queries (AEVT)..."

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
    -- Note: charlie has no age
  ]

  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"

  -- Query all entities with :person/name
  let withName := db.entitiesWithAttr (Attribute.mk ":person/name")
  test "Entities with name" (withName.length == 3)

  -- Query all entities with :person/age
  let withAge := db.entitiesWithAttr (Attribute.mk ":person/age")
  test "Entities with age" (withAge.length == 2)

  -- Query all datoms with :person/name
  let nameDatoms := db.datomsWithAttr (Attribute.mk ":person/name")
  test "Name datoms count" (nameDatoms.length == 3)

  IO.println ""

/-- Test value-based queries (AVET index). -/
def testValueQueries : IO Unit := do
  IO.println "Testing Value Queries (AVET)..."

  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId

  let tx : Transaction := [
    .add alice (Attribute.mk ":person/email") (Value.string "alice@example.com"),
    .add bob (Attribute.mk ":person/email") (Value.string "bob@example.com"),
    .add charlie (Attribute.mk ":person/age") (Value.int 30),
    .add alice (Attribute.mk ":person/age") (Value.int 30)
  ]

  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"

  -- Find entity by unique email
  let aliceByEmail := db.findOneByAttrValue
    (Attribute.mk ":person/email") (Value.string "alice@example.com")
  test "Find by email" (aliceByEmail == some alice)

  -- Find entities with age 30
  let age30 := db.findByAttrValue (Attribute.mk ":person/age") (Value.int 30)
  test "Find by age 30" (age30.length == 2)

  -- Find by non-existent value
  let notFound := db.findOneByAttrValue
    (Attribute.mk ":person/email") (Value.string "nobody@example.com")
  test "Not found returns none" (notFound.isNone)

  IO.println ""

/-- Test reverse reference queries (VAET index). -/
def testReverseRefs : IO Unit := do
  IO.println "Testing Reverse References (VAET)..."

  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let (project, db) := db.allocEntityId

  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add charlie (Attribute.mk ":person/name") (Value.string "Charlie"),
    .add project (Attribute.mk ":project/name") (Value.string "Ledger"),
    -- Alice and Bob work on the project
    .add alice (Attribute.mk ":person/works-on") (Value.ref project),
    .add bob (Attribute.mk ":person/works-on") (Value.ref project),
    -- Alice is friends with Bob
    .add alice (Attribute.mk ":person/friend") (Value.ref bob)
  ]

  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"

  -- Who references the project?
  let projectRefs := db.referencingEntities project
  test "Project refs count" (projectRefs.length == 2)
  test "Project refs contains alice" (projectRefs.contains alice)
  test "Project refs contains bob" (projectRefs.contains bob)

  -- Who references Bob?
  let bobRefs := db.referencingEntities bob
  test "Bob refs count" (bobRefs.length == 1)
  test "Bob refs contains alice" (bobRefs.contains alice)

  -- Who works on the project (via specific attribute)?
  let workers := db.referencingViaAttr project (Attribute.mk ":person/works-on")
  test "Workers count" (workers.length == 2)

  -- Who is friends with Bob?
  let bobFriends := db.referencingViaAttr bob (Attribute.mk ":person/friend")
  test "Bob friends count" (bobFriends.length == 1)

  -- Charlie has no incoming references
  let charlieRefs := db.referencingEntities charlie
  test "Charlie has no refs" (charlieRefs.isEmpty)

  IO.println ""

/-- Test time-travel with Connection. -/
def testTimeTravel : IO Unit := do
  IO.println "Testing Time Travel..."

  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId

  -- Transaction 1: Add Alice with age 25
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 25)
  ]
  let .ok (conn, report1) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx1Id := report1.txId

  -- Transaction 2: Update Alice's age to 26
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/age") (Value.int 25),
    .add alice (Attribute.mk ":person/age") (Value.int 26)
  ]
  let .ok (conn, report2) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"
  let tx2Id := report2.txId

  -- Transaction 3: Update Alice's age to 27
  let tx3 : Transaction := [
    .retract alice (Attribute.mk ":person/age") (Value.int 26),
    .add alice (Attribute.mk ":person/age") (Value.int 27)
  ]
  let .ok (conn, _) := conn.transact tx3 | throw <| IO.userError "Tx3 failed"

  -- Current database should show age 27
  let currentAge := conn.current.getOne alice (Attribute.mk ":person/age")
  test "Current age is 27" (currentAge == some (Value.int 27))

  -- asOf tx1 should show age 25
  let dbAtTx1 := conn.asOf tx1Id
  let ageAtTx1 := dbAtTx1.getOne alice (Attribute.mk ":person/age")
  test "Age at tx1 is 25" (ageAtTx1 == some (Value.int 25))

  -- asOf tx2 should show age 26
  let dbAtTx2 := conn.asOf tx2Id
  let ageAtTx2 := dbAtTx2.getOne alice (Attribute.mk ":person/age")
  test "Age at tx2 is 26" (ageAtTx2 == some (Value.int 26))

  -- since tx1 should include tx2 and tx3 datoms
  let changesSinceTx1 := conn.since tx1Id
  test "Changes since tx1" (changesSinceTx1.length == 4)  -- 2 from tx2 + 2 from tx3

  -- History should show all changes
  let history := conn.attrHistory alice (Attribute.mk ":person/age")
  test "History has 5 entries" (history.length == 5)  -- add 25, retract 25, add 26, retract 26, add 27

  IO.println ""

/-- Test retractions. -/
def testRetractions : IO Unit := do
  IO.println "Testing Retractions..."

  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId

  -- Add Alice
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (conn, _) := conn.transact tx1 | throw <| IO.userError "Tx1 failed"

  -- Verify both attributes exist
  test "Has name" (conn.current.getOne alice (Attribute.mk ":person/name") == some (Value.string "Alice"))
  test "Has email" (conn.current.getOne alice (Attribute.mk ":person/email") == some (Value.string "alice@example.com"))

  -- Retract email
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/email") (Value.string "alice@example.com")
  ]
  let .ok (conn, _) := conn.transact tx2 | throw <| IO.userError "Tx2 failed"

  -- Name should still exist, email should be gone
  test "Still has name" (conn.current.getOne alice (Attribute.mk ":person/name") == some (Value.string "Alice"))

  -- Email should be retracted (not visible in current view)
  let email := conn.current.getOne alice (Attribute.mk ":person/email")
  test "Email retracted" (email.isNone)

  IO.println ""

/-- Test entity history. -/
def testEntityHistory : IO Unit := do
  IO.println "Testing Entity History..."

  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId

  -- Multiple updates over time
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

  -- Get full history
  let history := conn.entityHistory alice
  test "Entity history count" (history.length == 5)  -- 1 + 2 + 2

  -- Get attribute history
  let statusHistory := conn.attrHistory alice (Attribute.mk ":person/status")
  test "Status history count" (statusHistory.length == 5)

  -- Verify order (should be sorted by tx)
  let txIds := statusHistory.map (·.tx.id)
  let sortedIds := (txIds.toArray.qsort (· < ·)).toList
  let isSorted := txIds == sortedIds
  test "History is sorted" isSorted

  IO.println ""

/-- Test Datalog queries. -/
def testQueries : IO Unit := do
  IO.println "Testing Datalog Queries..."

  -- Set up a database with people and relationships
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId

  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add bob (Attribute.mk ":person/age") (Value.int 25),
    .add charlie (Attribute.mk ":person/name") (Value.string "Charlie"),
    .add charlie (Attribute.mk ":person/age") (Value.int 30),
    .add alice (Attribute.mk ":person/friend") (Value.ref bob)
  ]

  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"

  -- Test 1: Simple pattern - find all entities with :person/name
  let pattern1 : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr (Attribute.mk ":person/name")
    value := .var ⟨"name"⟩
  }
  let result1 := Query.findEntities pattern1 db
  test "Query: entities with name" (result1.length == 3)

  -- Test 2: Pattern with constant value - find entities with age 30
  let pattern2 : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr (Attribute.mk ":person/age")
    value := .value (Value.int 30)
  }
  let result2 := Query.findEntities pattern2 db
  test "Query: entities with age 30" (result2.length == 2)
  test "Query: alice has age 30" (result2.contains alice)
  test "Query: charlie has age 30" (result2.contains charlie)

  -- Test 3: Full query with multiple patterns - find name of people aged 30
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
  test "Query: multi-pattern result count" (result3.size == 2)

  -- Test 4: Query with entity reference
  let pattern4 : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr (Attribute.mk ":person/friend")
    value := .var ⟨"friend"⟩
  }
  let result4 := Query.findEntities pattern4 db
  test "Query: entities with friends" (result4.length == 1)
  test "Query: alice has friend" (result4.contains alice)

  -- Test 5: Query with bound entity
  let pattern5 : Pattern := {
    entity := .entity alice
    attr := .attr (Attribute.mk ":person/name")
    value := .var ⟨"name"⟩
  }
  let result5 := Query.executePattern pattern5 Binding.empty db.indexes
  test "Query: alice's name binding" (result5.size == 1)

  IO.println ""

/-- Main test runner. -/
def main : IO Unit := do
  IO.println "╔══════════════════════════════════════╗"
  IO.println "║     Ledger Database Tests            ║"
  IO.println "╚══════════════════════════════════════╝"
  IO.println ""

  testCoreTypes
  testDatoms
  testDatabase
  testMultipleTransactions
  testAttributeQueries
  testValueQueries
  testReverseRefs
  testTimeTravel
  testRetractions
  testEntityHistory
  testQueries

  IO.println "════════════════════════════════════════"
  IO.println "All tests passed!"
