/-
  Ledger.Tests.Derive - Tests for the LedgerEntity derive handler
-/

import Crucible
import Ledger
import Tests.DeriveTypes

open Ledger.Derive
open Ledger.Tests.DeriveTypes

-- Generate entity code for test structures (must be outside namespaces)
makeLedgerEntity TestPerson
makeLedgerEntity TestTask
makeLedgerEntity TestItem

namespace Ledger.Tests.Derive

open Crucible
open Ledger
open Ledger.Tests.DeriveTypes

testSuite "LedgerEntity Derive"

-- ============================================
-- Attribute generation tests
-- ============================================

test "Derive: generates attribute constants" := do
  -- Check that attributes have correct names
  TestPerson.attr_name.name ≡ ":testperson/name"
  TestPerson.attr_age.name ≡ ":testperson/age"
  TestPerson.attr_active.name ≡ ":testperson/active"

test "Derive: generates attributes list" := do
  -- Check that the attributes list contains all fields
  TestPerson.attributes.length ≡ 3

test "Derive: skips id field" := do
  -- TestItem has id, name, value - but id should be skipped
  TestItem.attributes.length ≡ 2
  TestItem.attr_name.name ≡ ":testitem/name"
  TestItem.attr_value.name ≡ ":testitem/value"

test "Derive: lowercase prefix" := do
  -- Verify the attribute prefix is lowercase struct name
  TestPerson.attr_name.name ≡ ":testperson/name"

-- ============================================
-- Pull specification tests
-- ============================================

test "Derive: generates pullSpec" := do
  -- Check that pullSpec has correct number of patterns
  TestPerson.pullSpec.length ≡ 3

-- ============================================
-- Transaction builder tests
-- ============================================

test "Derive: createOps generates correct ops" := do
  let eid : EntityId := ⟨42⟩
  let person : TestPerson := { name := "Alice", age := 30, active := true }
  let ops := TestPerson.createOps eid person
  ops.length ≡ 3

test "Derive: createOps with EntityId reference" := do
  let eid : EntityId := ⟨100⟩
  let assigneeId : EntityId := ⟨50⟩
  let task : TestTask := { title := "Test", description := "Desc", priority := 1, assignee := assigneeId }
  let ops := TestTask.createOps eid task
  ops.length ≡ 4

-- ============================================
-- Round-trip tests (create -> pull)
-- ============================================

test "Derive: round-trip simple entity" := do
  let db := Db.empty
  let (eid, db) := db.allocEntityId
  let person : TestPerson := { name := "Alice", age := 30, active := true }
  let ops := TestPerson.createOps eid person
  let .ok (db, _) := db.transact ops | throw <| IO.userError "Transaction failed"

  match TestPerson.pull db eid with
  | some pulled =>
    pulled.name ≡ "Alice"
    pulled.age ≡ 30
    pulled.active ≡ true
  | none => throw <| IO.userError "Pull returned none"

test "Derive: round-trip with reference" := do
  let db := Db.empty
  let (taskId, db) := db.allocEntityId
  let (assigneeId, db) := db.allocEntityId

  let task : TestTask := {
    title := "Important Task"
    description := "This is a test task"
    priority := 5
    assignee := assigneeId
  }

  let ops := TestTask.createOps taskId task
  let .ok (db, _) := db.transact ops | throw <| IO.userError "Transaction failed"

  match TestTask.pull db taskId with
  | some pulled =>
    pulled.title ≡ "Important Task"
    pulled.description ≡ "This is a test task"
    pulled.priority ≡ 5
    ensure (pulled.assignee == assigneeId) "Assignee should match"
  | none => throw <| IO.userError "Pull returned none"

test "Derive: round-trip with Int field" := do
  let db := Db.empty
  let (eid, db) := db.allocEntityId
  let item : TestItem := { id := 999, name := "Widget", value := -42 }
  let ops := TestItem.createOps eid item
  let .ok (db, _) := db.transact ops | throw <| IO.userError "Transaction failed"

  match TestItem.pull db eid with
  | some pulled =>
    pulled.name ≡ "Widget"
    pulled.value ≡ -42
  | none => throw <| IO.userError "Pull returned none"

-- ============================================
-- Retraction tests
-- ============================================

test "Derive: retractionOps" := do
  let db := Db.empty
  let (eid, db) := db.allocEntityId
  let person : TestPerson := { name := "Bob", age := 25, active := false }
  let ops := TestPerson.createOps eid person
  let .ok (db, _) := db.transact ops | throw <| IO.userError "Create failed"

  -- Verify entity exists
  ensure (TestPerson.pull db eid).isSome "Entity should exist"

  -- Generate and apply retraction
  let retractions := TestPerson.retractionOps db eid
  retractions.length ≡ 3

  let .ok (db, _) := db.transact retractions | throw <| IO.userError "Retract failed"

  -- Verify entity no longer exists
  ensure (TestPerson.pull db eid).isNone "Entity should be gone"

end Ledger.Tests.Derive
