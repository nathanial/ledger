/-
  Ledger.Tests.Schema - Schema validation tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.Schema

open Crucible
open Ledger

testSuite "Schema Validation"

/-! ## Type Validation Tests -/

test "Type validation passes for correct type" := do
  let schema := DSL.schema
    |>.string ":person/name"
    |>.int ":person/age"
    |>.build
  let db := Db.empty.withSchema schema
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":person/name"⟩ (.string "Alice"),
    .add e1 ⟨":person/age"⟩ (.int 30)
  ]
  match db.transact tx with
  | .ok (db', _) => db'.size ≡ 2
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"

test "Type validation fails for wrong type" := do
  let schema := DSL.schema
    |>.int ":person/age"
    |>.build
  let db := Db.empty.withSchema schema
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":person/age"⟩ (.string "thirty")  -- Wrong type
  ]
  match db.transact tx with
  | .ok _ => throw <| IO.userError "Expected type mismatch error"
  | .error err =>
    let errStr := toString err
    let parts := errStr.splitOn "Type mismatch"
    (decide (parts.length >= 2)) ≡ true

test "Schema violation uses TxError.schemaViolation" := do
  let schema := DSL.schema
    |>.string ":person/name"
    |>.build
  let db := Db.empty.withSchema schema true
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":person/age"⟩ (.int 30)
  ]
  match db.transact tx with
  | .ok _ => throw <| IO.userError "Expected schemaViolation"
  | .error (.schemaViolation msg) =>
    let parts := msg.splitOn "Undefined attribute"
    (decide (parts.length >= 2)) ≡ true
  | .error _ => throw <| IO.userError "Expected schemaViolation error type"

/-! ## Cardinality Tests -/

test "Cardinality-one allows single value" := do
  let schema := DSL.schema
    |>.string ":person/name"
    |>.build
  let db := Db.empty.withSchema schema
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":person/name"⟩ (.string "Alice")
  ]
  match db.transact tx with
  | .ok (db', _) => db'.size ≡ 1
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"

test "Cardinality-one rejects multiple values in same tx" := do
  let schema := DSL.schema
    |>.string ":person/name"
    |>.build
  let db := Db.empty.withSchema schema
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":person/name"⟩ (.string "Alice"),
    .add e1 ⟨":person/name"⟩ (.string "Bob")  -- Second value - violation
  ]
  match db.transact tx with
  | .ok _ => throw <| IO.userError "Expected cardinality violation"
  | .error err =>
    let errStr := toString err
    let parts := errStr.splitOn "Cardinality violation"
    (decide (parts.length >= 2)) ≡ true

test "Cardinality-many allows multiple values" := do
  let schema := DSL.schema
    |>.string ":person/nicknames" |>.many
    |>.build
  let db := Db.empty.withSchema schema
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":person/nicknames"⟩ (.string "Al"),
    .add e1 ⟨":person/nicknames"⟩ (.string "Ally")
  ]
  match db.transact tx with
  | .ok (db', _) => db'.size ≡ 2
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"

/-! ## Uniqueness Tests -/

test "Uniqueness identity allows first value" := do
  let schema := DSL.schema
    |>.string ":person/email" |>.uniqueIdentity
    |>.build
  let db := Db.empty.withSchema schema
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":person/email"⟩ (.string "alice@example.com")
  ]
  match db.transact tx with
  | .ok (db', _) => db'.size ≡ 1
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"

test "Uniqueness identity rejects duplicate in different entity" := do
  let schema := DSL.schema
    |>.string ":person/email" |>.uniqueIdentity
    |>.build
  let db := Db.empty.withSchema schema
  let (e1, db) := db.allocEntityId
  let (e2, db) := db.allocEntityId

  -- First transaction succeeds
  let tx1 : Transaction := [
    .add e1 ⟨":person/email"⟩ (.string "alice@example.com")
  ]
  match db.transact tx1 with
  | .error err => throw <| IO.userError s!"First transaction failed: {err}"
  | .ok (db', _) =>
    -- Second transaction with same email should fail
    let tx2 : Transaction := [
      .add e2 ⟨":person/email"⟩ (.string "alice@example.com")
    ]
    match db'.transact tx2 with
    | .ok _ => throw <| IO.userError "Expected uniqueness violation"
    | .error err =>
      let errStr := toString err
      let parts := errStr.splitOn "Uniqueness violation"
      (decide (parts.length >= 2)) ≡ true

/-! ## Strict Mode Tests -/

test "Permissive mode allows undefined attributes" := do
  let schema := DSL.schema
    |>.string ":person/name"
    |>.build
  let db := Db.empty.withSchema schema (strict := false)
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":person/name"⟩ (.string "Alice"),
    .add e1 ⟨":undefined/attr"⟩ (.string "some value")  -- Not in schema
  ]
  match db.transact tx with
  | .ok (db', _) => db'.size ≡ 2
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"

test "Strict mode rejects undefined attributes" := do
  let schema := DSL.schema
    |>.string ":person/name"
    |>.build
  let db := Db.empty.withSchema schema (strict := true)
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":undefined/attr"⟩ (.string "some value")
  ]
  match db.transact tx with
  | .ok _ => throw <| IO.userError "Expected undefined attribute error"
  | .error err =>
    let errStr := toString err
    let parts := errStr.splitOn "Undefined attribute"
    (decide (parts.length >= 2)) ≡ true

/-! ## Schema-free Mode Tests -/

test "No schema allows any attributes" := do
  let db := Db.empty  -- No schema configured
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":any/attr"⟩ (.string "value"),
    .add e1 ⟨":another/attr"⟩ (.int 42)
  ]
  match db.transact tx with
  | .ok (db', _) => db'.size ≡ 2
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"

/-! ## Schema Builder Tests -/

test "Schema builder creates correct types" := do
  let schema := DSL.schema
    |>.string ":test/string"
    |>.int ":test/int"
    |>.float ":test/float"
    |>.bool ":test/bool"
    |>.instant ":test/instant"
    |>.ref ":test/ref"
    |>.keyword ":test/keyword"
    |>.bytes ":test/bytes"
    |>.build

  schema.size ≡ 8

test "Schema builder cardinality many" := do
  let schema := DSL.schema
    |>.string ":test/tags" |>.many
    |>.build
  match schema.get? ⟨":test/tags"⟩ with
  | some attrSchema => attrSchema.cardinality ≡ Cardinality.many
  | none => throw <| IO.userError "Attribute not found"

test "Schema builder unique identity" := do
  let schema := DSL.schema
    |>.string ":test/email" |>.uniqueIdentity
    |>.build
  match schema.get? ⟨":test/email"⟩ with
  | some attrSchema =>
    attrSchema.unique ≡ some Unique.identity
    attrSchema.indexed ≡ true
  | none => throw <| IO.userError "Attribute not found"

test "Schema builder component flag" := do
  let schema := DSL.schema
    |>.ref ":test/child" |>.component
    |>.build
  match schema.get? ⟨":test/child"⟩ with
  | some attrSchema =>
    attrSchema.component ≡ true
  | none => throw <| IO.userError "Attribute not found"

/-! ## Retraction Tests -/

test "Retractions don't need schema validation" := do
  let schema := DSL.schema
    |>.string ":person/name"
    |>.build
  let db := Db.empty.withSchema schema
  let (e1, db) := db.allocEntityId

  -- Add a value first
  let tx1 : Transaction := [.add e1 ⟨":person/name"⟩ (.string "Alice")]
  match db.transact tx1 with
  | .error err => throw <| IO.userError s!"First transaction failed: {err}"
  | .ok (db', _) =>
    -- Retract it
    let tx2 : Transaction := [.retract e1 ⟨":person/name"⟩ (.string "Alice")]
    match db'.transact tx2 with
    | .ok (db'', _) =>
      let name := db''.getOne e1 ⟨":person/name"⟩
      name ≡ none
    | .error err => throw <| IO.userError s!"Retraction failed: {err}"

/-! ## Ref Type Tests -/

test "Ref type validates entity references" := do
  let schema := DSL.schema
    |>.ref ":person/friend"
    |>.build
  let db := Db.empty.withSchema schema
  let (e1, db) := db.allocEntityId
  let (e2, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":person/friend"⟩ (.ref e2)
  ]
  match db.transact tx with
  | .ok (db', _) => db'.size ≡ 1
  | .error err => throw <| IO.userError s!"Transaction failed: {err}"

test "Ref type rejects non-ref values" := do
  let schema := DSL.schema
    |>.ref ":person/friend"
    |>.build
  let db := Db.empty.withSchema schema
  let (e1, db) := db.allocEntityId
  let tx : Transaction := [
    .add e1 ⟨":person/friend"⟩ (.string "not a ref")
  ]
  match db.transact tx with
  | .ok _ => throw <| IO.userError "Expected type mismatch error"
  | .error _ => pure ()

end Ledger.Tests.Schema
