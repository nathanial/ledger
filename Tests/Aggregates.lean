/-
  Ledger.Tests.Aggregates - Aggregate function tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.Aggregates

open Crucible
open Ledger

testSuite "Aggregate Functions"

/-! ## Count Tests -/

test "Aggregate: count all rows" := do
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
  let result := DSL.query
    |>.find "e"
    |>.where_ "e" ":person/name" "name"
    |>.count "total"
    |>.runAggregate db
  result.size ≡ 1
  match result.rows.head? with
  | some (_, values) =>
    match values.head? with
    | some (.int 3) => pure ()
    | some v => throw <| IO.userError s!"Expected int 3, got {v}"
    | none => throw <| IO.userError "No aggregate value"
  | none => throw <| IO.userError "No rows"

/-! ## Sum Tests -/

test "Aggregate: sum integers" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/age") (Value.int 25),
    .add charlie (Attribute.mk ":person/age") (Value.int 35)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result := DSL.query
    |>.find "age"
    |>.where_ "e" ":person/age" "age"
    |>.sum "age" "total"
    |>.runAggregate db
  result.size ≡ 1
  match result.rows.head? with
  | some (_, values) =>
    match values.head? with
    | some (.int 90) => pure ()  -- 30 + 25 + 35
    | some v => throw <| IO.userError s!"Expected int 90, got {v}"
    | none => throw <| IO.userError "No aggregate value"
  | none => throw <| IO.userError "No rows"

test "Aggregate: sum floats" := do
  let db := Db.empty
  let (a, db) := db.allocEntityId
  let (b, db) := db.allocEntityId
  let tx : Transaction := [
    .add a (Attribute.mk ":item/price") (Value.float 10.5),
    .add b (Attribute.mk ":item/price") (Value.float 20.5)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result := DSL.query
    |>.find "price"
    |>.where_ "e" ":item/price" "price"
    |>.sum "price" "total"
    |>.runAggregate db
  match result.rows.head? with
  | some (_, values) =>
    match values.head? with
    | some (.float f) =>
      if f > 30.9 && f < 31.1 then pure ()  -- Approximately 31.0
      else throw <| IO.userError s!"Expected ~31.0, got {f}"
    | some v => throw <| IO.userError s!"Expected float, got {v}"
    | none => throw <| IO.userError "No aggregate value"
  | none => throw <| IO.userError "No rows"

/-! ## Avg Tests -/

test "Aggregate: average integers" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/age") (Value.int 20),
    .add charlie (Attribute.mk ":person/age") (Value.int 40)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result := DSL.query
    |>.find "age"
    |>.where_ "e" ":person/age" "age"
    |>.avg "age" "average"
    |>.runAggregate db
  match result.rows.head? with
  | some (_, values) =>
    match values.head? with
    | some (.float f) =>
      if f > 29.9 && f < 30.1 then pure ()  -- Approximately 30.0
      else throw <| IO.userError s!"Expected ~30.0, got {f}"
    | some v => throw <| IO.userError s!"Expected float, got {v}"
    | none => throw <| IO.userError "No aggregate value"
  | none => throw <| IO.userError "No rows"

/-! ## Min/Max Tests -/

test "Aggregate: min integer" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/age") (Value.int 20),
    .add charlie (Attribute.mk ":person/age") (Value.int 40)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result := DSL.query
    |>.find "age"
    |>.where_ "e" ":person/age" "age"
    |>.min "age" "youngest"
    |>.runAggregate db
  match result.rows.head? with
  | some (_, values) =>
    match values.head? with
    | some (AggregateValue.value (.int 20)) => pure ()
    | some v => throw <| IO.userError s!"Expected value int 20, got {v}"
    | none => throw <| IO.userError "No aggregate value"
  | none => throw <| IO.userError "No rows"

test "Aggregate: max integer" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/age") (Value.int 20),
    .add charlie (Attribute.mk ":person/age") (Value.int 40)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result := DSL.query
    |>.find "age"
    |>.where_ "e" ":person/age" "age"
    |>.max "age" "oldest"
    |>.runAggregate db
  match result.rows.head? with
  | some (_, values) =>
    match values.head? with
    | some (AggregateValue.value (.int 40)) => pure ()
    | some v => throw <| IO.userError s!"Expected value int 40, got {v}"
    | none => throw <| IO.userError "No aggregate value"
  | none => throw <| IO.userError "No rows"

test "Aggregate: min/max strings" := do
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
  let result := DSL.query
    |>.find "name"
    |>.where_ "e" ":person/name" "name"
    |>.min "name" "first"
    |>.max "name" "last"
    |>.runAggregate db
  match result.rows.head? with
  | some (_, values) =>
    -- First value is min
    match values.head? with
    | some (AggregateValue.value (.string "Alice")) => pure ()
    | some v => throw <| IO.userError s!"Expected string Alice, got {v}"
    | none => throw <| IO.userError "No min value"
    -- Second value is max
    match values[1]? with
    | some (AggregateValue.value (.string "Charlie")) => pure ()
    | some v => throw <| IO.userError s!"Expected string Charlie, got {v}"
    | none => throw <| IO.userError "No max value"
  | none => throw <| IO.userError "No rows"

/-! ## Group By Tests -/

test "Aggregate: group by with count" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let (dave, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/dept") (Value.string "Engineering"),
    .add bob (Attribute.mk ":person/dept") (Value.string "Engineering"),
    .add charlie (Attribute.mk ":person/dept") (Value.string "Sales"),
    .add dave (Attribute.mk ":person/dept") (Value.string "Sales")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  -- Run the aggregate query (runAggregate uses executeForAggregate which doesn't project/distinct)
  let result := DSL.query
    |>.find "dept"
    |>.where_ "e" ":person/dept" "dept"
    |>.groupBy "dept"
    |>.count "count"
    |>.runAggregate db
  if result.size != 2 then
    throw <| IO.userError s!"Expected 2 groups, got {result.size}"
  -- Each department should have count of 2
  for (_, values) in result.rows do
    match values.head? with
    | some (.int 2) => pure ()
    | some v => throw <| IO.userError s!"Expected count 2, got {v}"
    | none => throw <| IO.userError "No count value"

test "Aggregate: group by with sum" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/dept") (Value.string "Engineering"),
    .add alice (Attribute.mk ":person/salary") (Value.int 100000),
    .add bob (Attribute.mk ":person/dept") (Value.string "Engineering"),
    .add bob (Attribute.mk ":person/salary") (Value.int 120000),
    .add charlie (Attribute.mk ":person/dept") (Value.string "Sales"),
    .add charlie (Attribute.mk ":person/salary") (Value.int 80000)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  -- Query with both dept and salary
  let query : Query := {
    find := [⟨"dept"⟩, ⟨"salary"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/dept")
        value := .var ⟨"dept"⟩
      },
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/salary")
        value := .var ⟨"salary"⟩
      }
    ]
  }
  let baseRel := Query.executeRaw query db
  let result := Aggregate.execute baseRel [⟨"dept"⟩] [AggregateSpec.sum "salary" "total_salary"]
  result.size ≡ 2  -- Two departments
  -- Find Engineering dept total
  let engTotal := result.rows.find? fun (key, _) =>
    match key.lookup ⟨"dept"⟩ with
    | some (.value (.string "Engineering")) => true
    | _ => false
  match engTotal with
  | some (_, values) =>
    match values.head? with
    | some (.int 220000) => pure ()  -- 100000 + 120000
    | some v => throw <| IO.userError s!"Expected 220000, got {v}"
    | none => throw <| IO.userError "No sum value"
  | none => throw <| IO.userError "Engineering dept not found"

/-! ## Empty Results Tests -/

test "Aggregate: count on empty relation" := do
  let db := Db.empty
  let result := DSL.query
    |>.find "e"
    |>.where_ "e" ":person/name" "name"
    |>.count "total"
    |>.runAggregate db
  result.size ≡ 1
  match result.rows.head? with
  | some (_, values) =>
    match values.head? with
    | some (.int 0) => pure ()
    | some v => throw <| IO.userError s!"Expected int 0, got {v}"
    | none => throw <| IO.userError "No aggregate value"
  | none => throw <| IO.userError "No rows"

test "Aggregate: sum on empty relation returns null" := do
  let db := Db.empty
  let result := DSL.query
    |>.find "age"
    |>.where_ "e" ":person/age" "age"
    |>.sum "age" "total"
    |>.runAggregate db
  match result.rows.head? with
  | some (_, values) =>
    match values.head? with
    | some .null => pure ()
    | some v => throw <| IO.userError s!"Expected null, got {v}"
    | none => throw <| IO.userError "No aggregate value"
  | none => throw <| IO.userError "No rows"

/-! ## Multiple Aggregates Test -/

test "Aggregate: multiple aggregates" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/age") (Value.int 20),
    .add charlie (Attribute.mk ":person/age") (Value.int 40)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let result := DSL.query
    |>.find "age"
    |>.where_ "e" ":person/age" "age"
    |>.count "cnt"
    |>.sum "age" "total"
    |>.avg "age" "average"
    |>.min "age" "youngest"
    |>.max "age" "oldest"
    |>.runAggregate db
  result.size ≡ 1
  match result.rows.head? with
  | some (_, values) =>
    -- Check count
    match values[0]? with
    | some (AggregateValue.int 3) => pure ()
    | some v => throw <| IO.userError s!"Count: expected 3, got {v}"
    | none => throw <| IO.userError "No count"
    -- Check sum
    match values[1]? with
    | some (AggregateValue.int 90) => pure ()
    | some v => throw <| IO.userError s!"Sum: expected 90, got {v}"
    | none => throw <| IO.userError "No sum"
    -- Check avg
    match values[2]? with
    | some (AggregateValue.float f) =>
      if f > 29.9 && f < 30.1 then pure ()
      else throw <| IO.userError s!"Avg: expected ~30, got {f}"
    | some v => throw <| IO.userError s!"Avg: expected float, got {v}"
    | none => throw <| IO.userError "No avg"
    -- Check min
    match values[3]? with
    | some (AggregateValue.value (.int 20)) => pure ()
    | some v => throw <| IO.userError s!"Min: expected 20, got {v}"
    | none => throw <| IO.userError "No min"
    -- Check max
    match values[4]? with
    | some (AggregateValue.value (.int 40)) => pure ()
    | some v => throw <| IO.userError s!"Max: expected 40, got {v}"
    | none => throw <| IO.userError "No max"
  | none => throw <| IO.userError "No rows"

end Ledger.Tests.Aggregates
