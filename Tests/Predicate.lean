/-
  Ledger.Tests.Predicate - Predicate query tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.Predicate

open Crucible
open Ledger
open Ledger.Query

testSuite "Predicate Queries"

private def seedDb : Db := Id.run do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add alice (Attribute.mk ":person/score") (Value.float 4.5),
    .add alice (Attribute.mk ":person/status") (Value.string "active"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add bob (Attribute.mk ":person/age") (Value.int 42),
    .add bob (Attribute.mk ":person/score") (Value.float 2.0),
    .add bob (Attribute.mk ":person/status") (Value.string "inactive"),
    .add charlie (Attribute.mk ":person/name") (Value.string "Charlie"),
    .add charlie (Attribute.mk ":person/age") (Value.int 25)
  ]
  let .ok (db, _) := db.transact tx | panic! "Tx failed"
  return db

test "Predicate: numeric comparisons" := do
  let db := seedDb
  let pred := Predicate.and [
    Predicate.gt (PredExpr.var "age") (PredExpr.int 30),
    Predicate.lte (PredExpr.var "age") (PredExpr.int 42),
    Predicate.ne (PredExpr.var "age") (PredExpr.int 35)
  ]
  let result := DSL.query
    |>.find "name"
    |>.where_ "e" ":person/name" "name"
    |>.where_ "e" ":person/age" "age"
    |>.wherePred pred
    |>.run db
  result.size ≡ 1
  match result.rows.bindings with
  | [b] =>
    match b.lookup ⟨"name"⟩ with
    | some (.value (.string "Bob")) => pure ()
    | _ => throw <| IO.userError "Expected Bob"
  | _ => throw <| IO.userError "Expected one result"

test "Predicate: arithmetic add/sub/mul" := do
  let db := seedDb
  let pred := Predicate.and [
    Predicate.gt (PredExpr.add (PredExpr.var "age") (PredExpr.int 5)) (PredExpr.int 40),
    Predicate.eq (PredExpr.sub (PredExpr.var "age") (PredExpr.int 2)) (PredExpr.int 40),
    Predicate.eq (PredExpr.mul (PredExpr.int 3) (PredExpr.int 5)) (PredExpr.int 15)
  ]
  let result := DSL.query
    |>.find "name"
    |>.where_ "e" ":person/name" "name"
    |>.where_ "e" ":person/age" "age"
    |>.wherePred pred
    |>.run db
  result.size ≡ 1
  match result.rows.bindings with
  | [b] =>
    match b.lookup ⟨"name"⟩ with
    | some (.value (.string "Bob")) => pure ()
    | _ => throw <| IO.userError "Expected Bob"
  | _ => throw <| IO.userError "Expected one result"

test "Predicate: arithmetic division" := do
  let db := seedDb
  let pred := Predicate.eq
    (PredExpr.div (PredExpr.var "score") (PredExpr.int 2))
    (PredExpr.float 2.25)
  let result := DSL.query
    |>.find "name"
    |>.where_ "e" ":person/name" "name"
    |>.where_ "e" ":person/score" "score"
    |>.wherePred pred
    |>.run db
  result.size ≡ 1
  match result.rows.bindings with
  | [b] =>
    match b.lookup ⟨"name"⟩ with
    | some (.value (.string "Alice")) => pure ()
    | _ => throw <| IO.userError "Expected Alice"
  | _ => throw <| IO.userError "Expected one result"

test "Predicate: division by zero yields no results" := do
  let db := seedDb
  let pred := Predicate.eq
    (PredExpr.div (PredExpr.var "age") (PredExpr.int 0))
    (PredExpr.int 1)
  let result := DSL.query
    |>.find "e"
    |>.where_ "e" ":person/age" "age"
    |>.wherePred pred
    |>.run db
  result.size ≡ 0

test "Predicate: string operations" := do
  let db := seedDb
  let pred := Predicate.and [
    Predicate.contains (PredExpr.var "name") (PredExpr.str "li"),
    Predicate.startsWith (PredExpr.var "name") (PredExpr.str "Al"),
    Predicate.endsWith (PredExpr.var "name") (PredExpr.str "ce")
  ]
  let result := DSL.query
    |>.find "name"
    |>.where_ "e" ":person/name" "name"
    |>.wherePred pred
    |>.run db
  result.size ≡ 1
  match result.rows.bindings with
  | [b] =>
    match b.lookup ⟨"name"⟩ with
    | some (.value (.string "Alice")) => pure ()
    | _ => throw <| IO.userError "Expected Alice"
  | _ => throw <| IO.userError "Expected one result"

test "Predicate: boolean logic or/not" := do
  let db := seedDb
  let pred := Predicate.and [
    Predicate.or [
      Predicate.eq (PredExpr.var "status") (PredExpr.str "active"),
      Predicate.eq (PredExpr.var "name") (PredExpr.str "Bob")
    ],
    Predicate.not (Predicate.eq (PredExpr.var "name") (PredExpr.str "Charlie"))
  ]
  let result := DSL.query
    |>.find "name"
    |>.where_ "e" ":person/name" "name"
    |>.where_ "e" ":person/status" "status"
    |>.wherePred pred
    |>.run db
  result.size ≡ 2

test "Predicate: unbound variable filters out" := do
  let db := seedDb
  let pred := Predicate.gt (PredExpr.var "missing") (PredExpr.int 0)
  let result := DSL.query
    |>.find "e"
    |>.wherePred pred
    |>.run db
  result.size ≡ 0

test "Predicate: type mismatch yields false" := do
  let db := seedDb
  let pred := Predicate.eq (PredExpr.var "name") (PredExpr.int 30)
  let result := DSL.query
    |>.find "name"
    |>.where_ "e" ":person/name" "name"
    |>.wherePred pred
    |>.run db
  result.size ≡ 0

end Ledger.Tests.Predicate
