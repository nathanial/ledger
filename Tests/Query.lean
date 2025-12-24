/-
  Ledger.Tests.Query - Datalog query tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.Query

open Crucible
open Ledger

testSuite "Datalog Queries"

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

#generate_tests

end Ledger.Tests.Query
