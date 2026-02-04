/-
  Ledger.Tests.DSL - DSL builder tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.DSL

open Crucible
open Ledger

testSuite "DSL Builders"

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

test "DSL: entitiesWithAttrValueStr" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  (DSL.entitiesWithAttrValueStr db ":person/name" "Alice").length ≡ 1

test "DSL: entityWithAttrValueStr" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  DSL.entityWithAttrValueStr db ":person/name" "Alice" ≡ some alice

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

test "DSL: entitiesWithAttr" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let txb := DSL.tx
    |>.addStr alice ":person/name" "Alice"
    |>.addStr bob ":person/name" "Bob"
  let .ok (db, _) := txb.run db | throw <| IO.userError "TxBuilder failed"
  let withName := DSL.entitiesWithAttr db ":person/name"
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

test "DSL: withNewEntity propagates error" := do
  let schema := DSL.schema
    |>.string ":person/name"
    |>.build
  let db := Db.empty.withSchema schema true
  let result := DSL.withNewEntity db fun e tb =>
    tb.addInt e ":person/age" 30
  match result with
  | .error (.schemaViolation _) => pure ()
  | .error _ => throw <| IO.userError "Expected schemaViolation"
  | .ok _ => throw <| IO.userError "Expected error from withNewEntity"

end Ledger.Tests.DSL
