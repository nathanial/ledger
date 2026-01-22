/-
  Ledger.Tests.RangeQuery - Tests for range query optimization
-/

import Crucible
import Ledger
import Ledger.Index.RBRange

namespace Ledger.Tests.RangeQuery

open Crucible
open Ledger

testSuite "Range Query Correctness"

/-! ## EAVT Index Range Queries -/

test "datomsForEntity finds correct entity" := do
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
  let aliceDatoms := db.indexes.eavt.datomsForEntity alice
  let bobDatoms := db.indexes.eavt.datomsForEntity bob
  aliceDatoms.length ≡ 2
  bobDatoms.length ≡ 2

test "datomsForEntity returns empty for nonexistent entity" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let nonexistent := EntityId.mk 999
  let datoms := db.indexes.eavt.datomsForEntity nonexistent
  datoms.length ≡ 0

test "datomsForEntityAttr filters correctly" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add alice (Attribute.mk ":person/email") (Value.string "alice@test.com")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let nameDatoms := db.indexes.eavt.datomsForEntityAttr alice (Attribute.mk ":person/name")
  let ageDatoms := db.indexes.eavt.datomsForEntityAttr alice (Attribute.mk ":person/age")
  nameDatoms.length ≡ 1
  ageDatoms.length ≡ 1

/-! ## AEVT Index Range Queries -/

test "datomsForAttr finds all entities with attribute" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add charlie (Attribute.mk ":person/age") (Value.int 40)  -- no name
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let nameDatoms := db.indexes.aevt.datomsForAttr (Attribute.mk ":person/name")
  nameDatoms.length ≡ 2

test "datomsForAttrEntity filters correctly" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let aliceName := db.indexes.aevt.datomsForAttrEntity (Attribute.mk ":person/name") alice
  let bobName := db.indexes.aevt.datomsForAttrEntity (Attribute.mk ":person/name") bob
  aliceName.length ≡ 1
  bobName.length ≡ 1

/-! ## AVET Index Range Queries -/

test "datomsForAttrValue finds matching entities" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/dept") (Value.string "Engineering"),
    .add bob (Attribute.mk ":person/dept") (Value.string "Engineering"),
    .add charlie (Attribute.mk ":person/dept") (Value.string "Sales")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let engDatoms := db.indexes.avet.datomsForAttrValue
    (Attribute.mk ":person/dept") (Value.string "Engineering")
  let salesDatoms := db.indexes.avet.datomsForAttrValue
    (Attribute.mk ":person/dept") (Value.string "Sales")
  engDatoms.length ≡ 2
  salesDatoms.length ≡ 1

test "entitiesWithAttrValue handles retractions" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/status") (Value.string "active"),
    .add bob (Attribute.mk ":person/status") (Value.string "active")
  ]
  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/status") (Value.string "active")
  ]
  let .ok (db, _) := db.transact tx2 | throw <| IO.userError "Tx2 failed"
  let activeEntities := db.indexes.avet.entitiesWithAttrValue
    (Attribute.mk ":person/status") (Value.string "active")
  activeEntities.length ≡ 1

/-! ## VAET Index Range Queries -/

test "datomsReferencingEntity finds references" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add bob (Attribute.mk ":person/friend") (Value.ref alice),
    .add charlie (Attribute.mk ":person/friend") (Value.ref alice)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let refs := db.indexes.vaet.datomsReferencingEntity alice
  refs.length ≡ 2

test "datomsReferencingViaAttr filters by attribute" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add bob (Attribute.mk ":person/friend") (Value.ref alice),
    .add charlie (Attribute.mk ":person/manager") (Value.ref alice)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let friendRefs := db.indexes.vaet.datomsReferencingViaAttr alice (Attribute.mk ":person/friend")
  let managerRefs := db.indexes.vaet.datomsReferencingViaAttr alice (Attribute.mk ":person/manager")
  friendRefs.length ≡ 1
  managerRefs.length ≡ 1

/-! ## Ordering Tests -/

test "datomsForEntity preserves EAVT order" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/z-attr") (Value.string "last"),
    .add alice (Attribute.mk ":person/a-attr") (Value.string "first")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let datoms := db.indexes.eavt.datomsForEntity alice
  -- Should be sorted by attribute, so a-attr comes before z-attr
  match datoms with
  | [d1, d2] => do
    ensure (d1.attr.name < d2.attr.name) "Should be sorted by attribute"
  | _ => throw <| IO.userError "Expected 2 datoms"

/-! ## Boundary Tests -/

test "Range query with first entity in db" := do
  let db := Db.empty
  let (first, db) := db.allocEntityId
  let (_, db) := db.allocEntityId  -- second
  let (_, db) := db.allocEntityId  -- third
  let tx : Transaction := [
    .add first (Attribute.mk ":person/name") (Value.string "First")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let datoms := db.indexes.eavt.datomsForEntity first
  datoms.length ≡ 1

test "Range query with last entity in db" := do
  let db := Db.empty
  let (_, db) := db.allocEntityId  -- first
  let (_, db) := db.allocEntityId  -- second
  let (last, db) := db.allocEntityId
  let tx : Transaction := [
    .add last (Attribute.mk ":person/name") (Value.string "Last")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let datoms := db.indexes.eavt.datomsForEntity last
  datoms.length ≡ 1

end Ledger.Tests.RangeQuery
