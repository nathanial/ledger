/-
  Ledger.Tests.Performance - Performance and scaling tests

  These tests exercise the database at scale to expose O(n²) bottlenecks.
-/

import Crucible
import Ledger

namespace Ledger.Tests.Performance

open Crucible
open Ledger

/-! ## Timing Utilities -/

def timeMs (action : IO α) : IO (α × Nat) := do
  let start ← IO.monoMsNow
  let result ← action
  let elapsed := (← IO.monoMsNow) - start
  return (result, elapsed)

/-! ## Data Generation -/

def personName : Attribute := ⟨":person/name"⟩
def personAge : Attribute := ⟨":person/age"⟩
def personDept : Attribute := ⟨":person/department"⟩
def personManager : Attribute := ⟨":person/manager"⟩

/-- Create N entities with name and age -/
def createPeople (n : Nat) : IO (Db × Array EntityId) := do
  let mut db := Db.empty
  let mut entities : Array EntityId := #[]
  for i in [:n] do
    let (eid, db') := db.allocEntityId
    entities := entities.push eid
    let tx : Transaction := [
      .add eid personName (.string s!"Person{i}"),
      .add eid personAge (.int (Int.ofNat i))
    ]
    match db'.transact tx with
    | .ok (db'', _) => db := db''
    | .error e => throw <| IO.userError s!"Transaction failed: {e}"
  return (db, entities)

/-- Create N entities all in same department -/
def createPeopleInDept (n : Nat) (dept : String) : IO (Db × Array EntityId) := do
  let mut db := Db.empty
  let mut entities : Array EntityId := #[]
  for i in [:n] do
    let (eid, db') := db.allocEntityId
    entities := entities.push eid
    let tx : Transaction := [
      .add eid personName (.string s!"Person{i}"),
      .add eid personDept (.string dept)
    ]
    match db'.transact tx with
    | .ok (db'', _) => db := db''
    | .error e => throw <| IO.userError s!"Transaction failed: {e}"
  return (db, entities)

/-- Create N references to a target entity -/
def createRefsTo (db : Db) (target : EntityId) (n : Nat) : IO Db := do
  let mut db := db
  for i in [:n] do
    let (eid, db') := db.allocEntityId
    let tx : Transaction := [
      .add eid personName (.string s!"Employee{i}"),
      .add eid personManager (.ref target)
    ]
    match db'.transact tx with
    | .ok (db'', _) => db := db''
    | .error e => throw <| IO.userError s!"Transaction failed: {e}"
  return db

/-- Add multiple values to same attribute (simulates moves without retraction) -/
def addManyValues (db : Db) (eid : EntityId) (attr : Attribute) (n : Nat) : IO Db := do
  let mut db := db
  for i in [:n] do
    let tx : Transaction := [.add eid attr (.int (Int.ofNat i))]
    match db.transact tx with
    | .ok (db', _) => db := db'
    | .error e => throw <| IO.userError s!"Transaction failed: {e}"
  return db

testSuite "Performance Tests"

/-! ## Transaction Performance -/

test "Insert 100 entities" := do
  let (_, elapsed) ← timeMs (createPeople 100)
  IO.println s!"  100 entities: {elapsed}ms"
  ensure (elapsed < 2000) s!"Too slow: {elapsed}ms (expected < 2000ms)"

test "Insert 1000 entities" := do
  let (_, elapsed) ← timeMs (createPeople 1000)
  IO.println s!"  1000 entities: {elapsed}ms"
  ensure (elapsed < 20000) s!"Too slow: {elapsed}ms (expected < 20000ms)"

test "Insert 5000 entities" := do
  let (_, elapsed) ← timeMs (createPeople 5000)
  IO.println s!"  5000 entities: {elapsed}ms"
  ensure (elapsed < 120000) s!"Too slow: {elapsed}ms (expected < 120000ms)"

/-! ## Entity Lookup Performance (EAVT) -/

test "getOne with 100 entities" := do
  let (db, entities) ← createPeople 100
  let target := entities[50]!
  let (_, elapsed) ← timeMs do
    for _ in [:100] do
      let _ := db.getOne target personName
    pure ()
  IO.println s!"  100 lookups in 100-entity db: {elapsed}ms"
  ensure (elapsed < 200) s!"Too slow: {elapsed}ms"

test "getOne with 1000 entities" := do
  let (db, entities) ← createPeople 1000
  let target := entities[500]!
  let (_, elapsed) ← timeMs do
    for _ in [:100] do
      let _ := db.getOne target personName
    pure ()
  IO.println s!"  100 lookups in 1000-entity db: {elapsed}ms"
  ensure (elapsed < 1000) s!"Too slow: {elapsed}ms"

test "getOne scaling comparison" := do
  -- Create databases of different sizes
  let (db500, e500) ← createPeople 500
  let (db2000, e2000) ← createPeople 2000
  let (db5000, e5000) ← createPeople 5000

  -- Measure lookup time in each (more iterations for measurable times)
  let (_, t500) ← timeMs do
    for _ in [:1000] do
      let _ := db500.getOne e500[250]! personName
  let (_, t2000) ← timeMs do
    for _ in [:1000] do
      let _ := db2000.getOne e2000[1000]! personName
  let (_, t5000) ← timeMs do
    for _ in [:1000] do
      let _ := db5000.getOne e5000[2500]! personName

  IO.println s!"  500 entities, 1000 lookups: {t500}ms"
  IO.println s!"  2000 entities, 1000 lookups: {t2000}ms"
  IO.println s!"  5000 entities, 1000 lookups: {t5000}ms"

  -- Check for super-linear scaling
  -- With O(n) complexity, 10x more entities = 10x slower
  -- With O(n²) complexity, 10x more entities = 100x slower
  let ratio := if t500 <= 1 then t5000.toFloat else t5000.toFloat / t500.toFloat
  IO.println s!"  Scaling ratio (5000/500): {ratio}x (linear would be 10x)"
  -- Allow up to 50x for now - will fail if O(n²)
  ensure (ratio < 100) s!"Scaling too steep: {ratio}x suggests O(n²)"

/-! ## Attribute Query Performance (AEVT) -/

test "entitiesWithAttr at scale" := do
  let (db, _) ← createPeople 5000
  let (_, elapsed) ← timeMs do
    for _ in [:100] do
      let _ := db.entitiesWithAttr personName
  IO.println s!"  100 entitiesWithAttr calls in 5000-entity db: {elapsed}ms"
  ensure (elapsed < 10000) s!"Too slow: {elapsed}ms"

/-! ## Value Lookup Performance (AVET) -/

test "entitiesWithAttrValue at scale" := do
  let (db, _) ← createPeopleInDept 5000 "Engineering"
  let (_, elapsed) ← timeMs do
    for _ in [:100] do
      let _ := db.entitiesWithAttrValue personDept (.string "Engineering")
  IO.println s!"  100 entitiesWithAttrValue in 5000-entity db: {elapsed}ms"
  ensure (elapsed < 30000) s!"Too slow: {elapsed}ms"

/-! ## Reverse Reference Performance (VAET) -/

test "referencingEntities at scale" := do
  let db := Db.empty
  let (manager, db) := db.allocEntityId
  let db ← createRefsTo db manager 2000

  let (_, elapsed) ← timeMs do
    for _ in [:100] do
      let _ := db.referencingEntities manager
  IO.println s!"  100 referencingEntities (2000 refs): {elapsed}ms"
  ensure (elapsed < 10000) s!"Too slow: {elapsed}ms"

/-! ## Multi-Value Performance (simulates move-without-retraction bug) -/

test "getOne with many values (move bug pattern)" := do
  let db := Db.empty
  let (eid, db) := db.allocEntityId
  let moveAttr : Attribute := ⟨":card/column"⟩
  -- Simulate 200 moves without retraction (exaggerated to expose O(n²))
  let db ← addManyValues db eid moveAttr 200

  let (_, elapsed) ← timeMs do
    for _ in [:500] do
      let _ := db.getOne eid moveAttr
  IO.println s!"  500 getOne calls on entity with 200 values: {elapsed}ms"
  ensure (elapsed < 5000) s!"Too slow: {elapsed}ms (O(n²) grouping?)"

/-! ## Pull Performance -/

test "pull at scale" := do
  let (db, entities) ← createPeople 2000
  let spec : PullSpec := [.attr personName, .attr personAge]

  let (_, elapsed) ← timeMs do
    for eid in entities do
      let _ := Pull.pull db eid spec
  IO.println s!"  Pull 2000 entities: {elapsed}ms"
  ensure (elapsed < 20000) s!"Too slow: {elapsed}ms"

/-! ## Query Performance -/

test "simple query at scale" := do
  let (db, _) ← createPeopleInDept 3000 "Sales"

  let pattern : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr personDept
    value := .value (.string "Sales")
  }

  let (_, elapsed) ← timeMs do
    for _ in [:50] do
      let _ := Query.findEntities pattern db
  IO.println s!"  50 queries matching 3000 entities: {elapsed}ms"
  ensure (elapsed < 30000) s!"Too slow: {elapsed}ms"

test "join query at scale" := do
  let db := Db.empty
  let (mgr1, db) := db.allocEntityId
  let (mgr2, db) := db.allocEntityId
  let (mgr3, db) := db.allocEntityId
  let (mgr4, db) := db.allocEntityId

  let tx : Transaction := [
    .add mgr1 personName (.string "Manager1"),
    .add mgr2 personName (.string "Manager2"),
    .add mgr3 personName (.string "Manager3"),
    .add mgr4 personName (.string "Manager4")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"

  -- Create 1000 employees split between 4 managers
  let mut db := db
  for i in [:1000] do
    let (eid, db') := db.allocEntityId
    let mgr := match i % 4 with
      | 0 => mgr1
      | 1 => mgr2
      | 2 => mgr3
      | _ => mgr4
    let tx : Transaction := [
      .add eid personName (.string s!"Employee{i}"),
      .add eid personManager (.ref mgr)
    ]
    match db'.transact tx with
    | .ok (db'', _) => db := db''
    | .error e => throw <| IO.userError s!"Tx failed: {e}"

  let query : Query := {
    find := [⟨"e"⟩, ⟨"mname"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr personManager
        value := .var ⟨"m"⟩
      },
      .pattern {
        entity := .var ⟨"m"⟩
        attr := .attr personName
        value := .var ⟨"mname"⟩
      }
    ]
  }

  let (_, elapsed) ← timeMs do
    for _ in [:20] do
      let _ := Query.execute query db
  IO.println s!"  20 join queries (1000 employees, 4 managers): {elapsed}ms"
  ensure (elapsed < 60000) s!"Too slow: {elapsed}ms (O(n*m) join?)"

/-! ## Stress Tests (larger scale to expose O(n²)) -/

test "STRESS: Insert 10000 entities" := do
  let (_, elapsed) ← timeMs (createPeople 10000)
  IO.println s!"  10000 entities: {elapsed}ms"
  ensure (elapsed < 300000) s!"Too slow: {elapsed}ms"

test "STRESS: getOne scaling 10000 entities" := do
  let (db, entities) ← createPeople 10000
  let target := entities[5000]!

  let (_, elapsed) ← timeMs do
    for _ in [:5000] do
      let _ := db.getOne target personName
  IO.println s!"  5000 getOne calls in 10000-entity db: {elapsed}ms"
  ensure (elapsed < 30000) s!"Too slow: {elapsed}ms"

test "STRESS: entitiesWithAttrValue 10000 entities" := do
  let (db, _) ← createPeopleInDept 10000 "Engineering"
  let (_, elapsed) ← timeMs do
    for _ in [:100] do
      let _ := db.entitiesWithAttrValue personDept (.string "Engineering")
  IO.println s!"  100 entitiesWithAttrValue in 10000-entity db: {elapsed}ms"
  -- This is where O(n*m) should really show up
  ensure (elapsed < 120000) s!"Too slow: {elapsed}ms"

test "STRESS: entity with 500 values (extreme move pattern)" := do
  let db := Db.empty
  let (eid, db) := db.allocEntityId
  let moveAttr : Attribute := ⟨":card/column"⟩
  -- Extreme case: 500 values without retraction
  let db ← addManyValues db eid moveAttr 500

  let (_, elapsed) ← timeMs do
    for _ in [:1000] do
      let _ := db.getOne eid moveAttr
  IO.println s!"  1000 getOne calls on entity with 500 values: {elapsed}ms"
  -- With O(n²) grouping, this should be noticeably slow
  ensure (elapsed < 30000) s!"Too slow: {elapsed}ms"

end Ledger.Tests.Performance
