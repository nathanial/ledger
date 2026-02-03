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

/-! ## Negation Tests -/

test "Query: negation excludes matching entities" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add charlie (Attribute.mk ":person/name") (Value.string "Charlie"),
    .add alice (Attribute.mk ":person/role") (Value.string "manager"),
    .add bob (Attribute.mk ":person/role") (Value.string "manager")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  -- Find people who are NOT managers
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"name"⟩
      },
      .not (.pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/role")
        value := .value (Value.string "manager")
      })
    ]
  }
  let result := Query.execute query db
  result.size ≡ 1  -- Only Charlie

test "Query: negation with no matches keeps all" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob")
    -- No one has :person/role
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  -- Find people who are NOT managers (everyone should match)
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"name"⟩
      },
      .not (.pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/role")
        value := .value (Value.string "manager")
      })
    ]
  }
  let result := Query.execute query db
  result.size ≡ 2  -- Both Alice and Bob

test "Query: negation with all matches removes all" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add alice (Attribute.mk ":person/role") (Value.string "manager"),
    .add bob (Attribute.mk ":person/role") (Value.string "manager")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  -- Find people who are NOT managers (no one should match)
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"name"⟩
      },
      .not (.pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/role")
        value := .value (Value.string "manager")
      })
    ]
  }
  let result := Query.execute query db
  result.size ≡ 0  -- No one passes

test "Query: negation with different attribute" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/active") (Value.bool true),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add bob (Attribute.mk ":person/active") (Value.bool false)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  -- Find people who are NOT active=false (Alice)
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"name"⟩
      },
      .not (.pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/active")
        value := .value (Value.bool false)
      })
    ]
  }
  let result := Query.execute query db
  result.size ≡ 1  -- Only Alice

/-! ## AND / OR / BLANK / DUPLICATE Coverage -/

test "Query: and clause chains patterns" := do
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
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .and [
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
    ]
  }
  let result := Query.execute query db
  result.size ≡ 1

test "Query: or clause unions branches" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add bob (Attribute.mk ":person/nickname") (Value.string "Bobby")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .or [
        .pattern {
          entity := .var ⟨"e"⟩
          attr := .attr (Attribute.mk ":person/name")
          value := .var ⟨"name"⟩
        },
        .pattern {
          entity := .var ⟨"e"⟩
          attr := .attr (Attribute.mk ":person/nickname")
          value := .var ⟨"name"⟩
        }
      ]
    ]
  }
  let result := Query.execute query db
  result.size ≡ 2

test "Query: nested or/and clauses" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30),
    .add alice (Attribute.mk ":person/dept") (Value.string "Engineering"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add bob (Attribute.mk ":person/age") (Value.int 25),
    .add bob (Attribute.mk ":person/dept") (Value.string "Sales"),
    .add charlie (Attribute.mk ":person/name") (Value.string "Charlie"),
    .add charlie (Attribute.mk ":person/age") (Value.int 30),
    .add charlie (Attribute.mk ":person/dept") (Value.string "Sales")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .or [
        .and [
          .pattern {
            entity := .var ⟨"e"⟩
            attr := .attr (Attribute.mk ":person/age")
            value := .value (Value.int 30)
          },
          .pattern {
            entity := .var ⟨"e"⟩
            attr := .attr (Attribute.mk ":person/dept")
            value := .value (Value.string "Engineering")
          },
          .pattern {
            entity := .var ⟨"e"⟩
            attr := .attr (Attribute.mk ":person/name")
            value := .var ⟨"name"⟩
          }
        ],
        .and [
          .pattern {
            entity := .var ⟨"e"⟩
            attr := .attr (Attribute.mk ":person/age")
            value := .value (Value.int 25)
          },
          .pattern {
            entity := .var ⟨"e"⟩
            attr := .attr (Attribute.mk ":person/dept")
            value := .value (Value.string "Sales")
          },
          .pattern {
            entity := .var ⟨"e"⟩
            attr := .attr (Attribute.mk ":person/name")
            value := .var ⟨"name"⟩
          }
        ]
      ]
    ]
  }
  let result := Query.execute query db
  result.size ≡ 2  -- Alice and Bob

test "Query: blank term matches without binding" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/tag") (Value.string "one"),
    .add alice (Attribute.mk ":person/tag") (Value.string "two")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"e"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/tag")
        value := .blank
      }
    ]
  }
  let result := Query.execute query db
  result.size ≡ 1

test "Query: repeated variable in single pattern" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/friend") (Value.ref alice),
    .add bob (Attribute.mk ":person/friend") (Value.ref alice)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"e"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/friend")
        value := .var ⟨"e"⟩
      }
    ]
  }
  let result := Query.execute query db
  result.size ≡ 1

test "Query: attribute variables bind and return attrs" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"a"⟩, ⟨"v"⟩]
    where_ := [
      .pattern {
        entity := .entity alice
        attr := .var ⟨"a"⟩
        value := .var ⟨"v"⟩
      }
    ]
  }
  let result := Query.execute query db
  result.size ≡ 2
  let hasName := result.rows.bindings.any fun b =>
    match b.lookup ⟨"a"⟩, b.lookup ⟨"v"⟩ with
    | some (.attr a), some (.value (.string "Alice")) => a.name == ":person/name"
    | _, _ => false
  let hasAge := result.rows.bindings.any fun b =>
    match b.lookup ⟨"a"⟩, b.lookup ⟨"v"⟩ with
    | some (.attr a), some (.value (.int 30)) => a.name == ":person/age"
    | _, _ => false
  ensure hasName "Expected name attribute binding"
  ensure hasAge "Expected age attribute binding"

test "Query: value term entity matches refs" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let (charlie, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/friend") (Value.ref bob),
    .add charlie (Attribute.mk ":person/friend") (Value.ref alice)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let pattern : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr (Attribute.mk ":person/friend")
    value := .entity bob
  }
  let result := Query.findEntities pattern db
  result.length ≡ 1

test "Query: projection distinct removes duplicates" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Sam"),
    .add bob (Attribute.mk ":person/name") (Value.string "Sam")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"name"⟩
      }
    ]
  }
  let result := Query.execute query db
  result.size ≡ 1

test "Query: or branch duplicates are distinct" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Sam"),
    .add alice (Attribute.mk ":person/nickname") (Value.string "Sam")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .or [
        .pattern {
          entity := .var ⟨"e"⟩
          attr := .attr (Attribute.mk ":person/name")
          value := .var ⟨"name"⟩
        },
        .pattern {
          entity := .var ⟨"e"⟩
          attr := .attr (Attribute.mk ":person/nickname")
          value := .var ⟨"name"⟩
        }
      ]
    ]
  }
  let result := Query.execute query db
  result.size ≡ 1

test "Query: retracted values are not returned" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx1 : Transaction := [
    .add alice (Attribute.mk ":person/status") (Value.string "active")
  ]
  let .ok (db, _) := db.transact tx1 | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .retract alice (Attribute.mk ":person/status") (Value.string "active")
  ]
  let .ok (db, _) := db.transact tx2 | throw <| IO.userError "Tx2 failed"
  let query : Query := {
    find := [⟨"status"⟩]
    where_ := [
      .pattern {
        entity := .entity alice
        attr := .attr (Attribute.mk ":person/status")
        value := .var ⟨"status"⟩
      }
    ]
  }
  let result := Query.execute query db
  result.size ≡ 0

/-! ## Binding Conflicts and Coercions -/

test "Query: conflicting bindings yield no results" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/age") (Value.int 30)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"e"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .value (Value.string "Alice")
      },
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .value (Value.string "Bob")
      }
    ]
  }
  let result := Query.execute query db
  result.size ≡ 0

test "Query: type-mismatched variable reuse fails" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"x"⟩]
    where_ := [
      .pattern {
        entity := .entity alice
        attr := .var ⟨"x"⟩
        value := .value (Value.string "Alice")
      },
      .pattern {
        entity := .entity alice
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"x"⟩
      }
    ]
  }
  let result := Query.execute query db
  result.size ≡ 0

test "Query: value ref variable used as entity" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/friend") (Value.ref bob),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .pattern {
        entity := .entity alice
        attr := .attr (Attribute.mk ":person/friend")
        value := .var ⟨"f"⟩
      },
      .pattern {
        entity := .var ⟨"f"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"name"⟩
      }
    ]
  }
  let result := Query.execute query db
  result.size ≡ 1

test "Query: entity variable used as value (ref match)" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/friend") (Value.ref bob),
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"e"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .value (Value.string "Bob")
      },
      .pattern {
        entity := .entity alice
        attr := .attr (Attribute.mk ":person/friend")
        value := .var ⟨"e"⟩
      }
    ]
  }
  let result := Query.execute query db
  match result.rows.bindings with
  | [b] =>
    match b.lookup ⟨"e"⟩ with
    | some (.entity e) => ensure (e == bob) "Expected bob entity binding"
    | some (.value (.ref e)) => ensure (e == bob) "Expected bob ref binding"
    | _ => throw <| IO.userError "Expected entity binding for bob"
  | _ => throw <| IO.userError s!"Expected one result, got {result.size}"

/-! ## OR with Shared Bindings -/

test "Query: or branch respects shared bindings" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
    .add alice (Attribute.mk ":person/dept") (Value.string "Sales"),
    .add bob (Attribute.mk ":person/name") (Value.string "Bob"),
    .add bob (Attribute.mk ":person/dept") (Value.string "Engineering")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"name"⟩
      },
      .or [
        .pattern {
          entity := .var ⟨"e"⟩
          attr := .attr (Attribute.mk ":person/dept")
          value := .value (Value.string "Sales")
        },
        .pattern {
          entity := .var ⟨"e"⟩
          attr := .attr (Attribute.mk ":person/dept")
          value := .value (Value.string "Engineering")
        }
      ]
    ]
  }
  let result := Query.execute query db
  result.size ≡ 2

test "Query: or drops branch-only vars" := do
  let db := Db.empty
  let (sam, db) := db.allocEntityId
  let tx : Transaction := [
    .add sam (Attribute.mk ":person/name") (Value.string "Sam"),
    .add sam (Attribute.mk ":person/nickname") (Value.string "Sam"),
    .add sam (Attribute.mk ":person/tag") (Value.string "vip")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .or [
        .pattern {
          entity := .var ⟨"e"⟩
          attr := .attr (Attribute.mk ":person/name")
          value := .var ⟨"name"⟩
        },
        .and [
          .pattern {
            entity := .var ⟨"e"⟩
            attr := .attr (Attribute.mk ":person/nickname")
            value := .var ⟨"name"⟩
          },
          .pattern {
            entity := .var ⟨"e"⟩
            attr := .attr (Attribute.mk ":person/tag")
            value := .var ⟨"tag"⟩
          }
        ]
      ]
    ]
  }
  let rel := Query.executeForAggregate query db
  ensure (rel.bindings.all fun b => (b.lookup ⟨"tag"⟩).isNone) "Expected tag to be out of scope"

test "Query: or dedups branch-local multiplicity" := do
  let db := Db.empty
  let (sam, db) := db.allocEntityId
  let tx : Transaction := [
    .add sam (Attribute.mk ":person/name") (Value.string "Sam"),
    .add sam (Attribute.mk ":person/nickname") (Value.string "Sam"),
    .add sam (Attribute.mk ":person/tag") (Value.string "vip"),
    .add sam (Attribute.mk ":person/tag") (Value.string "new")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"name"⟩]
    where_ := [
      .or [
        .pattern {
          entity := .var ⟨"e"⟩
          attr := .attr (Attribute.mk ":person/name")
          value := .var ⟨"name"⟩
        },
        .and [
          .pattern {
            entity := .var ⟨"e"⟩
            attr := .attr (Attribute.mk ":person/nickname")
            value := .var ⟨"name"⟩
          },
          .pattern {
            entity := .var ⟨"e"⟩
            attr := .attr (Attribute.mk ":person/tag")
            value := .var ⟨"tag"⟩
          }
        ]
      ]
    ]
  }
  let rel := Query.executeForAggregate query db
  rel.size ≡ 1

/-! ## Negation with Unbound Vars -/

test "Query: negation with unbound vars yields empty" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"e"⟩]
    where_ := [
      .not (.pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .value (Value.string "Alice")
      })
    ]
  }
  let result := Query.execute query db
  result.size ≡ 0

/-! ## VAET Selection and Validation -/

test "Query: value-only ref pattern uses VAET path" := do
  let db := Db.empty
  let (alice, db) := db.allocEntityId
  let (bob, db) := db.allocEntityId
  let tx : Transaction := [
    .add bob (Attribute.mk ":person/friend") (Value.ref alice)
  ]
  let .ok (db, _) := db.transact tx | throw <| IO.userError "Tx failed"
  let pattern : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .var ⟨"a"⟩
    value := .value (Value.ref alice)
  }
  let result := Query.executePattern pattern Binding.empty db.indexes
  result.size ≡ 1

test "Query: isValid checks find vars appear in where" := do
  let okQuery : Query := {
    find := [⟨"e"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"name"⟩
      }
    ]
  }
  let badQuery : Query := {
    find := [⟨"missing"⟩]
    where_ := [
      .pattern {
        entity := .var ⟨"e"⟩
        attr := .attr (Attribute.mk ":person/name")
        value := .var ⟨"name"⟩
      }
    ]
  }
  Query.isValid okQuery ≡ true
  Query.isValid badQuery ≡ false

end Ledger.Tests.Query
