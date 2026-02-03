/-
  Ledger.Tests.Macros - Query macro DSL tests
-/

import Crucible
import Ledger
import Ledger.DSL.Macros

namespace Ledger.Tests.Macros

open Crucible
open Ledger
open Ledger.DSL

testSuite "Query Macros"

test "Macro query: patterns and predicates" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let (bob, conn) := conn.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (.string "Alice"),
    .add alice (Attribute.mk ":person/age") (.int 30),
    .add bob (Attribute.mk ":person/name") (.string "Bob"),
    .add bob (Attribute.mk ":person/age") (.int 19)
  ]
  let .ok (conn, _) := conn.transact tx | throw <| IO.userError "Tx failed"
  let query := query! {
    :find ?e
    :where
      [?e :person/age ?age]
      [(> ?age 21)]
  }
  let result := Query.execute query conn.db
  result.rows.size ≡ 1

test "Macro query: recursive rules" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let (bob, conn) := conn.allocEntityId
  let (carol, conn) := conn.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/parent") (.ref bob),
    .add bob (Attribute.mk ":person/parent") (.ref carol)
  ]
  let .ok (conn, _) := conn.transact tx | throw <| IO.userError "Tx failed"
  let query := query! {
    :find ?x ?y
    :where
      (ancestor ?x ?y)
    :rules
      (rule ancestor [?x ?y]
        [?x :person/parent ?y])
      (rule ancestor [?x ?y]
        [?x :person/parent ?z]
        (ancestor ?z ?y))
  }
  let result := Query.execute query conn.db
  result.rows.size ≡ 3

end Ledger.Tests.Macros
