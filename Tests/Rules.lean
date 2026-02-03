/-
  Ledger.Tests.Rules - Datalog rule and recursion tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.Rules

open Crucible
open Ledger

testSuite "Rules"

private def ancestorRules : List RuleDef :=
  let parentAttr := Attribute.mk ":person/parent"
  let ruleBase : RuleDef := {
    name := "ancestor"
    params := [⟨"x"⟩, ⟨"y"⟩]
    body := [
      .pattern {
        entity := .var ⟨"x"⟩
        attr := .attr parentAttr
        value := .var ⟨"y"⟩
      }
    ]
  }
  let ruleRec : RuleDef := {
    name := "ancestor"
    params := [⟨"x"⟩, ⟨"y"⟩]
    body := [
      .pattern {
        entity := .var ⟨"x"⟩
        attr := .attr parentAttr
        value := .var ⟨"z"⟩
      },
      .rule {
        name := "ancestor"
        args := [.var ⟨"z"⟩, .var ⟨"y"⟩]
      }
    ]
  }
  [ruleBase, ruleRec]

private def extractPairs (result : Query.QueryResult) : List (EntityId × EntityId) :=
  result.rows.bindings.filterMap fun b =>
    match b.lookup ⟨"x"⟩ >>= BoundValue.asEntity?, b.lookup ⟨"y"⟩ >>= BoundValue.asEntity? with
    | some ex, some ey => some (ex, ey)
    | _, _ => none

test "Rule: recursive ancestor derives transitive closure" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let (bob, conn) := conn.allocEntityId
  let (carol, conn) := conn.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/parent") (.ref bob),
    .add bob (Attribute.mk ":person/parent") (.ref carol)
  ]
  let .ok (conn, _) := conn.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"x"⟩, ⟨"y"⟩]
    where_ := [
      .rule { name := "ancestor", args := [.var ⟨"x"⟩, .var ⟨"y"⟩] }
    ]
    rules := ancestorRules
  }
  let result := Query.execute query conn.db
  let pairs := extractPairs result
  ensure (pairs.contains (alice, bob)) "Alice should be ancestor of Bob"
  ensure (pairs.contains (bob, carol)) "Bob should be ancestor of Carol"
  ensure (pairs.contains (alice, carol)) "Alice should be ancestor of Carol"
  pairs.length ≡ 3

test "Rule: call with constant arg filters results" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let (bob, conn) := conn.allocEntityId
  let (carol, conn) := conn.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/parent") (.ref bob),
    .add bob (Attribute.mk ":person/parent") (.ref carol)
  ]
  let .ok (conn, _) := conn.transact tx | throw <| IO.userError "Tx failed"
  let query : Query := {
    find := [⟨"x"⟩]
    where_ := [
      .rule { name := "ancestor", args := [.var ⟨"x"⟩, .entity carol] }
    ]
    rules := ancestorRules
  }
  let result := Query.execute query conn.db
  let ancestors := result.rows.bindings.filterMap fun b =>
    b.lookup ⟨"x"⟩ >>= BoundValue.asEntity?
  ensure (ancestors.contains alice) "Alice should be ancestor of Carol"
  ensure (ancestors.contains bob) "Bob should be ancestor of Carol"
  ancestors.length ≡ 2

end Ledger.Tests.Rules
