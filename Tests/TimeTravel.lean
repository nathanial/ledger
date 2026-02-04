/-
  Ledger.Tests.TimeTravel - Time-travel edge case tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.TimeTravel

open Crucible
open Ledger

testSuite "Time Travel Edge Cases"

test "asOf on empty history yields empty db" := do
  let conn := Connection.create
  let db := conn.asOf TxId.genesis
  db.size ≡ 0
  db.basisT.id ≡ 0

test "since on empty history yields empty list" := do
  let conn := Connection.create
  let changes := conn.since TxId.genesis
  changes.length ≡ 0

test "entityHistory on empty history yields empty" := do
  let conn := Connection.create
  let history := conn.entityHistory ⟨1⟩
  history.length ≡ 0

test "attrHistory on empty history yields empty" := do
  let conn := Connection.create
  let history := conn.attrHistory ⟨1⟩ (Attribute.mk ":person/name")
  history.length ≡ 0

test "single transaction time-travel consistency" := do
  let conn := Connection.create
  let (alice, conn) := conn.allocEntityId
  let tx : Transaction := [
    .add alice (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (conn, report) := conn.transact tx | throw <| IO.userError "Tx failed"
  let dbAt := conn.asOf report.txId
  dbAt.getOne alice (Attribute.mk ":person/name") ≡ some (Value.string "Alice")
  let changes := conn.since TxId.genesis
  changes.length ≡ 1
  let history := conn.entityHistory alice
  history.length ≡ 1

end Ledger.Tests.TimeTravel
