/-
  Ledger.Tests.Core - Core types tests (EntityId, TxId, Attribute, Value, Datom)
-/

import Crucible
import Ledger

namespace Ledger.Tests.Core

open Crucible
open Ledger

testSuite "Core Types"

/-! ## EntityId Tests -/

test "EntityId equality" := do
  let e1 := EntityId.mk 1
  ensure (e1 == e1) "EntityId should equal itself"

test "EntityId inequality" := do
  let e1 := EntityId.mk 1
  let e2 := EntityId.mk 2
  ensure (e1 != e2) "Different EntityIds should not be equal"

test "EntityId ordering" := do
  let e1 := EntityId.mk 1
  let e2 := EntityId.mk 2
  ensure (compare e1 e2 == .lt) "EntityId 1 should be less than 2"

test "EntityId temp check" := do
  ensure (EntityId.mk (-1) |>.isTemp) "Negative EntityId should be temp"

/-! ## TxId Tests -/

test "TxId next" := do
  let t1 := TxId.mk 1
  let t2 := t1.next
  t2.id ≡ 2

/-! ## Attribute Tests -/

test "Attribute equality" := do
  let a1 := Attribute.mk ":person/name"
  ensure (a1 == a1) "Attribute should equal itself"

test "Attribute keyword" := do
  let a2 := Attribute.keyword "person" "age"
  a2.name ≡ ":person/age"

/-! ## Value Tests -/

test "Value int equality" := do
  ensure (Value.int 42 == Value.int 42) "Same int values should be equal"

test "Value string equality" := do
  ensure (Value.string "hello" == Value.string "hello") "Same string values should be equal"

test "Value ordering same type" := do
  ensure (compare (Value.int 1) (Value.int 2) == .lt) "Int 1 should be less than 2"

test "Value ordering diff type" := do
  ensure (compare (Value.int 0) (Value.string "") == .lt) "Int should be less than string"

/-! ## Datom Tests -/

test "Datom added flag" := do
  let e := EntityId.mk 1
  let a := Attribute.mk ":person/name"
  let v := Value.string "Alice"
  let t := TxId.mk 1
  let d1 := Datom.assert e a v t
  ensure d1.added "Asserted datom should have added=true"

test "Datom retracted flag" := do
  let e := EntityId.mk 1
  let a := Attribute.mk ":person/name"
  let v := Value.string "Alice"
  let t := TxId.mk 1
  let d2 := Datom.retract e a v t
  ensure (!d2.added) "Retracted datom should have added=false"

test "Datom equality" := do
  let e := EntityId.mk 1
  let a := Attribute.mk ":person/name"
  let v := Value.string "Alice"
  let t := TxId.mk 1
  let d1 := Datom.assert e a v t
  ensure (d1.entity == e && d1.attr == a && d1.value == v) "Datom fields should match"

/-! ## DecidableEq Tests -/

test "Value DecidableEq - equal values" := do
  let v1 := Value.string "hello"
  let v2 := Value.string "hello"
  -- Using DecidableEq for pattern matching
  if v1 = v2 then pure () else throw <| IO.userError "Values should be equal"

test "Value DecidableEq - different values" := do
  let v1 := Value.string "hello"
  let v2 := Value.string "world"
  if v1 = v2 then throw <| IO.userError "Values should not be equal" else pure ()

test "Datom DecidableEq - equal datoms" := do
  let e := EntityId.mk 1
  let a := Attribute.mk ":test/attr"
  let v := Value.int 42
  let t := TxId.mk 1
  let d1 := Datom.assert e a v t
  let d2 := Datom.assert e a v t
  if d1 = d2 then pure () else throw <| IO.userError "Datoms should be equal"

test "Datom DecidableEq - different datoms" := do
  let e := EntityId.mk 1
  let a := Attribute.mk ":test/attr"
  let t := TxId.mk 1
  let d1 := Datom.assert e a (Value.int 42) t
  let d2 := Datom.assert e a (Value.int 43) t
  if d1 = d2 then throw <| IO.userError "Datoms should not be equal" else pure ()

end Ledger.Tests.Core
