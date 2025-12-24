/-
  Ledger.Tests.Persistence - JSON persistence tests
-/

import Crucible
import Ledger

namespace Ledger.Tests.Persistence

open Crucible
open Ledger

testSuite "JSON Persistence"

test "JSON: Value int roundtrip" := do
  let v := Value.int 42
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value float roundtrip" := do
  let v := Value.float 3.14
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  ensure (v' == some v) "Float should roundtrip"

test "JSON: Value string roundtrip" := do
  let v := Value.string "hello world"
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value bool true roundtrip" := do
  let v := Value.bool true
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value bool false roundtrip" := do
  let v := Value.bool false
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value instant roundtrip" := do
  let v := Value.instant 1703347200000
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value ref roundtrip" := do
  let v := Value.ref ⟨123⟩
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value keyword roundtrip" := do
  let v := Value.keyword ":status/active"
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Value bytes roundtrip" := do
  let v := Value.bytes (ByteArray.mk #[1, 2, 3, 4, 5])
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

test "JSON: Datom roundtrip" := do
  let d : Datom := {
    entity := ⟨1⟩
    attr := ⟨":person/name"⟩
    value := .string "Alice"
    tx := ⟨5⟩
    added := true
  }
  let json := Persist.JSON.datomToJson d
  match Persist.JSON.datomFromJson json with
  | some d' =>
    ensure (d'.entity == d.entity && d'.attr == d.attr && d'.value == d.value) "Datom fields should match"
  | none => throw <| IO.userError "Datom parse failed"

test "JSON: TxLogEntry roundtrip" := do
  let entry : TxLogEntry := {
    txId := ⟨1⟩
    txInstant := 1703347200000
    datoms := #[
      { entity := ⟨1⟩, attr := ⟨":person/name"⟩, value := .string "Alice", tx := ⟨1⟩, added := true },
      { entity := ⟨1⟩, attr := ⟨":person/age"⟩, value := .int 30, tx := ⟨1⟩, added := true }
    ]
  }
  let json := Persist.JSON.txLogEntryToJson entry
  match Persist.JSON.txLogEntryFromJson json with
  | some entry' => entry'.txId.id ≡ entry.txId.id
  | none => throw <| IO.userError "TxLogEntry parse failed"

test "JSON: Base64 empty roundtrip" := do
  let data := ByteArray.empty
  let encoded := Persist.JSON.base64Encode data
  match Persist.JSON.base64Decode encoded with
  | some decoded => decoded.size ≡ 0
  | none => throw <| IO.userError "Base64 decode failed"

test "JSON: Base64 roundtrip" := do
  let data := ByteArray.mk #[72, 101, 108, 108, 111]  -- "Hello"
  let encoded := Persist.JSON.base64Encode data
  match Persist.JSON.base64Decode encoded with
  | some decoded => decoded ≡ data
  | none => throw <| IO.userError "Base64 decode failed"

test "JSON: String with special chars" := do
  let v := Value.string "Hello\nWorld\t\"quoted\""
  let json := Persist.JSON.valueToJson v
  let v' := Persist.JSON.valueFromJson json
  v' ≡ some v

#generate_tests

end Ledger.Tests.Persistence
