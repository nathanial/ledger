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

test "Snapshot: JSON roundtrip" := do
  let snap : Persist.Snapshot := {
    basisT := ⟨3⟩
    nextEntityId := ⟨10⟩
    currentFacts := #[
      { entity := ⟨1⟩, attr := ⟨":person/name"⟩, value := .string "Alice", tx := ⟨3⟩, added := true }
    ]
    txLog := #[
      { txId := ⟨1⟩, txInstant := 0, datoms := #[
        { entity := ⟨1⟩, attr := ⟨":person/name"⟩, value := .string "Alice", tx := ⟨1⟩, added := true }
      ]}
    ]
  }
  let json := Persist.Snapshot.toJson snap
  match Persist.Snapshot.fromJson json with
  | some snap' =>
    snap'.basisT.id ≡ snap.basisT.id
    snap'.nextEntityId.id ≡ snap.nextEntityId.id
    snap'.currentFacts.size ≡ snap.currentFacts.size
    snap'.txLog.size ≡ snap.txLog.size
  | none => throw <| IO.userError "Snapshot parse failed"

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

/-! ## JSONL Replay Tests (Reproducing title update bug) -/

test "JSONL: Direct datom insertion with multiple values" := do
  -- Simulate JSONL replay: directly insert datoms without going through transact
  let card := EntityId.mk 193
  let titleAttr := Attribute.mk ":card/title"

  -- Create datoms as they would appear in JSONL
  -- Entity 193 has title updates from tx 201 to 203
  let datoms := #[
    Datom.mk card titleAttr (Value.string "Magic") (TxId.mk 201) true,
    Datom.mk card titleAttr (Value.string "Magic Gun") (TxId.mk 202) true,
    Datom.mk card titleAttr (Value.string "Magic Gun") (TxId.mk 203) true
  ]

  -- Insert directly into indexes (as JSONL replay does)
  let mut indexes := Indexes.empty
  for d in datoms do
    indexes := indexes.insertDatom d

  let db : Db := {
    basisT := TxId.mk 203
    indexes := indexes
    historyIndexes := indexes
    currentFacts := Db.currentFactsFromDatoms datoms.toList
    nextEntityId := EntityId.mk 194
  }

  -- getOne should return "Magic Gun" (the value with highest txId)
  let title := db.getOne card titleAttr
  title ≡ some (Value.string "Magic Gun")

test "JSONL: Many values same entity-attribute" := do
  -- Simulate the exact scenario from homebase.jsonl entity 193
  let card := EntityId.mk 193
  let titleAttr := Attribute.mk ":card/title"

  let datoms := #[
    Datom.mk card titleAttr (Value.string "Fungus") (TxId.mk 190) true,
    Datom.mk card titleAttr (Value.string "Fungus 123") (TxId.mk 191) true,
    Datom.mk card titleAttr (Value.string "Fungus 123 123123") (TxId.mk 196) true,
    Datom.mk card titleAttr (Value.string "Fungus ") (TxId.mk 199) true,
    Datom.mk card titleAttr (Value.string "Fungus") (TxId.mk 200) true,
    Datom.mk card titleAttr (Value.string "Magic") (TxId.mk 201) true,
    Datom.mk card titleAttr (Value.string "Magic Gun") (TxId.mk 202) true,
    Datom.mk card titleAttr (Value.string "Magic Gun") (TxId.mk 203) true
  ]

  let mut indexes := Indexes.empty
  for d in datoms do
    indexes := indexes.insertDatom d

  let db : Db := {
    basisT := TxId.mk 203
    indexes := indexes
    historyIndexes := indexes
    currentFacts := Db.currentFactsFromDatoms datoms.toList
    nextEntityId := EntityId.mk 194
  }

  -- getOne should return "Magic Gun" (highest tx is 203 for value "Magic Gun")
  let title := db.getOne card titleAttr
  title ≡ some (Value.string "Magic Gun")

test "JSONL: Datoms from entity attr correctly retrieved" := do
  -- Verify that datomsForEntityAttr returns all datoms
  let card := EntityId.mk 193
  let titleAttr := Attribute.mk ":card/title"

  let datoms := #[
    Datom.mk card titleAttr (Value.string "Magic") (TxId.mk 201) true,
    Datom.mk card titleAttr (Value.string "Magic Gun") (TxId.mk 202) true,
    Datom.mk card titleAttr (Value.string "Magic Gun") (TxId.mk 203) true
  ]

  let mut indexes := Indexes.empty
  for d in datoms do
    indexes := indexes.insertDatom d

  -- Check that all datoms are returned
  let retrieved := indexes.datomsForEntityAttr card titleAttr
  retrieved.length ≡ 3

test "JSONL: Replay builds correct indexes" := do
  -- Test that replaying a JSONL-like sequence builds working indexes
  let card := EntityId.mk 1
  let attr := Attribute.mk ":test/value"

  -- Parse simulated JSONL entries
  let entries := #[
    TxLogEntry.mk (TxId.mk 1) 0 #[
      Datom.mk card attr (Value.string "A") (TxId.mk 1) true
    ],
    TxLogEntry.mk (TxId.mk 2) 0 #[
      Datom.mk card attr (Value.string "B") (TxId.mk 2) true
    ],
    TxLogEntry.mk (TxId.mk 3) 0 #[
      Datom.mk card attr (Value.string "C") (TxId.mk 3) true
    ]
  ]

  -- Replay (mimicking replayJournal)
  let mut indexes := Indexes.empty
  for entry in entries do
    for datom in entry.datoms do
      indexes := indexes.insertDatom datom

  let allDatoms := (entries.toList.map fun entry => entry.datoms.toList).flatten
  let db : Db := {
    basisT := TxId.mk 3
    indexes := indexes
    historyIndexes := indexes
    currentFacts := Db.currentFactsFromDatoms allDatoms
    nextEntityId := EntityId.mk 2
  }

  -- Should return "C" (highest tx)
  let value := db.getOne card attr
  value ≡ some (Value.string "C")

test "JSONL: File round-trip with multiple updates" := do
  -- Create a temp file with JSONL data
  let tempPath : System.FilePath := "/tmp/ledger_test_updates.jsonl"

  -- Write JSONL entries simulating multiple title updates
  let entries := [
    "{\"txId\":201,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Magic\"},201,true]]}",
    "{\"txId\":202,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Magic Gun\"},202,true]]}",
    "{\"txId\":203,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Magic Gun\"},203,true]]}"
  ]

  let content := String.intercalate "\n" entries
  IO.FS.writeFile tempPath content

  -- Replay the journal
  let conn ← Persist.JSONL.replayJournal tempPath

  -- Check that getOne returns "Magic Gun"
  let card := EntityId.mk 193
  let titleAttr := Attribute.mk ":card/title"
  let title := conn.db.getOne card titleAttr
  title ≡ some (Value.string "Magic Gun")

test "JSONL: File with many title changes" := do
  -- Exact replica of homebase.jsonl entity 193 scenario
  let tempPath : System.FilePath := "/tmp/ledger_test_many_updates.jsonl"

  let entries := [
    "{\"txId\":190,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Fungus\"},190,true]]}",
    "{\"txId\":191,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Fungus 123\"},191,true]]}",
    "{\"txId\":196,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Fungus 123 123123\"},196,true]]}",
    "{\"txId\":199,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Fungus \"},199,true]]}",
    "{\"txId\":200,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Fungus\"},200,true]]}",
    "{\"txId\":201,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Magic\"},201,true]]}",
    "{\"txId\":202,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Magic Gun\"},202,true]]}",
    "{\"txId\":203,\"instant\":0,\"datoms\":[[193,\":card/title\",{\"t\":\"string\",\"v\":\"Magic Gun\"},203,true]]}"
  ]

  let content := String.intercalate "\n" entries
  IO.FS.writeFile tempPath content

  -- Replay the journal
  let conn ← Persist.JSONL.replayJournal tempPath

  -- Check datom count (current visible facts, duplicate assertions collapsed)
  let card := EntityId.mk 193
  let titleAttr := Attribute.mk ":card/title"
  let datoms := conn.db.indexes.datomsForEntityAttr card titleAttr
  ensure (datoms.length == 6) s!"Expected 6 datoms, got {datoms.length}"

  -- Check that getOne returns "Magic Gun" (value with highest tx)
  let title := conn.db.getOne card titleAttr
  title ≡ some (Value.string "Magic Gun")

test "Snapshot: load + replay tail" := do
  let journalPath : System.FilePath := "/tmp/ledger_snapshot_test.jsonl"
  let snapshotPath := Persist.Snapshot.defaultPath journalPath
  IO.FS.writeFile journalPath ""
  if (← snapshotPath.pathExists) then
    IO.FS.removeFile snapshotPath

  let pc ← Persist.PersistentConnection.create journalPath
  let (e1, pc) := pc.allocEntityId
  let tx1 : Transaction := [
    .add e1 (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (pc, _) := (← pc.transact tx1) | throw <| IO.userError "Tx1 failed"
  pc.snapshot
  let tx2 : Transaction := [
    .add e1 (Attribute.mk ":person/age") (Value.int 42)
  ]
  let .ok (pc, _) := (← pc.transact tx2) | throw <| IO.userError "Tx2 failed"
  pc.close

  let pc2 ← Persist.PersistentConnection.create journalPath
  pc2.db.getOne e1 (Attribute.mk ":person/name") ≡ some (Value.string "Alice")
  pc2.db.getOne e1 (Attribute.mk ":person/age") ≡ some (Value.int 42)
  pc2.allTxIds.length ≡ 2

test "Compaction: snapshot + journal trim" := do
  let journalPath : System.FilePath := "/tmp/ledger_compaction_test.jsonl"
  let snapshotPath := Persist.Snapshot.defaultPath journalPath
  IO.FS.writeFile journalPath ""
  if (← snapshotPath.pathExists) then
    IO.FS.removeFile snapshotPath

  let pc ← Persist.PersistentConnection.create journalPath
  let (e1, pc) := pc.allocEntityId
  let tx1 : Transaction := [
    .add e1 (Attribute.mk ":person/name") (Value.string "Alice")
  ]
  let .ok (pc, _) := (← pc.transact tx1) | throw <| IO.userError "Tx1 failed"
  let tx2 : Transaction := [
    .add e1 (Attribute.mk ":person/age") (Value.int 42)
  ]
  let .ok (pc, _) := (← pc.transact tx2) | throw <| IO.userError "Tx2 failed"

  let (pc, _) ← Persist.Compaction.compact pc
  pc.close

  let entries ← Persist.JSONL.readJournal journalPath
  entries.size ≡ 0

  let pc2 ← Persist.PersistentConnection.create journalPath
  pc2.db.getOne e1 (Attribute.mk ":person/name") ≡ some (Value.string "Alice")
  pc2.db.getOne e1 (Attribute.mk ":person/age") ≡ some (Value.int 42)
  pc2.allTxIds.length ≡ 2

end Ledger.Tests.Persistence
