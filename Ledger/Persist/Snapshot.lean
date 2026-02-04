/-
  Ledger.Persist.Snapshot

  Snapshot persistence for faster recovery.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Db.Database
import Ledger.Db.Connection
import Ledger.Index.Manager
import Ledger.Persist.JSON
import Staple.Json

namespace Ledger.Persist

open Ledger.Persist.JSON
open Staple.Json.Value

private abbrev JValue := Staple.Json.Value

/-- Snapshot of the database and transaction log at a point in time. -/
structure Snapshot where
  basisT : TxId
  nextEntityId : EntityId
  currentFacts : Array Datom
  txLog : Array TxLogEntry
  deriving Repr, Inhabited

namespace Snapshot

/-- Default snapshot path derived from the journal path. -/
def defaultPath (journalPath : System.FilePath) : System.FilePath :=
  System.FilePath.mk (journalPath.toString ++ ".snapshot.json")

/-- Build a snapshot from a connection. -/
def fromConnection (conn : Connection) : Snapshot :=
  let facts := conn.db.currentFacts.toList.map Prod.snd
  { basisT := conn.db.basisT
  , nextEntityId := conn.db.nextEntityId
  , currentFacts := facts.toArray
  , txLog := conn.txLog }

/-- Build a connection from a snapshot. -/
def toConnection (snap : Snapshot) : Connection :=
  let currentFacts := Db.currentFactsFromDatoms snap.currentFacts.toList
  let indexes := snap.currentFacts.toList.foldl Indexes.insertDatom Indexes.empty
  let historyDatoms := (snap.txLog.toList.map fun entry => entry.datoms.toList).flatten
  let historyIndexes := historyDatoms.foldl Indexes.insertDatom Indexes.empty
  let db : Db := {
    basisT := snap.basisT
    indexes := indexes
    historyIndexes := historyIndexes
    currentFacts := currentFacts
    nextEntityId := snap.nextEntityId
  }
  { db := db, txLog := snap.txLog }

/-- Serialize a snapshot to JSON string. -/
def toJson (snap : Snapshot) : String := Id.run do
  let mut factsJson := ""
  for i in [:snap.currentFacts.size] do
    if i > 0 then factsJson := factsJson ++ ","
    factsJson := factsJson ++ datomToJson snap.currentFacts[i]!

  let mut logJson := ""
  for i in [:snap.txLog.size] do
    if i > 0 then logJson := logJson ++ ","
    logJson := logJson ++ txLogEntryToJson snap.txLog[i]!

  s!"\{\"basisT\":{snap.basisT.id},\"nextEntityId\":{snap.nextEntityId.id},\"currentFacts\":[{factsJson}],\"txLog\":[{logJson}]}"

private def fromJsonValue (v : JValue) : Option Snapshot := do
  let basisT ← getNatField? "basisT" v
  let nextEntityId ← getIntField? "nextEntityId" v
  let factVals ← getArrField? "currentFacts" v
  let logVals ← getArrField? "txLog" v

  let mut facts : Array Datom := #[]
  for factVal in factVals do
    let d ← datomFromJsonValue factVal
    facts := facts.push d

  let mut entries : Array TxLogEntry := #[]
  for logVal in logVals do
    let entry ← txLogEntryFromJsonValue logVal
    entries := entries.push entry

  return {
    basisT := ⟨basisT⟩
    nextEntityId := ⟨nextEntityId⟩
    currentFacts := facts
    txLog := entries
  }

/-- Deserialize a snapshot from JSON string. -/
def fromJson (s : String) : Option Snapshot := do
  let v ← Staple.Json.parse? s
  fromJsonValue v

/-- Write a snapshot file. -/
def write (path : System.FilePath) (snap : Snapshot) : IO Unit := do
  IO.FS.writeFile path (toJson snap)

/-- Read a snapshot file. -/
def read (path : System.FilePath) : IO (Option Snapshot) := do
  if !(← path.pathExists) then
    return none
  let content ← IO.FS.readFile path
  return fromJson content

end Snapshot

end Ledger.Persist
