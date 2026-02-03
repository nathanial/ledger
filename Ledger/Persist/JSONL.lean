/-
  Ledger.Persist.JSONL

  JSONL (JSON Lines) file I/O for transaction log entries.
  Each line in the file is a complete JSON object representing a TxLogEntry.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Db.TimeTravel
import Ledger.Db.Connection
import Ledger.Persist.JSON

namespace Ledger.Persist.JSONL

open Ledger.Persist.JSON

/-- Append a transaction log entry to a JSONL file -/
def appendEntry (handle : IO.FS.Handle) (entry : TxLogEntry) : IO Unit := do
  let json := txLogEntryToJson entry
  handle.putStrLn json
  handle.flush

/-- Read all transaction log entries from a JSONL file -/
def readJournal (path : System.FilePath) : IO (Array TxLogEntry) := do
  if !(← path.pathExists) then
    return #[]

  let content ← IO.FS.readFile path
  let lines := content.splitOn "\n" |>.filter (·.trim.length > 0)

  let mut entries : Array TxLogEntry := #[]
  for line in lines do
    match txLogEntryFromJson line with
    | some entry => entries := entries.push entry
    | none =>
      -- Log warning but continue (skip malformed lines)
      IO.eprintln s!"Warning: skipping malformed JSONL line"

  return entries

/-- Replay a journal file to build a Connection -/
def replayJournal (path : System.FilePath) : IO Connection := do
  let entries ← readJournal path

  -- Start with empty connection
  let mut conn := Connection.create

  -- Replay each transaction's datoms
  for entry in entries do
    -- Update the connection's database with these datoms
    -- We need to update basisT and nextEntityId
    let newBasisT := entry.txId

    -- Update indexes (current + history)
    let mut indexes := conn.db.indexes
    let mut historyIndexes := conn.db.historyIndexes
    let mut currentFacts := conn.db.currentFacts
    for datom in entry.datoms do
      historyIndexes := historyIndexes.insertDatom datom
      if datom.added then
        let key := FactKey.ofDatom datom
        if let some prev := currentFacts[key]? then
          indexes := indexes.removeDatom prev
        indexes := indexes.insertDatom datom
        currentFacts := currentFacts.insert key datom
      else
        let key := FactKey.ofDatom datom
        if let some prev := currentFacts[key]? then
          indexes := indexes.removeDatom prev
        currentFacts := currentFacts.erase key

    -- Find max entity ID to update nextEntityId
    let mut maxEntityId := conn.db.nextEntityId.id
    for datom in entry.datoms do
      if datom.entity.id > maxEntityId then
        maxEntityId := datom.entity.id

    -- Create new database state
    let db' : Db := {
      basisT := newBasisT
      indexes := indexes
      historyIndexes := historyIndexes
      currentFacts := currentFacts
      nextEntityId := ⟨maxEntityId + 1⟩
    }

    -- Add to transaction log
    let logEntry : TxLogEntry := entry
    let txLog' := conn.txLog.add logEntry

    conn := { db := db', txLog := txLog' }

  return conn

end Ledger.Persist.JSONL
