/-
  Ledger.Persist.Connection

  PersistentConnection wraps Connection and automatically persists
  transactions to a JSONL file.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Tx.Types
import Ledger.Db.Database
import Ledger.Db.TimeTravel
import Ledger.Db.Connection
import Ledger.Persist.JSON
import Ledger.Persist.JSONL
import Ledger.Persist.Snapshot

open Ledger.Persist.JSON
open Ledger.Persist.JSONL
open Ledger.Persist.Snapshot

namespace Ledger.Persist

/-- Persistent connection that auto-writes transactions to JSONL -/
structure PersistentConnection where
  /-- The underlying in-memory connection -/
  conn : Connection
  /-- Path to the JSONL journal file -/
  journalPath : System.FilePath
  /-- Open file handle for appending -/
  handle : IO.FS.Handle

namespace PersistentConnection

/-- Open or create a persistent connection from a JSONL file.
    If the file exists, replays all transactions to restore state.
    Opens the file for appending new transactions. -/
def create (path : System.FilePath) : IO PersistentConnection := do
  let snapshotPath := Snapshot.defaultPath path
  let snap? ← Snapshot.read snapshotPath
  let baseConn := match snap? with
    | some snap => Snapshot.toConnection snap
    | none => Connection.create
  let baseTx := match snap? with
    | some snap => snap.basisT
    | none => TxId.genesis

  -- Replay journal tail (if any)
  let conn ← replayJournalSince baseConn path baseTx

  -- Open file for appending
  let handle ← IO.FS.Handle.mk path .append

  return {
    conn := conn
    journalPath := path
    handle := handle
  }

/-- Process a transaction and automatically persist to journal.
    Returns the updated connection and transaction report. -/
def transact (pc : PersistentConnection) (tx : Transaction) (instant : Nat := 0)
    : IO (Except TxError (PersistentConnection × TxReport)) := do
  match pc.conn.transact tx instant with
  | .error e => return .error e
  | .ok (conn', report) =>
    -- Create log entry
    let entry : TxLogEntry := {
      txId := report.txId
      txInstant := report.txInstant
      datoms := report.txData
    }

    -- Persist to journal
    appendEntry pc.handle entry

    -- Return updated connection
    return .ok ({ pc with conn := conn' }, report)

/-- Close the journal file handle -/
def close (pc : PersistentConnection) : IO Unit := do
  pc.handle.flush

/-- Write a snapshot for this connection (default path). -/
def snapshot (pc : PersistentConnection) : IO Unit := do
  let snap := Snapshot.fromConnection pc.conn
  let path := Snapshot.defaultPath pc.journalPath
  Snapshot.write path snap


/-- Get the underlying database for queries -/
def db (pc : PersistentConnection) : Db :=
  pc.conn.db

/-- Get the current database snapshot -/
def current (pc : PersistentConnection) : Db :=
  pc.conn.current

/-- Allocate a new entity ID -/
def allocEntityId (pc : PersistentConnection) : EntityId × PersistentConnection :=
  let (eid, conn') := pc.conn.allocEntityId
  (eid, { pc with conn := conn' })

/-- Allocate multiple entity IDs -/
def allocEntityIds (pc : PersistentConnection) (n : Nat) : List EntityId × PersistentConnection :=
  let (eids, conn') := pc.conn.allocEntityIds n
  (eids, { pc with conn := conn' })

/-- Get the basis transaction of the current database -/
def basisT (pc : PersistentConnection) : TxId :=
  pc.conn.basisT

/-- Get the database as it existed at a specific transaction -/
def asOf (pc : PersistentConnection) (txId : TxId) : Db :=
  pc.conn.asOf txId

/-- Get all datoms that were asserted or retracted since a specific transaction -/
def since (pc : PersistentConnection) (txId : TxId) : List Datom :=
  pc.conn.since txId

/-- Get transaction data for a specific transaction -/
def txData (pc : PersistentConnection) (txId : TxId) : Option TxLogEntry :=
  pc.conn.txData txId

/-- Get the full history of an entity -/
def entityHistory (pc : PersistentConnection) (entity : EntityId) : List Datom :=
  pc.conn.entityHistory entity

/-- Get the full history of a specific attribute on an entity -/
def attrHistory (pc : PersistentConnection) (entity : EntityId) (attr : Attribute) : List Datom :=
  pc.conn.attrHistory entity attr

/-- Get all transaction IDs in the log -/
def allTxIds (pc : PersistentConnection) : List TxId :=
  pc.conn.allTxIds

end PersistentConnection

end Ledger.Persist
