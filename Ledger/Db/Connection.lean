/-
  Ledger.Db.Connection

  Connection maintains a mutable reference to the current database
  along with the full transaction log for time-travel queries.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Index.Manager
import Ledger.Tx.Types
import Ledger.Tx.Functions
import Ledger.Db.Database
import Ledger.Db.TimeTravel

namespace Ledger

/-- A Connection wraps mutable state around an immutable database.
    It maintains the current database value and a full transaction log
    for time-travel queries. -/
structure Connection where
  /-- The current database snapshot. -/
  db : Db
  /-- Full transaction log for history/time-travel. -/
  txLog : TxLog
  deriving Inhabited

namespace Connection

/-- Create a new connection with an empty database. -/
def create : Connection :=
  { db := Db.empty
  , txLog := TxLog.empty }

/-- Get the current database snapshot. -/
def current (conn : Connection) : Db := conn.db

/-- Get the basis transaction of the current database. -/
def basisT (conn : Connection) : TxId := conn.db.basisT

/-- Process a transaction, returning a new connection and report.
    The transaction log is updated with the new transaction. -/
def transact (conn : Connection) (tx : Transaction) (instant : Nat := 0)
    : Except TxError (Connection × TxReport) := do
  let (db', report) ← conn.db.transact tx instant

  -- Add to transaction log
  let logEntry : TxLogEntry :=
    { txId := report.txId
    , txInstant := report.txInstant
    , datoms := report.txData }

  let conn' : Connection :=
    { db := db'
    , txLog := conn.txLog.add logEntry }

  return (conn', report)

/-- Process a transaction with a custom tx function registry. -/
def transactWith (conn : Connection) (registry : TxFuncRegistry) (tx : Transaction)
    (instant : Nat := 0) : Except TxError (Connection × TxReport) := do
  let (db', report) ← conn.db.transactWith registry tx instant

  -- Add to transaction log
  let logEntry : TxLogEntry :=
    { txId := report.txId
    , txInstant := report.txInstant
    , datoms := report.txData }

  let conn' : Connection :=
    { db := db'
    , txLog := conn.txLog.add logEntry }

  return (conn', report)

/-- Allocate a new entity ID. -/
def allocEntityId (conn : Connection) : EntityId × Connection :=
  let (eid, db') := conn.db.allocEntityId
  (eid, { conn with db := db' })

/-- Allocate multiple entity IDs. -/
def allocEntityIds (conn : Connection) (n : Nat) : List EntityId × Connection :=
  let (eids, db') := conn.db.allocEntityIds n
  (eids, { conn with db := db' })

-- ============================================================
-- Time-travel queries
-- ============================================================

/-- Get the database as it existed at a specific transaction.
    This filters out any datoms added after txId and properly
    handles retractions. -/
def asOf (conn : Connection) (txId : TxId) : Db :=
  -- Get all datoms from transactions up to txId
  let relevantTxs := conn.txLog.upTo txId
  let allDatoms := (relevantTxs.map fun entry => entry.datoms.toList).flatten

  -- Filter to only visible datoms (handling retractions)
  let visibleDatoms := TimeTravel.filterVisibleAt allDatoms txId

  -- Rebuild indexes from visible datoms
  let indexes := visibleDatoms.foldl Indexes.insertDatom Indexes.empty
  let historyIndexes := allDatoms.foldl Indexes.insertDatom Indexes.empty
  let currentFacts := Db.currentFactsFromDatoms visibleDatoms

  { basisT := txId
  , indexes := indexes
  , historyIndexes := historyIndexes
  , currentFacts := currentFacts
  , nextEntityId := conn.db.nextEntityId }

/-- Get all datoms that were asserted or retracted since a specific transaction.
    Returns the raw datoms including both assertions and retractions. -/
def since (conn : Connection) (txId : TxId) : DatomSeq :=
  let relevantTxs := conn.txLog.since txId
  (relevantTxs.map fun entry => entry.datoms.toList).flatten

/-- Get transaction data for a specific transaction. -/
def txData (conn : Connection) (txId : TxId) : Option TxLogEntry :=
  conn.txLog.get? txId

/-- Get the full history of an entity (all assertions and retractions). -/
def entityHistory (conn : Connection) (entity : EntityId) : DatomSeq :=
  let allDatoms := (conn.txLog.toList.map fun entry => entry.datoms.toList).flatten
  TimeTravel.entityHistory allDatoms entity

/-- Get the full history of a specific attribute on an entity. -/
def attrHistory (conn : Connection) (entity : EntityId) (attr : Attribute) : DatomSeq :=
  let allDatoms := (conn.txLog.toList.map fun entry => entry.datoms.toList).flatten
  TimeTravel.attrHistory allDatoms entity attr

/-- Get all transaction IDs in the log. -/
def allTxIds (conn : Connection) : List TxId :=
  conn.txLog.toList.map (·.txId)

end Connection

end Ledger
