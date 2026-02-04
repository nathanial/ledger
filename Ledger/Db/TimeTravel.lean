/-
  Ledger.Db.TimeTravel

  Time-travel query support for the database.
  Provides asOf, since, and history queries.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Core.Util
import Ledger.Index.Manager

namespace Ledger

/-- Transaction log entry storing all datoms from a transaction. -/
structure TxLogEntry where
  /-- The transaction ID. -/
  txId : TxId
  /-- Unix timestamp when the transaction was processed. -/
  txInstant : Nat
  /-- All datoms (assertions and retractions) in this transaction. -/
  datoms : Array Datom
  deriving Repr, Inhabited

/-- Transaction log - ordered list of all transactions. -/
abbrev TxLog := Array TxLogEntry

namespace TxLog

/-- Create an empty transaction log. -/
def empty : TxLog := #[]

/-- Add a transaction to the log. -/
def add (log : TxLog) (entry : TxLogEntry) : TxLog :=
  log.push entry

/-- Get transaction by ID. -/
def get? (log : TxLog) (txId : TxId) : Option TxLogEntry :=
  log.find? fun e => e.txId == txId

/-- Get all transactions since (but not including) a specific transaction. -/
def since (log : TxLog) (txId : TxId) : List TxLogEntry :=
  log.toList.filter fun e => e.txId.id > txId.id

/-- Get all transactions up to and including a specific transaction. -/
def upTo (log : TxLog) (txId : TxId) : List TxLogEntry :=
  log.toList.filter fun e => e.txId.id <= txId.id

end TxLog

namespace TimeTravel

/-- Filter a list of datoms to only those visible at a specific transaction.
    Groups by (entity, attribute, value) and checks visibility for each. -/
def filterVisibleAt (allDatoms : DatomSeq) (txId : TxId) : DatomSeq :=
  Util.filterVisibleDatomsAt allDatoms txId

/-- Get all datoms that were added or retracted since a specific transaction. -/
def datomsSince (allDatoms : DatomSeq) (txId : TxId) : DatomSeq :=
  allDatoms.filter fun d => d.tx.id > txId.id

/-- Get the full history of datoms for an entity.
    Returns all assertions and retractions in transaction order. -/
def entityHistory (allDatoms : DatomSeq) (entity : EntityId) : DatomSeq :=
  let entityDatoms := allDatoms.filter fun d => d.entity == entity
  -- Sort by transaction ID ascending
  entityDatoms.toArray.qsort (fun a b => a.tx.id < b.tx.id) |>.toList

/-- Get the full history of a specific attribute on an entity. -/
def attrHistory (allDatoms : DatomSeq) (entity : EntityId) (attr : Attribute) : DatomSeq :=
  let filtered := allDatoms.filter fun d => d.entity == entity && d.attr == attr
  filtered.toArray.qsort (fun a b => a.tx.id < b.tx.id) |>.toList

end TimeTravel

end Ledger
