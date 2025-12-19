/-
  Ledger.Db.TimeTravel

  Time-travel query support for the database.
  Provides asOf, since, and history queries.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
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

/-- Get the current value of a datom at a specific transaction.
    Returns the datom if it's visible (asserted and not retracted).
    Takes all datoms for a specific (entity, attr, value) triple. -/
def datomVisibleAt? (datoms : List Datom) (txId : TxId) : Option Datom :=
  let relevant := datoms.filter fun d => d.tx.id <= txId.id
  let sorted := relevant.toArray.qsort fun a b => a.tx.id > b.tx.id
  if h : sorted.size > 0 then
    let d := sorted[0]
    if d.added then some d else none
  else
    none

/-- Check if two datoms refer to the same fact (same entity, attr, value). -/
private def sameFact (a b : Datom) : Bool :=
  a.entity == b.entity && a.attr == b.attr && a.value == b.value

/-- Group datoms by their fact identity (entity, attr, value).
    Returns a list of groups, where each group contains all datoms
    for a specific fact. -/
private def groupByFact (datoms : List Datom) : List (List Datom) :=
  datoms.foldl (init := []) fun groups d =>
    match groups.findIdx? (fun g => g.any (sameFact d)) with
    | some idx =>
      groups.set idx (d :: groups[idx]!)
    | none =>
      [d] :: groups

/-- Filter a list of datoms to only those visible at a specific transaction.
    Groups by (entity, attribute, value) and checks visibility for each. -/
def filterVisibleAt (allDatoms : List Datom) (txId : TxId) : List Datom :=
  let groups := groupByFact allDatoms
  groups.filterMap fun group => datomVisibleAt? group txId

/-- Get all datoms that were added or retracted since a specific transaction. -/
def datomsSince (allDatoms : List Datom) (txId : TxId) : List Datom :=
  allDatoms.filter fun d => d.tx.id > txId.id

/-- Get the full history of datoms for an entity.
    Returns all assertions and retractions in transaction order. -/
def entityHistory (allDatoms : List Datom) (entity : EntityId) : List Datom :=
  let entityDatoms := allDatoms.filter fun d => d.entity == entity
  -- Sort by transaction ID ascending
  entityDatoms.toArray.qsort (fun a b => a.tx.id < b.tx.id) |>.toList

/-- Get the full history of a specific attribute on an entity. -/
def attrHistory (allDatoms : List Datom) (entity : EntityId) (attr : Attribute) : List Datom :=
  let filtered := allDatoms.filter fun d => d.entity == entity && d.attr == attr
  filtered.toArray.qsort (fun a b => a.tx.id < b.tx.id) |>.toList

end TimeTravel

end Ledger
