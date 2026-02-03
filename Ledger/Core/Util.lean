/-
  Ledger.Core.Util

  Shared helpers for core data structures.
-/

import Ledger.Core.Datom

namespace Ledger

namespace Util

/-- Check if two datoms refer to the same fact (same entity, attr, value). -/
def sameFact (a b : Datom) : Bool :=
  a.entity == b.entity && a.attr == b.attr && a.value == b.value

/-- Group datoms by their fact identity (entity, attr, value). -/
def groupByFact (datoms : List Datom) : List (List Datom) :=
  datoms.foldl (init := []) fun groups d =>
    match groups.findIdx? (fun g => g.any (sameFact d)) with
    | some idx =>
      groups.set idx (d :: groups[idx]!)
    | none =>
      [d] :: groups

/-- Latest datom by transaction ID (no time filter). -/
private def latestDatom? (datoms : List Datom) : Option Datom :=
  match datoms with
  | [] => none
  | d :: rest =>
    let latest := rest.foldl (init := d) fun best cand =>
      if cand.tx.id > best.tx.id then cand else best
    some latest

/-- Latest datom at or before a transaction ID. -/
def latestDatomAt? (datoms : List Datom) (txId : TxId) : Option Datom :=
  latestDatom? (datoms.filter fun d => d.tx.id <= txId.id)

/-- Visible datom at current time (latest datom is an assertion). -/
def visibleDatom? (datoms : List Datom) : Option Datom := do
  let d ← latestDatom? datoms
  if d.added then some d else none

/-- Visible datom at a transaction ID (latest at or before is an assertion). -/
def visibleDatomAt? (datoms : List Datom) (txId : TxId) : Option Datom := do
  let d ← latestDatomAt? datoms txId
  if d.added then some d else none

/-- Filter a list of datoms to only those currently visible. -/
def filterVisibleDatoms (allDatoms : List Datom) : List Datom :=
  let groups := groupByFact allDatoms
  groups.filterMap visibleDatom?

/-- Filter a list of datoms to only those visible at a transaction. -/
def filterVisibleDatomsAt (allDatoms : List Datom) (txId : TxId) : List Datom :=
  let groups := groupByFact allDatoms
  groups.filterMap (fun group => visibleDatomAt? group txId)

end Util

end Ledger
