/-
  Ledger.Core.Datom

  The fundamental unit of information in Ledger - the Datom.
  A datom is a 5-tuple: (Entity, Attribute, Value, Transaction, Added?)
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value

namespace Ledger

/-- A Datom is the fundamental unit of information in the database.
    It represents a fact: "Entity E has Attribute A with Value V,
    as of Transaction T, and was Added (true) or Retracted (false)." -/
structure Datom where
  /-- The entity this fact is about -/
  entity : EntityId
  /-- The attribute (property name) -/
  attr : Attribute
  /-- The value of the attribute -/
  value : Value
  /-- The transaction that asserted/retracted this fact -/
  tx : TxId
  /-- True if asserted, false if retracted -/
  added : Bool := true
  deriving Repr, Inhabited, DecidableEq

/-- Sequence of datoms. -/
abbrev DatomSeq := List Datom

namespace Datom

instance : ToString Datom where
  toString d :=
    let op := if d.added then "+" else "-"
    s!"[{op} {d.entity} {d.attr} {d.value} {d.tx}]"

/-- Create an assertion datom. -/
def assert (e : EntityId) (a : Attribute) (v : Value) (tx : TxId) : Datom :=
  { entity := e, attr := a, value := v, tx := tx, added := true }

/-- Create a retraction datom. -/
def retract (e : EntityId) (a : Attribute) (v : Value) (tx : TxId) : Datom :=
  { entity := e, attr := a, value := v, tx := tx, added := false }

/-- Compare datoms in EAVT order (Entity, Attribute, Value, Tx). -/
def compareEAVT (a b : Datom) : Ordering :=
  match compare a.entity b.entity with
  | .eq => match compare a.attr b.attr with
    | .eq => match compare a.value b.value with
      | .eq => compare a.tx b.tx
      | o => o
    | o => o
  | o => o

/-- Compare datoms in AEVT order (Attribute, Entity, Value, Tx). -/
def compareAEVT (a b : Datom) : Ordering :=
  match compare a.attr b.attr with
  | .eq => match compare a.entity b.entity with
    | .eq => match compare a.value b.value with
      | .eq => compare a.tx b.tx
      | o => o
    | o => o
  | o => o

/-- Compare datoms in AVET order (Attribute, Value, Entity, Tx). -/
def compareAVET (a b : Datom) : Ordering :=
  match compare a.attr b.attr with
  | .eq => match compare a.value b.value with
    | .eq => match compare a.entity b.entity with
      | .eq => compare a.tx b.tx
      | o => o
    | o => o
  | o => o

/-- Compare datoms in VAET order (Value, Attribute, Entity, Tx).
    Primarily useful for reference values to find reverse references. -/
def compareVAET (a b : Datom) : Ordering :=
  match compare a.value b.value with
  | .eq => match compare a.attr b.attr with
    | .eq => match compare a.entity b.entity with
      | .eq => compare a.tx b.tx
      | o => o
    | o => o
  | o => o

instance : Ord Datom where
  compare a b := compareEAVT a b

instance : Hashable Datom where
  hash d := hash (d.entity, d.attr, d.value, d.tx, d.added)

end Datom

end Ledger
