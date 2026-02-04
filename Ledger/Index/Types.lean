/-
  Ledger.Index.Types

  Index key types for the various database indexes.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value

namespace Ledger

/-- Key for EAVT index (Entity, Attribute, Value, Tx order). -/
structure EAVTKey where
  entity : EntityId
  attr : Attribute
  value : Value
  tx : TxId
  deriving Repr, Inhabited, DecidableEq

namespace EAVTKey

instance : Ord EAVTKey where
  compare a b :=
    match compare a.entity b.entity with
    | .eq => match compare a.attr b.attr with
      | .eq => match compare a.value b.value with
        | .eq => compare a.tx b.tx
        | o => o
      | o => o
    | o => o

instance : LT EAVTKey where
  lt a b := compare a b == .lt

instance : LE EAVTKey where
  le a b := compare a b != .gt

/-- Minimum possible Value for ordering (int has lowest typeTag). -/
def minValue : Value := .int (-9223372036854775808)  -- Int64 min

/-- Minimum possible Attribute for ordering. -/
def minAttr : Attribute := ⟨""⟩

/-- Minimum possible EntityId for ordering. -/
def minEntity : EntityId := ⟨-9223372036854775808⟩  -- Int64 min

/-- Minimum possible TxId for ordering. -/
def minTx : TxId := TxId.genesis

/-- Create minimum key for entity-based range query. -/
def minForEntity (e : EntityId) : EAVTKey :=
  { entity := e, attr := minAttr, value := minValue, tx := minTx }

/-- Create minimum key for entity+attr range query. -/
def minForEntityAttr (e : EntityId) (a : Attribute) : EAVTKey :=
  { entity := e, attr := a, value := minValue, tx := minTx }

/-- Create minimum key for entity+attr+value range query. -/
def minForEntityAttrValue (e : EntityId) (a : Attribute) (v : Value) : EAVTKey :=
  { entity := e, attr := a, value := v, tx := minTx }

/-- Check if key matches entity. -/
def matchesEntity (e : EntityId) (k : EAVTKey) : Bool := k.entity == e

/-- Check if key matches entity and attribute. -/
def matchesEntityAttr (e : EntityId) (a : Attribute) (k : EAVTKey) : Bool :=
  k.entity == e && k.attr == a

/-- Check if key matches entity, attribute, and value. -/
def matchesEntityAttrValue (e : EntityId) (a : Attribute) (v : Value) (k : EAVTKey) : Bool :=
  k.entity == e && k.attr == a && k.value == v

end EAVTKey


/-- Key for AEVT index (Attribute, Entity, Value, Tx order). -/
structure AEVTKey where
  attr : Attribute
  entity : EntityId
  value : Value
  tx : TxId
  deriving Repr, Inhabited, DecidableEq

namespace AEVTKey

instance : Ord AEVTKey where
  compare a b :=
    match compare a.attr b.attr with
    | .eq => match compare a.entity b.entity with
      | .eq => match compare a.value b.value with
        | .eq => compare a.tx b.tx
        | o => o
      | o => o
    | o => o

instance : LT AEVTKey where
  lt a b := compare a b == .lt

instance : LE AEVTKey where
  le a b := compare a b != .gt

/-- Create minimum key for attribute-based range query. -/
def minForAttr (a : Attribute) : AEVTKey :=
  { attr := a, entity := EAVTKey.minEntity, value := EAVTKey.minValue, tx := EAVTKey.minTx }

/-- Create minimum key for attr+entity range query. -/
def minForAttrEntity (a : Attribute) (e : EntityId) : AEVTKey :=
  { attr := a, entity := e, value := EAVTKey.minValue, tx := EAVTKey.minTx }

/-- Check if key matches attribute. -/
def matchesAttr (a : Attribute) (k : AEVTKey) : Bool := k.attr == a

/-- Check if key matches attribute and entity. -/
def matchesAttrEntity (a : Attribute) (e : EntityId) (k : AEVTKey) : Bool :=
  k.attr == a && k.entity == e

end AEVTKey


/-- Key for AVET index (Attribute, Value, Entity, Tx order). -/
structure AVETKey where
  attr : Attribute
  value : Value
  entity : EntityId
  tx : TxId
  deriving Repr, Inhabited, DecidableEq

namespace AVETKey

instance : Ord AVETKey where
  compare a b :=
    match compare a.attr b.attr with
    | .eq => match compare a.value b.value with
      | .eq => match compare a.entity b.entity with
        | .eq => compare a.tx b.tx
        | o => o
      | o => o
    | o => o

instance : LT AVETKey where
  lt a b := compare a b == .lt

instance : LE AVETKey where
  le a b := compare a b != .gt

/-- Create minimum key for attr+value range query. -/
def minForAttrValue (a : Attribute) (v : Value) : AVETKey :=
  { attr := a, value := v, entity := EAVTKey.minEntity, tx := EAVTKey.minTx }

/-- Check if key matches attribute and value. -/
def matchesAttrValue (a : Attribute) (v : Value) (k : AVETKey) : Bool :=
  k.attr == a && k.value == v

end AVETKey


/-- Key for VAET index (Value, Attribute, Entity, Tx order).
    Used primarily for reverse reference lookups. -/
structure VAETKey where
  value : Value
  attr : Attribute
  entity : EntityId
  tx : TxId
  deriving Repr, Inhabited, DecidableEq

namespace VAETKey

instance : Ord VAETKey where
  compare a b :=
    match compare a.value b.value with
    | .eq => match compare a.attr b.attr with
      | .eq => match compare a.entity b.entity with
        | .eq => compare a.tx b.tx
        | o => o
      | o => o
    | o => o

instance : LT VAETKey where
  lt a b := compare a b == .lt

instance : LE VAETKey where
  le a b := compare a b != .gt

/-- Create minimum key for value-based range query (for reverse references). -/
def minForValue (v : Value) : VAETKey :=
  { value := v, attr := EAVTKey.minAttr, entity := EAVTKey.minEntity, tx := EAVTKey.minTx }

/-- Create minimum key for value+attr range query. -/
def minForValueAttr (v : Value) (a : Attribute) : VAETKey :=
  { value := v, attr := a, entity := EAVTKey.minEntity, tx := EAVTKey.minTx }

/-- Check if key matches value. -/
def matchesValue (v : Value) (k : VAETKey) : Bool := k.value == v

/-- Check if key matches value and attribute. -/
def matchesValueAttr (v : Value) (a : Attribute) (k : VAETKey) : Bool :=
  k.value == v && k.attr == a

end VAETKey

end Ledger
