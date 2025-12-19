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
  deriving Repr, Inhabited

namespace EAVTKey

instance : BEq EAVTKey where
  beq a b :=
    a.entity == b.entity &&
    a.attr == b.attr &&
    a.value == b.value &&
    a.tx == b.tx

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

end EAVTKey


/-- Key for AEVT index (Attribute, Entity, Value, Tx order). -/
structure AEVTKey where
  attr : Attribute
  entity : EntityId
  value : Value
  tx : TxId
  deriving Repr, Inhabited

namespace AEVTKey

instance : BEq AEVTKey where
  beq a b :=
    a.attr == b.attr &&
    a.entity == b.entity &&
    a.value == b.value &&
    a.tx == b.tx

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

end AEVTKey


/-- Key for AVET index (Attribute, Value, Entity, Tx order). -/
structure AVETKey where
  attr : Attribute
  value : Value
  entity : EntityId
  tx : TxId
  deriving Repr, Inhabited

namespace AVETKey

instance : BEq AVETKey where
  beq a b :=
    a.attr == b.attr &&
    a.value == b.value &&
    a.entity == b.entity &&
    a.tx == b.tx

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

end AVETKey


/-- Key for VAET index (Value, Attribute, Entity, Tx order).
    Used primarily for reverse reference lookups. -/
structure VAETKey where
  value : Value
  attr : Attribute
  entity : EntityId
  tx : TxId
  deriving Repr, Inhabited

namespace VAETKey

instance : BEq VAETKey where
  beq a b :=
    a.value == b.value &&
    a.attr == b.attr &&
    a.entity == b.entity &&
    a.tx == b.tx

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

end VAETKey

end Ledger
