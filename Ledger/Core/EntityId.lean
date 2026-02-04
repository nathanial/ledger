/-
  Ledger.Core.EntityId

  Entity IDs and Transaction IDs for the fact database.
  Negative entity IDs are used for temporary entities (pre-transaction).
-/

namespace Ledger

/-- Entity ID - unique identifier for entities in the database.
    Negative IDs represent temporary entities that will be resolved
    to permanent IDs during transaction processing. -/
structure EntityId where
  id : Int
  deriving Repr, DecidableEq, Hashable, Inhabited

namespace EntityId

instance : Ord EntityId where
  compare a b := compare a.id b.id

instance : LT EntityId where
  lt a b := a.id < b.id

instance : LE EntityId where
  le a b := a.id <= b.id

instance : ToString EntityId where
  toString e := s!"e{e.id}"

/-- Check if this is a temporary entity ID. -/
def isTemp (e : EntityId) : Bool := e.id < 0

/-- The null/invalid entity ID. -/
def null : EntityId := ⟨0⟩

end EntityId


/-- Transaction ID - monotonically increasing identifier for transactions.
    Each transaction gets a unique TxId, allowing temporal queries. -/
structure TxId where
  id : Nat
  deriving Repr, DecidableEq, Hashable, Inhabited

namespace TxId

instance : Ord TxId where
  compare a b := compare a.id b.id

instance : LT TxId where
  lt a b := a.id < b.id

instance : LE TxId where
  le a b := a.id <= b.id

instance : ToString TxId where
  toString t := s!"tx{t.id}"

/-- The initial transaction ID (before any user transactions). -/
def genesis : TxId := ⟨0⟩

/-- Get the next transaction ID. -/
def next (t : TxId) : TxId := ⟨t.id + 1⟩

end TxId

end Ledger
