/-
  Ledger.Query.Var

  Logic variable definition for queries.
-/

namespace Ledger

/-- A logic variable, identified by name (e.g., "?e", "?name"). -/
structure Var where
  name : String
  deriving Repr, BEq, Hashable, Ord, Inhabited

instance : ToString Var where
  toString v := s!"?{v.name}"

namespace Var

/-- Create a variable from a name (without the ? prefix). -/
def ofName (name : String) : Var := ⟨name⟩

end Var

end Ledger
