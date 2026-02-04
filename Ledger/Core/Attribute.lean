/-
  Ledger.Core.Attribute

  Attribute identifiers for the fact database.
  In schema-free mode, any string can be used as an attribute.
-/

namespace Ledger

/-- Attribute identifier - represents the property being asserted.
    Uses keyword-style naming convention (e.g., ":person/name", ":movie/title").
    In schema-free mode, any attribute can be used without prior declaration. -/
structure Attribute where
  name : String
  deriving Repr, DecidableEq, Hashable, Inhabited

namespace Attribute

instance : Ord Attribute where
  compare a b := compare a.name b.name

instance : LT Attribute where
  lt a b := a.name < b.name

instance : LE Attribute where
  le a b := a.name <= b.name

instance : ToString Attribute where
  toString a := a.name

/-- Create an attribute using keyword syntax (adds colon prefix if missing). -/
def keyword (ns : String) (name : String) : Attribute :=
  ⟨s!":{ns}/{name}"⟩

/-- Built-in attribute: entity identifier (unique name for lookup). -/
def dbIdent : Attribute := ⟨":db/ident"⟩

/-- Built-in attribute: documentation string. -/
def dbDoc : Attribute := ⟨":db/doc"⟩

/-- Built-in attribute: transaction instant (timestamp). -/
def dbTxInstant : Attribute := ⟨":db/txInstant"⟩

end Attribute

end Ledger
