/-
  Ledger.Pull.Pattern

  Pull pattern types for entity-centric data retrieval.
  Supports attribute selection, wildcards, nested pulls, and reverse references.
-/

import Ledger.Core.Attribute

namespace Ledger

/-- A pull pattern specifies what data to retrieve for an entity. -/
inductive PullPattern where
  /-- Pull a single attribute. -/
  | attr (a : Attribute)
  /-- Pull all attributes (wildcard). -/
  | wildcard
  /-- Pull an attribute and recursively pull the referenced entity. -/
  | nested (a : Attribute) (subpattern : List PullPattern)
  /-- Pull reverse references - entities that reference this one via the attribute. -/
  | reverse (a : Attribute) (subpattern : List PullPattern)
  /-- Pull with a limit on cardinality-many values. -/
  | limited (a : Attribute) (limit : Nat)
  /-- Pull with a default value if attribute is missing. -/
  | withDefault (a : Attribute) (default : String)
  deriving Repr, Inhabited

namespace PullPattern

/-- Create a simple attribute pull. -/
def attr' (name : String) : PullPattern := .attr (Attribute.mk name)

/-- Create a nested pull. -/
def nested' (name : String) (sub : List PullPattern) : PullPattern :=
  .nested (Attribute.mk name) sub

/-- Create a reverse reference pull.
    In Datomic, this is written as `_:person/friend` to find who has this entity as a friend. -/
def reverse' (name : String) (sub : List PullPattern) : PullPattern :=
  .reverse (Attribute.mk name) sub

/-- Check if this is a wildcard pattern. -/
def isWildcard : PullPattern → Bool
  | .wildcard => true
  | _ => false

/-- Get the attribute if this pattern has one. -/
def getAttribute? : PullPattern → Option Attribute
  | .attr a => some a
  | .nested a _ => some a
  | .reverse a _ => some a
  | .limited a _ => some a
  | .withDefault a _ => some a
  | .wildcard => none

/-- Check if this is a nested pattern (follows references). -/
def isNested : PullPattern → Bool
  | .nested _ _ => true
  | .reverse _ _ => true
  | _ => false

end PullPattern

/-- A pull specification is a list of patterns. -/
abbrev PullSpec := List PullPattern

namespace PullSpec

/-- Create a pull spec from attribute names. -/
def fromAttrs (names : List String) : PullSpec :=
  names.map fun n => .attr (Attribute.mk n)

/-- Create a pull spec with a wildcard (all attributes). -/
def all : PullSpec := [.wildcard]

/-- Check if this spec includes a wildcard. -/
def hasWildcard (spec : PullSpec) : Bool :=
  spec.any PullPattern.isWildcard

end PullSpec

end Ledger
