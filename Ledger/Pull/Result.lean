/-
  Ledger.Pull.Result

  Result types for the Pull API.
  Represents hierarchical entity data as nested structures.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value

namespace Ledger

/-- A pulled value can be a scalar, a reference, or a nested entity. -/
inductive PullValue where
  /-- A scalar value (int, string, etc.). -/
  | scalar (v : Value)
  /-- A reference to another entity (just the ID, not pulled). -/
  | ref (e : EntityId)
  /-- A nested pulled entity. -/
  | entity (data : List (Attribute × PullValue))
  /-- Multiple values (for cardinality-many attributes). -/
  | many (values : List PullValue)
  deriving Repr, Inhabited

/-- Alias for nested pull data. -/
abbrev PullEntity := List (Attribute × PullValue)

namespace PullValue

/-- Check if this is a scalar value. -/
def isScalar : PullValue → Bool
  | .scalar _ => true
  | _ => false

/-- Check if this is a reference. -/
def isRef : PullValue → Bool
  | .ref _ => true
  | _ => false

/-- Check if this is a nested entity. -/
def isEntity : PullValue → Bool
  | .entity _ => true
  | _ => false

/-- Check if this is a many-valued result. -/
def isMany : PullValue → Bool
  | .many _ => true
  | _ => false

/-- Get the scalar value if this is a scalar. -/
def asScalar? : PullValue → Option Value
  | .scalar v => some v
  | _ => none

/-- Get the entity ID if this is a reference. -/
def asRef? : PullValue → Option EntityId
  | .ref e => some e
  | _ => none

/-- Get nested data if this is an entity. -/
def asEntity? : PullValue → Option PullEntity
  | .entity data => some data
  | _ => none

/-- Convert a Value to a PullValue. -/
def fromValue (v : Value) : PullValue :=
  match v with
  | .ref e => .ref e
  | _ => .scalar v

end PullValue

/-- A pull result is a map from attributes to pulled values. -/
structure PullResult where
  /-- The entity that was pulled. -/
  entity : EntityId
  /-- The pulled data. -/
  data : PullEntity
  deriving Repr, Inhabited

namespace PullResult

/-- Create an empty pull result. -/
def empty (e : EntityId) : PullResult := ⟨e, []⟩

/-- Add an attribute-value pair to the result. -/
def add (r : PullResult) (a : Attribute) (v : PullValue) : PullResult :=
  ⟨r.entity, (a, v) :: r.data⟩

/-- Get the value for an attribute. -/
def get? (r : PullResult) (a : Attribute) : Option PullValue :=
  (r.data.find? fun (a', _) => a' == a).map Prod.snd

/-- Get all attribute names in the result. -/
def attributes (r : PullResult) : List Attribute :=
  r.data.map Prod.fst

/-- Check if the result is empty. -/
def isEmpty (r : PullResult) : Bool :=
  r.data.isEmpty

/-- Number of attributes in the result. -/
def size (r : PullResult) : Nat :=
  r.data.length

/-- Convert to a PullValue (for nesting). -/
def toPullValue (r : PullResult) : PullValue :=
  .entity r.data

end PullResult

end Ledger
