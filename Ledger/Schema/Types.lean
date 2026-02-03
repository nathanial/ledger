/-
  Ledger.Schema.Types

  Schema definitions for attribute constraints.
  Schema attributes can be stored as entities in the database using :db/* attributes.
-/

import Std.Data.HashMap
import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value

namespace Ledger

/-- Allowed value types for a schema-constrained attribute. -/
inductive ValueType where
  | int
  | float
  | string
  | bool
  | instant
  | ref
  | keyword
  | bytes
  deriving Repr, DecidableEq, Inhabited

namespace ValueType

/-- Convert to keyword string for storage. -/
def toKeyword : ValueType → String
  | .int => ":db.type/int"
  | .float => ":db.type/float"
  | .string => ":db.type/string"
  | .bool => ":db.type/bool"
  | .instant => ":db.type/instant"
  | .ref => ":db.type/ref"
  | .keyword => ":db.type/keyword"
  | .bytes => ":db.type/bytes"

/-- Parse from keyword string. -/
def fromKeyword (s : String) : Option ValueType :=
  match s with
  | ":db.type/int" => some .int
  | ":db.type/float" => some .float
  | ":db.type/string" => some .string
  | ":db.type/bool" => some .bool
  | ":db.type/instant" => some .instant
  | ":db.type/ref" => some .ref
  | ":db.type/keyword" => some .keyword
  | ":db.type/bytes" => some .bytes
  | _ => none

/-- Check if a Value matches this type. -/
def matchesValue (vt : ValueType) (v : Value) : Bool :=
  match vt, v with
  | .int, .int _ => true
  | .float, .float _ => true
  | .string, .string _ => true
  | .bool, .bool _ => true
  | .instant, .instant _ => true
  | .ref, .ref _ => true
  | .keyword, .keyword _ => true
  | .bytes, .bytes _ => true
  | _, _ => false

instance : ToString ValueType where
  toString vt := vt.toKeyword

end ValueType


/-- Cardinality constraint for an attribute. -/
inductive Cardinality where
  /-- Single value - new assertions replace old (use retract-then-add). -/
  | one
  /-- Multiple values - values accumulate. -/
  | many
  deriving Repr, DecidableEq, Inhabited

namespace Cardinality

def toKeyword : Cardinality → String
  | .one => ":db.cardinality/one"
  | .many => ":db.cardinality/many"

def fromKeyword (s : String) : Option Cardinality :=
  match s with
  | ":db.cardinality/one" => some .one
  | ":db.cardinality/many" => some .many
  | _ => none

instance : ToString Cardinality where
  toString c := c.toKeyword

end Cardinality


/-- Uniqueness constraint for an attribute. -/
inductive Unique where
  /-- Identity - this attribute can be used to lookup entities (like a primary key). -/
  | identity
  /-- Value - no two entities can have the same value for this attribute. -/
  | value
  deriving Repr, DecidableEq, Inhabited

namespace Unique

def toKeyword : Unique → String
  | .identity => ":db.unique/identity"
  | .value => ":db.unique/value"

def fromKeyword (s : String) : Option Unique :=
  match s with
  | ":db.unique/identity" => some .identity
  | ":db.unique/value" => some .value
  | _ => none

instance : ToString Unique where
  toString u := u.toKeyword

end Unique


/-- Schema definition for a single attribute.
    This is the in-memory representation; can be stored as datoms. -/
structure AttributeSchema where
  /-- The attribute this schema defines. -/
  ident : Attribute
  /-- Required: the value type. -/
  valueType : ValueType
  /-- Cardinality (default: one). -/
  cardinality : Cardinality := .one
  /-- Optional uniqueness constraint. -/
  unique : Option Unique := none
  /-- Whether to index in AVET for value lookups (default: false for non-unique). -/
  indexed : Bool := false
  /-- Component ref (cascading delete); only meaningful for ref types. -/
  component : Bool := false
  /-- Documentation string. -/
  doc : Option String := none
  deriving Repr, Inhabited


/-- Schema is a collection of attribute schemas, keyed by attribute name.
    Stored as a HashMap for O(1) lookup during validation. -/
structure Schema where
  inner : Std.HashMap Attribute AttributeSchema
  deriving Inhabited

namespace Schema

def empty : Schema := ⟨{}⟩

def get? (schema : Schema) (attr : Attribute) : Option AttributeSchema :=
  schema.inner[attr]?

def containsAttr (schema : Schema) (attr : Attribute) : Bool :=
  schema.inner.contains attr

def insert (schema : Schema) (attrSchema : AttributeSchema) : Schema :=
  ⟨schema.inner.insert attrSchema.ident attrSchema⟩

def size (schema : Schema) : Nat :=
  schema.inner.size

def toList (schema : Schema) : List AttributeSchema :=
  schema.inner.toList.map (·.2)

end Schema


/-- Errors that can occur during schema validation. -/
inductive SchemaError where
  /-- Value type mismatch. -/
  | typeMismatch (attr : Attribute) (expected : ValueType) (actual : Value)
  /-- Cardinality-one violated: multiple values for single-valued attribute. -/
  | cardinalityViolation (entity : EntityId) (attr : Attribute)
  /-- Uniqueness constraint violated. -/
  | uniquenessViolation (attr : Attribute) (value : Value) (existingEntity : EntityId) (newEntity : EntityId)
  /-- Attribute used but not defined in schema (when strict mode enabled). -/
  | undefinedAttribute (attr : Attribute)
  /-- Invalid schema definition. -/
  | invalidSchema (msg : String)
  deriving Repr, Inhabited

namespace SchemaError

instance : ToString SchemaError where
  toString err := match err with
    | .typeMismatch attr expected actual =>
      s!"Type mismatch for {attr}: expected {expected}, got {actual}"
    | .cardinalityViolation e attr =>
      s!"Cardinality violation: entity {e} already has a value for {attr}"
    | .uniquenessViolation attr v existing newEntity =>
      s!"Uniqueness violation for {attr}={v}: entity {existing} already has this value, cannot add to {newEntity}"
    | .undefinedAttribute attr =>
      s!"Undefined attribute: {attr} (schema is in strict mode)"
    | .invalidSchema msg =>
      s!"Invalid schema: {msg}"

end SchemaError


/-- Configuration for schema validation. -/
structure SchemaConfig where
  /-- The schema to validate against. -/
  schema : Schema
  /-- If true, reject transactions with attributes not in schema. -/
  strictMode : Bool := false


-- Built-in schema attributes (stored as :db/* attributes)
namespace SchemaAttr
  def ident : Attribute := ⟨":db/ident"⟩
  def valueType_ : Attribute := ⟨":db/valueType"⟩
  def cardinality : Attribute := ⟨":db/cardinality"⟩
  def unique : Attribute := ⟨":db/unique"⟩
  def indexed : Attribute := ⟨":db/index"⟩
  def isComponent : Attribute := ⟨":db/isComponent"⟩
  def doc : Attribute := ⟨":db/doc"⟩
end SchemaAttr

end Ledger
