/-
  Ledger.DSL.SchemaBuilder

  Fluent DSL for defining schemas.
-/

import Ledger.Schema.Types
import Ledger.Schema.Install
import Ledger.Tx.Types

namespace Ledger.DSL

/-- Builder for creating attribute schemas with a fluent API. -/
structure SchemaBuilder where
  attrs : List AttributeSchema := []
  deriving Inhabited

namespace SchemaBuilder

def new : SchemaBuilder := {}

/-- Define a string attribute. -/
def string (sb : SchemaBuilder) (name : String) : SchemaBuilder :=
  { sb with attrs := sb.attrs ++ [{ ident := ⟨name⟩, valueType := .string }] }

/-- Define an int attribute. -/
def int (sb : SchemaBuilder) (name : String) : SchemaBuilder :=
  { sb with attrs := sb.attrs ++ [{ ident := ⟨name⟩, valueType := .int }] }

/-- Define a float attribute. -/
def float (sb : SchemaBuilder) (name : String) : SchemaBuilder :=
  { sb with attrs := sb.attrs ++ [{ ident := ⟨name⟩, valueType := .float }] }

/-- Define a bool attribute. -/
def bool (sb : SchemaBuilder) (name : String) : SchemaBuilder :=
  { sb with attrs := sb.attrs ++ [{ ident := ⟨name⟩, valueType := .bool }] }

/-- Define an instant (timestamp) attribute. -/
def instant (sb : SchemaBuilder) (name : String) : SchemaBuilder :=
  { sb with attrs := sb.attrs ++ [{ ident := ⟨name⟩, valueType := .instant }] }

/-- Define a ref (entity reference) attribute. -/
def ref (sb : SchemaBuilder) (name : String) : SchemaBuilder :=
  { sb with attrs := sb.attrs ++ [{ ident := ⟨name⟩, valueType := .ref }] }

/-- Define a keyword attribute. -/
def keyword (sb : SchemaBuilder) (name : String) : SchemaBuilder :=
  { sb with attrs := sb.attrs ++ [{ ident := ⟨name⟩, valueType := .keyword }] }

/-- Define a bytes attribute. -/
def bytes (sb : SchemaBuilder) (name : String) : SchemaBuilder :=
  { sb with attrs := sb.attrs ++ [{ ident := ⟨name⟩, valueType := .bytes }] }

/-- Set the last attribute to cardinality many. -/
def many (sb : SchemaBuilder) : SchemaBuilder :=
  match sb.attrs.reverse with
  | [] => sb
  | last :: rest =>
    { sb with attrs := rest.reverse ++ [{ last with cardinality := .many }] }

/-- Set the last attribute as unique identity (lookup key). -/
def uniqueIdentity (sb : SchemaBuilder) : SchemaBuilder :=
  match sb.attrs.reverse with
  | [] => sb
  | last :: rest =>
    { sb with attrs := rest.reverse ++ [{ last with unique := some .identity, indexed := true }] }

/-- Set the last attribute as unique value (no duplicates). -/
def uniqueValue (sb : SchemaBuilder) : SchemaBuilder :=
  match sb.attrs.reverse with
  | [] => sb
  | last :: rest =>
    { sb with attrs := rest.reverse ++ [{ last with unique := some .value, indexed := true }] }

/-- Set the last attribute as indexed (for AVET lookups). -/
def indexed (sb : SchemaBuilder) : SchemaBuilder :=
  match sb.attrs.reverse with
  | [] => sb
  | last :: rest =>
    { sb with attrs := rest.reverse ++ [{ last with indexed := true }] }

/-- Set the last attribute as a component ref (cascading delete). -/
def component (sb : SchemaBuilder) : SchemaBuilder :=
  match sb.attrs.reverse with
  | [] => sb
  | last :: rest =>
    { sb with attrs := rest.reverse ++ [{ last with component := true }] }

/-- Add documentation to the last attribute. -/
def doc (sb : SchemaBuilder) (d : String) : SchemaBuilder :=
  match sb.attrs.reverse with
  | [] => sb
  | last :: rest =>
    { sb with attrs := rest.reverse ++ [{ last with doc := some d }] }

/-- Build the Schema. -/
def build (sb : SchemaBuilder) : Schema :=
  sb.attrs.foldl Schema.insert Schema.empty

/-- Generate transaction ops to install schema into database. -/
def installOps (sb : SchemaBuilder) (startEid : EntityId) : Transaction :=
  Schema.installAllOps sb.attrs startEid

end SchemaBuilder

/-- Entry point for schema DSL. -/
def schema : SchemaBuilder := SchemaBuilder.new

end Ledger.DSL
