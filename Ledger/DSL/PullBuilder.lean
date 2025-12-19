/-
  Ledger.DSL.PullBuilder

  Fluent builder API for constructing pull patterns.
  Provides a more ergonomic way to build pull specifications.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Pull.Pattern
import Ledger.Pull.Result
import Ledger.Pull.Executor
import Ledger.Db.Database

namespace Ledger

namespace DSL

/-- Pull pattern builder for fluent construction. -/
structure PullBuilder where
  patterns : List PullPattern := []
  deriving Repr, Inhabited

namespace PullBuilder

/-- Create a new pull builder. -/
def new : PullBuilder := {}

/-- Add an attribute to pull. -/
def attr (pb : PullBuilder) (name : String) : PullBuilder :=
  { pb with patterns := pb.patterns ++ [.attr (Attribute.mk name)] }

/-- Add multiple attributes to pull. -/
def attrs (pb : PullBuilder) (names : List String) : PullBuilder :=
  { pb with patterns := pb.patterns ++ names.map (fun n => .attr (Attribute.mk n)) }

/-- Add wildcard (pull all attributes). -/
def all (pb : PullBuilder) : PullBuilder :=
  { pb with patterns := pb.patterns ++ [.wildcard] }

/-- Add a nested pull (follow reference and pull sub-attributes). -/
def nested (pb : PullBuilder) (refAttr : String) (subAttrs : List String) : PullBuilder :=
  let subPatterns := subAttrs.map (fun n => PullPattern.attr (Attribute.mk n))
  { pb with patterns := pb.patterns ++ [.nested (Attribute.mk refAttr) subPatterns] }

/-- Add a nested pull with a sub-builder. -/
def nestedWith (pb : PullBuilder) (refAttr : String) (subBuilder : PullBuilder) : PullBuilder :=
  { pb with patterns := pb.patterns ++ [.nested (Attribute.mk refAttr) subBuilder.patterns] }

/-- Add a reverse reference pull. -/
def reverse (pb : PullBuilder) (attr : String) (subAttrs : List String) : PullBuilder :=
  let subPatterns := subAttrs.map (fun n => PullPattern.attr (Attribute.mk n))
  { pb with patterns := pb.patterns ++ [.reverse (Attribute.mk attr) subPatterns] }

/-- Add a reverse reference pull with a sub-builder. -/
def reverseWith (pb : PullBuilder) (attr : String) (subBuilder : PullBuilder) : PullBuilder :=
  { pb with patterns := pb.patterns ++ [.reverse (Attribute.mk attr) subBuilder.patterns] }

/-- Add limited pull (for cardinality-many). -/
def limited (pb : PullBuilder) (attr : String) (limit : Nat) : PullBuilder :=
  { pb with patterns := pb.patterns ++ [.limited (Attribute.mk attr) limit] }

/-- Add pull with default value. -/
def withDefault (pb : PullBuilder) (attr : String) (default : String) : PullBuilder :=
  { pb with patterns := pb.patterns ++ [.withDefault (Attribute.mk attr) default] }

/-- Build the pull specification. -/
def build (pb : PullBuilder) : PullSpec := pb.patterns

/-- Build and execute the pull. -/
def run (pb : PullBuilder) (db : Db) (e : EntityId) : PullResult :=
  Pull.pull db e pb.patterns

/-- Build and execute on multiple entities. -/
def runMany (pb : PullBuilder) (db : Db) (entities : List EntityId) : List PullResult :=
  Pull.pullMany db entities pb.patterns

end PullBuilder

/-- Shorthand for creating a pull builder. -/
def pull : PullBuilder := PullBuilder.new

/-- Quick pull: get a single attribute value. -/
def getValue (db : Db) (e : EntityId) (attr : String) : Option Value :=
  Pull.pullOne db e attr

/-- Quick pull: get a string attribute. -/
def getString (db : Db) (e : EntityId) (attr : String) : Option String :=
  match getValue db e attr with
  | some (.string s) => some s
  | _ => none

/-- Quick pull: get an int attribute. -/
def getInt (db : Db) (e : EntityId) (attr : String) : Option Int :=
  match getValue db e attr with
  | some (.int i) => some i
  | _ => none

/-- Quick pull: get a bool attribute. -/
def getBool (db : Db) (e : EntityId) (attr : String) : Option Bool :=
  match getValue db e attr with
  | some (.bool b) => some b
  | _ => none

/-- Quick pull: get a reference attribute. -/
def getRef (db : Db) (e : EntityId) (attr : String) : Option EntityId :=
  match getValue db e attr with
  | some (.ref ref) => some ref
  | _ => none

end DSL

end Ledger
