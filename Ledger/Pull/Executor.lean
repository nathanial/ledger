/-
  Ledger.Pull.Executor

  Pull API executor.
  Retrieves hierarchical entity data based on pull patterns.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Db.Database
import Ledger.Pull.Pattern
import Ledger.Pull.Result

namespace Ledger

namespace Pull

/-- Configuration for pull execution. -/
structure PullConfig where
  /-- Maximum recursion depth for nested pulls (prevents infinite loops). -/
  maxDepth : Nat := 10
  /-- Whether to include the entity ID in results. -/
  includeId : Bool := false
  deriving Repr, Inhabited

/-- State tracked during pull execution. -/
structure PullState where
  /-- Current recursion depth. -/
  depth : Nat := 0
  /-- Entities already visited (for cycle detection). -/
  visited : List EntityId := []
  deriving Repr, Inhabited

namespace PullState

/-- Check if we've exceeded max depth. -/
def atMaxDepth (s : PullState) (config : PullConfig) : Bool :=
  s.depth >= config.maxDepth

/-- Check if an entity has been visited. -/
def hasVisited (s : PullState) (e : EntityId) : Bool :=
  s.visited.contains e

/-- Mark an entity as visited and increment depth. -/
def visit (s : PullState) (e : EntityId) : PullState :=
  { s with depth := s.depth + 1, visited := e :: s.visited }

end PullState

/-- Get all values for an entity's attribute from the database.
    Filters out retracted values. -/
def getAttrValues (db : Db) (e : EntityId) (a : Attribute) : List Value :=
  db.get e a

/-- Pull a single attribute for an entity. -/
def pullAttr (db : Db) (e : EntityId) (a : Attribute) : Option PullValue :=
  let values := getAttrValues db e a
  match values with
  | [] => none
  | [v] => some (PullValue.fromValue v)
  | vs => some (.many (vs.map PullValue.fromValue))

/-- Get all attributes for an entity (for wildcard). -/
def getAllAttrs (db : Db) (e : EntityId) : List Attribute :=
  let datoms := db.entity e
  -- Filter for assertions and extract unique attributes
  let attrs := datoms.filter (·.added) |>.map (·.attr)
  attrs.foldl (init := []) fun acc a =>
    if acc.contains a then acc else a :: acc

/-- Pull all attributes for an entity (wildcard pattern). -/
def pullWildcard (db : Db) (e : EntityId) : PullEntity :=
  let attrs := getAllAttrs db e
  attrs.filterMap fun a =>
    match pullAttr db e a with
    | some v => some (a, v)
    | none => none

mutual

/-- Pull nested entity data for a reference value. -/
partial def pullNestedEntity (db : Db) (refEntity : EntityId) (subpatterns : List PullPattern)
    (config : PullConfig) (state : PullState) : PullValue :=
  if state.atMaxDepth config || state.hasVisited refEntity then
    -- At max depth or cycle: just return the reference
    .ref refEntity
  else
    let newState := state.visit refEntity
    let data := subpatterns.filterMap fun p =>
      pullPatternRec db refEntity p config newState
    .entity data

/-- Pull a single pattern for an entity. -/
partial def pullPatternRec (db : Db) (e : EntityId) (pattern : PullPattern)
    (config : PullConfig) (state : PullState) : Option (Attribute × PullValue) :=
  match pattern with
  | .attr a =>
    match pullAttr db e a with
    | some v => some (a, v)
    | none => none

  | .wildcard =>
    -- Wildcard returns multiple attributes, handled separately
    none

  | .nested a subpatterns =>
    let values := getAttrValues db e a
    let refs := values.filterMap fun v =>
      match v with
      | .ref refE => some refE
      | _ => none
    match refs with
    | [] => none
    | [refE] =>
      let nested := pullNestedEntity db refE subpatterns config state
      some (a, nested)
    | refEs =>
      let nestedValues := refEs.map fun refE =>
        pullNestedEntity db refE subpatterns config state
      some (a, .many nestedValues)

  | .reverse a subpatterns =>
    -- Find entities that reference this entity via attribute a
    let referers := db.referencingViaAttr e a
    match referers with
    | [] => none
    | refs =>
      let nestedValues := refs.map fun refE =>
        pullNestedEntity db refE subpatterns config state
      some (a, .many nestedValues)

  | .limited a limit =>
    let values := getAttrValues db e a
    let limited := values.take limit
    match limited with
    | [] => none
    | [v] => some (a, PullValue.fromValue v)
    | vs => some (a, .many (vs.map PullValue.fromValue))

  | .withDefault a defaultStr =>
    match pullAttr db e a with
    | some v => some (a, v)
    | none => some (a, .scalar (.string defaultStr))

end

/-- Execute a pull specification on an entity. -/
def pull (db : Db) (e : EntityId) (spec : PullSpec)
    (config : PullConfig := {}) : PullResult :=
  let state : PullState := {}

  -- Check if spec includes wildcard
  let wildcardData := if spec.hasWildcard
    then pullWildcard db e
    else []

  -- Pull specific patterns (excluding wildcards)
  let patternData := spec.filterMap fun p =>
    if p.isWildcard then none
    else pullPatternRec db e p config state

  -- Combine wildcard data with pattern data (pattern data takes precedence)
  let combinedData := patternData ++ wildcardData.filter fun (a, _) =>
    !patternData.any fun (a', _) => a' == a

  { entity := e, data := combinedData }

/-- Pull multiple entities with the same spec. -/
def pullMany (db : Db) (entities : List EntityId) (spec : PullSpec)
    (config : PullConfig := {}) : List PullResult :=
  entities.map fun e => pull db e spec config

/-- Convenience: Pull a single attribute as a value.
    If there are multiple values, returns the most recent one (first in list). -/
def pullOne (db : Db) (e : EntityId) (attrName : String) : Option Value :=
  let result := pull db e [.attr (Attribute.mk attrName)]
  match result.get? (Attribute.mk attrName) with
  | some (.scalar v) => some v
  | some (.ref refE) => some (.ref refE)
  | some (.many ((.scalar v) :: _)) => some v
  | some (.many ((.ref refE) :: _)) => some (.ref refE)
  | _ => none

/-- Convenience: Pull with attribute names. -/
def pullAttrs (db : Db) (e : EntityId) (attrNames : List String) : PullResult :=
  pull db e (PullSpec.fromAttrs attrNames)

end Pull

end Ledger
