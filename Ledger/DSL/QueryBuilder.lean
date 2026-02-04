/-
  Ledger.DSL.QueryBuilder

  Fluent builder API for constructing Datalog queries.
  Provides a more ergonomic way to build queries without macros.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Query.AST
import Ledger.Query.Binding
import Ledger.Query.Executor
import Ledger.Query.Aggregates
import Ledger.Query.Predicate
import Ledger.Db.Database

namespace Ledger

namespace DSL

/-- Query builder for fluent query construction. -/
structure QueryBuilder where
  findVars : List Var := []
  patterns : List Pattern := []
  predicates : List Query.Predicate := []
  ruleCalls : List RuleCall := []
  rules : List RuleDef := []
  groupVars : List Var := []
  aggregates : List AggregateSpec := []
  deriving Repr, Inhabited

namespace QueryBuilder

/-- Create a new query builder. -/
def new : QueryBuilder := {}

/-- Add a variable to the find clause. -/
def find (qb : QueryBuilder) (varName : String) : QueryBuilder :=
  { qb with findVars := qb.findVars ++ [⟨varName⟩] }

/-- Add multiple variables to the find clause. -/
def findAll (qb : QueryBuilder) (varNames : List String) : QueryBuilder :=
  { qb with findVars := qb.findVars ++ varNames.map Var.mk }

/-- Add a pattern to the where clause using a variable for entity. -/
def where_ (qb : QueryBuilder) (eVar : String) (attr : String) (vVar : String) : QueryBuilder :=
  let pattern : Pattern := {
    entity := .var ⟨eVar⟩
    attr := .attr (Attribute.mk attr)
    value := .var ⟨vVar⟩
  }
  { qb with patterns := qb.patterns ++ [pattern] }

/-- Add a pattern with a constant entity. -/
def whereEntity (qb : QueryBuilder) (e : EntityId) (attr : String) (vVar : String) : QueryBuilder :=
  let pattern : Pattern := {
    entity := .entity e
    attr := .attr (Attribute.mk attr)
    value := .var ⟨vVar⟩
  }
  { qb with patterns := qb.patterns ++ [pattern] }

/-- Add a pattern with a constant value (int). -/
def whereInt (qb : QueryBuilder) (eVar : String) (attr : String) (v : Int) : QueryBuilder :=
  let pattern : Pattern := {
    entity := .var ⟨eVar⟩
    attr := .attr (Attribute.mk attr)
    value := .value (.int v)
  }
  { qb with patterns := qb.patterns ++ [pattern] }

/-- Add a pattern with a constant value (string). -/
def whereStr (qb : QueryBuilder) (eVar : String) (attr : String) (v : String) : QueryBuilder :=
  let pattern : Pattern := {
    entity := .var ⟨eVar⟩
    attr := .attr (Attribute.mk attr)
    value := .value (.string v)
  }
  { qb with patterns := qb.patterns ++ [pattern] }

/-- Add a pattern with a constant value (ref). -/
def whereRef (qb : QueryBuilder) (eVar : String) (attr : String) (ref : EntityId) : QueryBuilder :=
  let pattern : Pattern := {
    entity := .var ⟨eVar⟩
    attr := .attr (Attribute.mk attr)
    value := .entity ref
  }
  { qb with patterns := qb.patterns ++ [pattern] }

/-- Add a predicate clause to the query. -/
def wherePred (qb : QueryBuilder) (pred : Query.Predicate) : QueryBuilder :=
  { qb with predicates := qb.predicates ++ [pred] }

/-- Add a rule call clause to the query. -/
def whereRule (qb : QueryBuilder) (name : String) (args : List Term) : QueryBuilder :=
  let call : RuleCall := { name := name, args := args }
  { qb with ruleCalls := qb.ruleCalls ++ [call] }

/-- Add a rule definition. -/
def addRule (qb : QueryBuilder) (rule : RuleDef) : QueryBuilder :=
  { qb with rules := qb.rules ++ [rule] }

/-- Add a group-by variable. -/
def groupBy (qb : QueryBuilder) (varName : String) : QueryBuilder :=
  { qb with groupVars := qb.groupVars ++ [⟨varName⟩] }

/-- Add multiple group-by variables. -/
def groupByAll (qb : QueryBuilder) (varNames : List String) : QueryBuilder :=
  { qb with groupVars := qb.groupVars ++ varNames.map Var.mk }

/-- Add a count aggregate (counts all rows). -/
def count (qb : QueryBuilder) (resultName : String := "count") : QueryBuilder :=
  { qb with aggregates := qb.aggregates ++ [AggregateSpec.count resultName] }

/-- Add a count aggregate for a specific variable. -/
def countVar (qb : QueryBuilder) (varName : String) (resultName : String := "count") : QueryBuilder :=
  { qb with aggregates := qb.aggregates ++ [AggregateSpec.countVar varName resultName] }

/-- Add a sum aggregate. -/
def sum (qb : QueryBuilder) (varName : String) (resultName : String := "sum") : QueryBuilder :=
  { qb with aggregates := qb.aggregates ++ [AggregateSpec.sum varName resultName] }

/-- Add an avg aggregate. -/
def avg (qb : QueryBuilder) (varName : String) (resultName : String := "avg") : QueryBuilder :=
  { qb with aggregates := qb.aggregates ++ [AggregateSpec.avg varName resultName] }

/-- Add a min aggregate. -/
def min (qb : QueryBuilder) (varName : String) (resultName : String := "min") : QueryBuilder :=
  { qb with aggregates := qb.aggregates ++ [AggregateSpec.min varName resultName] }

/-- Add a max aggregate. -/
def max (qb : QueryBuilder) (varName : String) (resultName : String := "max") : QueryBuilder :=
  { qb with aggregates := qb.aggregates ++ [AggregateSpec.max varName resultName] }

/-- Check if this query has aggregates. -/
def hasAggregates (qb : QueryBuilder) : Bool :=
  !qb.aggregates.isEmpty

/-- Build the query. -/
def build (qb : QueryBuilder) : Query :=
  let clauses : List Clause :=
    qb.patterns.map Clause.pattern
      ++ qb.predicates.map Clause.predicate
      ++ qb.ruleCalls.map Clause.rule
  { find := qb.findVars
  , where_ := clauses
  , rules := qb.rules }

/-- Build and execute the query. -/
def run (qb : QueryBuilder) (db : Db) : Query.QueryResult :=
  Query.execute qb.build db

/-- Build and execute, returning just the bindings. -/
def runRaw (qb : QueryBuilder) (db : Db) : Relation :=
  Query.executeRaw qb.build db

/-- Build and execute with aggregation, returning aggregate results. -/
def runAggregate (qb : QueryBuilder) (db : Db) : Aggregate.AggregateResult :=
  -- For aggregations, we need all matching bindings (no projection/distinct)
  let baseResult := Query.executeForAggregate qb.build db
  Aggregate.execute baseResult qb.groupVars qb.aggregates

/-- Build and execute with aggregation, returning as a relation. -/
def runAggregateRaw (qb : QueryBuilder) (db : Db) : Relation :=
  (qb.runAggregate db).toRelation

end QueryBuilder

/-- Shorthand for creating a query builder. -/
def query : QueryBuilder := QueryBuilder.new

/-- Quick query: find entities with a specific attribute. -/
def entitiesWithAttr (db : Db) (attr : String) : List EntityId :=
  let pattern : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr (Attribute.mk attr)
    value := .blank
  }
  Query.findEntities pattern db

/-- Quick query: find entities where attr = value (int). -/
def entitiesWithAttrValueInt (db : Db) (attr : String) (v : Int) : List EntityId :=
  let pattern : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr (Attribute.mk attr)
    value := .value (.int v)
  }
  Query.findEntities pattern db

/-- Quick query: find entities where attr = value (string). -/
def entitiesWithAttrValueStr (db : Db) (attr : String) (v : String) : List EntityId :=
  let pattern : Pattern := {
    entity := .var ⟨"e"⟩
    attr := .attr (Attribute.mk attr)
    value := .value (.string v)
  }
  Query.findEntities pattern db

/-- Quick query: find entity by unique attribute value. -/
def entityWithAttrValueStr (db : Db) (attr : String) (v : String) : Option EntityId :=
  (entitiesWithAttrValueStr db attr v).head?

@[deprecated "use entitiesWithAttr" (since := "2026-02-04")]
def findEntitiesWith (db : Db) (attr : String) : List EntityId :=
  entitiesWithAttr db attr

@[deprecated "use entitiesWithAttrValueInt" (since := "2026-02-04")]
def findEntitiesWhereInt (db : Db) (attr : String) (v : Int) : List EntityId :=
  entitiesWithAttrValueInt db attr v

@[deprecated "use entitiesWithAttrValueStr" (since := "2026-02-04")]
def findEntitiesWhereStr (db : Db) (attr : String) (v : String) : List EntityId :=
  entitiesWithAttrValueStr db attr v

@[deprecated "use entityWithAttrValueStr" (since := "2026-02-04")]
def findEntityByStr (db : Db) (attr : String) (v : String) : Option EntityId :=
  entityWithAttrValueStr db attr v

end DSL

end Ledger
