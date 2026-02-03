/-
  Ledger.Query.Executor

  Query execution engine.
  Executes Datalog queries against the database, producing result relations.
-/

import Ledger.Core.Datom
import Ledger.Index.Manager
import Ledger.Db.Database
import Ledger.Query.AST
import Ledger.Query.Binding
import Ledger.Query.Unify
import Ledger.Query.Predicate
import Ledger.Query.PredicateEval
import Ledger.Query.IndexSelect

namespace Ledger

namespace Query

/-- Result of a query - a relation with column names. -/
structure QueryResult where
  /-- Column names (the :find variables). -/
  columns : List Var
  /-- Result rows. -/
  rows : Relation
  deriving Repr, Inhabited

namespace QueryResult

/-- Number of result rows. -/
def size (r : QueryResult) : Nat := r.rows.size

/-- Check if empty. -/
def isEmpty (r : QueryResult) : Bool := r.rows.isEmpty

/-- Convert to list of value tuples. -/
def toTuples (r : QueryResult) : List (List (Option BoundValue)) :=
  r.rows.bindings.map fun b => r.columns.map (b.lookup ·)

end QueryResult

/-- Execute a single pattern against the database indexes.
    Uses index selection for efficiency. -/
def executePattern (pattern : Pattern) (b : Binding) (idx : Indexes) : Relation :=
  let candidates := IndexSelect.fetchCandidates pattern b idx
  Unify.matchPattern pattern candidates b

/-- Order patterns by selectivity for more efficient execution.
    More bound patterns should execute first. -/
def orderPatterns (patterns : List Pattern) (b : Binding) : List Pattern :=
  patterns.toArray.qsort (fun p1 p2 =>
    IndexSelect.selectivity p1 b < IndexSelect.selectivity p2 b
  ) |>.toList

/-- Execute a list of patterns with join optimization.
    Executes most selective patterns first and joins results. -/
def executePatterns (patterns : List Pattern) (idx : Indexes) : Relation :=
  match patterns with
  | [] => Relation.singleton Binding.empty
  | [p] => executePattern p Binding.empty idx
  | ps =>
    -- Order by selectivity
    let ordered := orderPatterns ps Binding.empty
    -- Execute first pattern
    match ordered with
    | [] => Relation.empty
    | first :: rest =>
      let initial := executePattern first Binding.empty idx
      -- Join with remaining patterns
      rest.foldl (init := initial) fun relation pattern =>
        -- For each binding in the relation, execute the pattern and collect results
        relation.flatMap fun b =>
          let candidates := IndexSelect.fetchCandidates pattern b idx
          (Unify.matchPattern pattern candidates b).bindings

/-- Execute a clause against the database, transforming an input relation.
    Each clause type transforms the input bindings:
    - pattern: joins input with matching datoms
    - and: chains clauses sequentially
    - or: unions results from each branch
    - not: filters out bindings that match the inner clause -/
def executeClause (clause : Clause) (input : Relation) (idx : Indexes) : Relation :=
  match clause with
  | .pattern p =>
    -- For each input binding, find matching datoms and extend the binding
    input.flatMap fun b =>
      let candidates := IndexSelect.fetchCandidates p b idx
      (Unify.matchPattern p candidates b).bindings
  | .predicate p =>
    input.filter fun b => Predicate.eval p b
  | .and clauses =>
    -- Chain clauses: output of one becomes input of next
    clauses.foldl (init := input) fun rel clause' =>
      executeClause clause' rel idx
  | .or clauses =>
    -- Union: collect results from each branch with same input
    let results := clauses.flatMap fun c =>
      (executeClause c input idx).bindings
    ⟨results⟩
  | .not innerClause =>
    -- Negation-as-failure: keep bindings where inner clause produces no matches
    input.filter fun b =>
      let innerInput := Relation.singleton b
      let innerResult := executeClause innerClause innerInput idx
      innerResult.isEmpty

/-- Execute a full query against the database. -/
def execute (query : Query) (db : Db) : QueryResult :=
  -- Start with a singleton empty binding
  let initial := Relation.singleton Binding.empty

  -- Execute all where clauses, threading through the relation
  let relation := query.where_.foldl (init := initial) fun rel clause =>
    executeClause clause rel db.indexes

  -- Project to find variables and remove duplicates
  let projected := relation.project query.find |>.distinct

  { columns := query.find
  , rows := projected }

/-- Execute a query and return raw bindings. -/
def executeRaw (query : Query) (db : Db) : Relation :=
  let result := execute query db
  result.rows

/-- Execute query clauses without projection or distinct.
    Useful for aggregations where we need all matching bindings. -/
def executeForAggregate (query : Query) (db : Db) : Relation :=
  -- Start with a singleton empty binding
  let initial := Relation.singleton Binding.empty
  -- Execute all where clauses, threading through the relation
  query.where_.foldl (init := initial) fun rel clause =>
    executeClause clause rel db.indexes

/-- Convenience: Execute a simple pattern query. -/
def findPattern (pattern : Pattern) (findVars : List String) (db : Db) : QueryResult :=
  let query : Query := {
    find := findVars.map Var.ofName
    where_ := [.pattern pattern]
  }
  execute query db

/-- Convenience: Find entities matching a pattern. -/
def findEntities (pattern : Pattern) (db : Db) : List EntityId :=
  let relation := executePattern pattern Binding.empty db.indexes
  relation.bindings.filterMap fun b =>
    match pattern.entity with
    | .var v =>
      match b.lookup v with
      | some (.entity e) => some e
      | some (.value (.ref e)) => some e
      | _ => none
    | .entity e => some e
    | _ => none

end Query

end Ledger
