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

/-- Execute a clause against the database. -/
def executeClause (clause : Clause) (idx : Indexes) : Relation :=
  match clause with
  | .pattern p => executePattern p Binding.empty idx
  | .and clauses =>
    match clauses with
    | [] => Relation.singleton Binding.empty
    | [c] => executeClause c idx
    | c :: cs =>
      let first := executeClause c idx
      cs.foldl (init := first) fun rel clause' =>
        let clauseRel := executeClause clause' idx
        Relation.join rel clauseRel
  | .or clauses =>
    let results := (clauses.map fun c => (executeClause c idx).bindings).flatten
    ⟨results⟩
  | .not _ =>
    -- Negation requires full scan and exclusion
    -- For now, simplified: returns empty (proper impl needs stratification)
    Relation.empty

/-- Execute a full query against the database. -/
def execute (query : Query) (db : Db) : QueryResult :=
  -- Extract all patterns from where clauses
  let patterns := (query.where_.map Clause.patterns).flatten

  -- Execute patterns with join optimization
  let relation := executePatterns patterns db.indexes

  -- Project to find variables and remove duplicates
  let projected := relation.project query.find |>.distinct

  { columns := query.find
  , rows := projected }

/-- Execute a query and return raw bindings. -/
def executeRaw (query : Query) (db : Db) : Relation :=
  let result := execute query db
  result.rows

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
