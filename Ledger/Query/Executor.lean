/-
  Ledger.Query.Executor

  Query execution engine.
  Executes Datalog queries against the database, producing result relations.
-/

import Std.Data.HashMap
import Ledger.Core.Datom
import Ledger.Index.Manager
import Ledger.Db.Database
import Ledger.Query.AST
import Ledger.Query.Binding
import Ledger.Query.Unify
import Ledger.Query.Predicate
import Ledger.Query.PredicateEval
import Ledger.Query.IndexSelect
import Ledger.Query.Rules

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
private def boundValueCompatible (a b : BoundValue) : Bool :=
  if a == b then true
  else
    match a.asEntity?, b.asEntity? with
    | some e1, some e2 => e1 == e2
    | _, _ =>
      match a.asValue?, b.asValue? with
      | some v1, some v2 => v1 == v2
      | _, _ => false

private def unifyTermWithValue (term : Term) (val : BoundValue) (b : Binding) : Option Binding :=
  match term with
  | .blank => some b
  | .entity e =>
    match val.asEntity? with
    | some e' => if e == e' then some b else none
    | none => none
  | .attr a =>
    match val with
    | .attr a' => if a == a' then some b else none
    | _ => none
  | .value v =>
    match val.asValue? with
    | some v' => if v == v' then some b else none
    | none => none
  | .var v =>
    match b.lookup v with
    | none => some (b.bind v val)
    | some existing => if boundValueCompatible existing val then some b else none

private def bindingValues (params : List Var) (b : Binding) : Option (List BoundValue) :=
  params.mapM fun v => b.lookup v

private def bindingFromValues (params : List Var) (values : List BoundValue) : Binding :=
  Binding.ofList (params.zip values)

private def varsInAllBindings (rel : Relation) : List Var :=
  match rel.bindings with
  | [] => []
  | b :: rest =>
    rest.foldl (init := b.vars) fun acc b' =>
      acc.filter (b'.isBound ·)

private def intersectVars (a b : List Var) : List Var :=
  a.filter (b.contains ·)

private def commonClauseVars (clauses : List Clause) : List Var :=
  match clauses with
  | [] => []
  | c :: rest =>
    rest.foldl (init := c.vars) fun acc c' =>
      intersectVars acc c'.vars

private def executeRuleCall (call : RuleCall) (input : Relation) (rules : RuleEnv) : Relation :=
  let key := RuleKey.ofName call.name call.arity
  match rules.get? key with
  | none => Relation.empty
  | some table =>
    input.flatMap fun b =>
      table.relation.bindings.filterMap fun rb =>
        match bindingValues table.params rb with
        | none => none
        | some vals =>
          let pairs := call.args.zip vals
          pairs.foldlM (init := b) (fun acc (arg, val) => unifyTermWithValue arg val acc)

def executeClause (clause : Clause) (input : Relation) (idx : Indexes) (rules : RuleEnv) : Relation :=
  match clause with
  | .pattern p =>
    -- For each input binding, find matching datoms and extend the binding
    input.flatMap fun b =>
      let candidates := IndexSelect.fetchCandidates p b idx
      (Unify.matchPattern p candidates b).bindings
  | .predicate p =>
    input.filter fun b => Predicate.eval p b
  | .rule call =>
    executeRuleCall call input rules
  | .and clauses =>
    -- Chain clauses: output of one becomes input of next
    clauses.foldl (init := input) fun rel clause' =>
      executeClause clause' rel idx rules
  | .or clauses =>
    -- Union with shared variable scoping and deduplication
    let inputVars := varsInAllBindings input
    let sharedBranchVars := commonClauseVars clauses
    let sharedVars := inputVars ++ sharedBranchVars.filter (fun v => !inputVars.contains v)
    let results := clauses.flatMap fun c =>
      let rel := executeClause c input idx rules
      rel.bindings.filterMap fun b =>
        match bindingValues sharedVars b with
        | some vals => some (bindingFromValues sharedVars vals)
        | none => none
    Relation.distinct ⟨results⟩
  | .not innerClause =>
    -- Negation-as-failure: keep bindings where inner clause produces no matches
    input.filter fun b =>
      let innerInput := Relation.singleton b
      let innerResult := executeClause innerClause innerInput idx rules
      innerResult.isEmpty

private def projectBindingOrdered (params : List Var) (b : Binding) : Option Binding :=
  let entries? := params.mapM fun v =>
    b.lookup v |>.map fun val => (v, val)
  entries?.map Binding.ofList

private def normalizeRelation (fromParams toParams : List Var) (rel : Relation) : Relation :=
  let bindings := rel.bindings.filterMap fun b =>
    match bindingValues fromParams b with
    | some vals => some (bindingFromValues toParams vals)
    | none => none
  Relation.distinct ⟨bindings⟩

private def executeRuleDef (rule : RuleDef) (idx : Indexes) (rules : RuleEnv) : Relation :=
  let initial := Relation.singleton Binding.empty
  let rel := rule.body.foldl (init := initial) fun acc clause =>
    executeClause clause acc idx rules
  let projected := rel.bindings.filterMap (projectBindingOrdered rule.params)
  Relation.distinct ⟨projected⟩

private def buildRuleEnv (ruleDefs : List RuleDef) (idx : Indexes) : RuleEnv := Id.run do
  if ruleDefs.isEmpty then
    return RuleEnv.empty

  let mut groups : Std.HashMap RuleKey (List RuleDef) := {}
  for rule in ruleDefs do
    let key := RuleKey.ofName rule.name rule.arity
    let existing := match groups[key]? with
      | some defs => defs
      | none => []
    groups := groups.insert key (rule :: existing)

  let mut env : RuleEnv := {}
  for (key, _) in groups.toList do
    let params := RuleKey.canonicalParams key
    env := env.insert key { params := params, relation := Relation.empty }

  let mut changed := true
  while changed do
    changed := false
    for (key, defs) in groups.toList do
      let table := match env[key]? with
        | some t => t
        | none => { params := RuleKey.canonicalParams key, relation := Relation.empty }
      let mut merged := table.relation
      for defn in defs do
        let rel := executeRuleDef defn idx env
        let normalized := normalizeRelation defn.params table.params rel
        merged := Relation.distinct ⟨merged.bindings ++ normalized.bindings⟩
      if merged.size > table.relation.size then
        changed := true
      env := env.insert key { table with relation := merged }

  return env

/-- Execute a full query against the database. -/
def execute (query : Query) (db : Db) : QueryResult :=
  -- Start with a singleton empty binding
  let initial := Relation.singleton Binding.empty
  let ruleEnv := buildRuleEnv query.rules db.indexes

  -- Execute all where clauses, threading through the relation
  let relation := query.where_.foldl (init := initial) fun rel clause =>
    executeClause clause rel db.indexes ruleEnv

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
  let ruleEnv := buildRuleEnv query.rules db.indexes
  -- Execute all where clauses, threading through the relation
  query.where_.foldl (init := initial) fun rel clause =>
    executeClause clause rel db.indexes ruleEnv

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
