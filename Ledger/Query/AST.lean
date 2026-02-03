/-
  Ledger.Query.AST

  Abstract syntax tree for Datalog-style queries.
  Supports pattern matching over datoms with logic variables.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Query.Var
import Ledger.Query.Predicate

namespace Ledger

/-- A term in a pattern - either a constant, variable, or blank (wildcard). -/
inductive Term where
  /-- A concrete entity ID. -/
  | entity (e : EntityId)
  /-- A concrete attribute. -/
  | attr (a : Attribute)
  /-- A concrete value. -/
  | value (v : Value)
  /-- A logic variable to be bound. -/
  | var (v : Var)
  /-- A blank/wildcard - matches anything but doesn't bind. -/
  | blank
  deriving Repr, BEq, Inhabited

namespace Term

/-- Create an entity term. -/
def e (id : Int) : Term := .entity (EntityId.mk id)

/-- Create an attribute term. -/
def a (name : String) : Term := .attr (Attribute.mk name)

/-- Create a value term from an int. -/
def int (n : Int) : Term := .value (Value.int n)

/-- Create a value term from a string. -/
def str (s : String) : Term := .value (Value.string s)

/-- Create a variable term. -/
def v (name : String) : Term := .var (Var.mk name)

/-- Check if this term is a variable. -/
def isVar : Term → Bool
  | .var _ => true
  | _ => false

/-- Check if this term is a constant (not var or blank). -/
def isConst : Term → Bool
  | .entity _ | .attr _ | .value _ => true
  | _ => false

/-- Get the variable if this is a variable term. -/
def getVar? : Term → Option Var
  | .var v => some v
  | _ => none

/-- Get variables used in this term. -/
def vars : Term → List Var
  | .var v => [v]
  | _ => []

end Term

/-- A pattern matching datoms: [entity attribute value].
    Each position can be a constant, variable, or blank. -/
structure Pattern where
  /-- Entity position (matches datom.entity). -/
  entity : Term
  /-- Attribute position (matches datom.attr). -/
  attr : Term
  /-- Value position (matches datom.value). -/
  value : Term
  deriving Repr, BEq, Inhabited

namespace Pattern

/-- Create a pattern from three terms. -/
def create (e a v : Term) : Pattern := ⟨e, a, v⟩

/-- Create a pattern with all variables. -/
def allVars (eName aName vName : String) : Pattern :=
  ⟨.var ⟨eName⟩, .var ⟨aName⟩, .var ⟨vName⟩⟩

/-- Get all variables used in this pattern. -/
def vars (p : Pattern) : List Var :=
  [p.entity.getVar?, p.attr.getVar?, p.value.getVar?].filterMap id

/-- Count the number of constants (bound positions) in this pattern. -/
def constCount (p : Pattern) : Nat :=
  [p.entity.isConst, p.attr.isConst, p.value.isConst].filter id |>.length

end Pattern

/-- A rule call clause: (rule-name arg1 arg2 ...) -/
structure RuleCall where
  name : String
  args : List Term
  deriving Repr, Inhabited

namespace RuleCall

def arity (rc : RuleCall) : Nat := rc.args.length

def vars (rc : RuleCall) : List Var :=
  (rc.args.map Term.vars).flatten.eraseDups

end RuleCall

/-- A where clause - currently just patterns, but extensible. -/
inductive Clause where
  /-- A simple pattern to match. -/
  | pattern (p : Pattern)
  /-- A predicate expression to filter bindings. -/
  | predicate (p : Query.Predicate)
  /-- A rule call. -/
  | rule (call : RuleCall)
  /-- Logical AND of multiple clauses. -/
  | and (clauses : List Clause)
  /-- Logical OR of multiple clauses. -/
  | or (clauses : List Clause)
  /-- Negation (not pattern). -/
  | not (clause : Clause)
  deriving Repr, Inhabited

namespace Clause

/-- Create a simple pattern clause. -/
def pat (e a v : Term) : Clause := .pattern ⟨e, a, v⟩

/-- Get all patterns in this clause (flattened). -/
def patterns : Clause → List Pattern
  | .pattern p => [p]
  | .predicate _ => []
  | .rule _ => []
  | .and cs => (cs.map patterns).flatten
  | .or cs => (cs.map patterns).flatten
  | .not c => c.patterns

/-- Get all variables used in this clause. -/
def vars : Clause → List Var
  | .pattern p => p.vars
  | .predicate p => Query.Predicate.vars p
  | .rule c => c.vars
  | .and cs => (cs.map vars).flatten.eraseDups
  | .or cs => (cs.map vars).flatten.eraseDups
  | .not c => c.vars

end Clause

/-- A rule definition with a name, parameters, and body clauses. -/
structure RuleDef where
  name : String
  params : List Var
  body : List Clause
  deriving Repr, Inhabited

namespace RuleDef

def arity (r : RuleDef) : Nat := r.params.length

end RuleDef

/-- A complete query with find clause and where clause. -/
structure Query where
  /-- Variables to return in results. -/
  find : List Var
  /-- Patterns to match. -/
  where_ : List Clause
  /-- Rule definitions for this query. -/
  rules : List RuleDef := []
  deriving Repr, Inhabited

namespace Query

/-- Create a simple query from find variables and pattern clauses. -/
def create (findVars : List String) (patterns : List Pattern) : Query :=
  { find := findVars.map Var.ofName
  , where_ := patterns.map .pattern }

/-- Get all variables mentioned in the where clause. -/
def whereVars (q : Query) : List Var :=
  (q.where_.map Clause.vars).flatten.eraseDups

/-- Validate that all find variables appear in where clause. -/
def isValid (q : Query) : Bool :=
  let whereVs := q.whereVars
  q.find.all (whereVs.contains ·)

end Query

end Ledger
