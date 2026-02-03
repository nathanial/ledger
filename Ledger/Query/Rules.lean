/-
  Ledger.Query.Rules

  Support types for rule evaluation.
-/

import Std.Data.HashMap
import Ledger.Query.Var
import Ledger.Query.Binding

namespace Ledger

namespace Query

/-- Rule key (name + arity). -/
structure RuleKey where
  name : String
  arity : Nat
  deriving Repr, BEq, Hashable, Inhabited

namespace RuleKey

def ofName (name : String) (arity : Nat) : RuleKey := ⟨name, arity⟩

/-- Canonical parameter variables for rule tables. -/
def canonicalParams (key : RuleKey) : List Var :=
  (List.range key.arity).map fun i =>
    Var.ofName s!"__rule_{key.name}_{i}"

end RuleKey

/-- A rule table: canonical params + derived relation. -/
structure RuleTable where
  params : List Var
  relation : Relation
  deriving Repr, Inhabited

abbrev RuleEnv := Std.HashMap RuleKey RuleTable

namespace RuleEnv

def empty : RuleEnv := {}

def get? (env : RuleEnv) (key : RuleKey) : Option RuleTable :=
  env[key]?

def insert (env : RuleEnv) (key : RuleKey) (table : RuleTable) : RuleEnv :=
  Std.HashMap.insert env key table

end RuleEnv

end Query

end Ledger
