/-
  Ledger.Query.Predicate

  Predicate expressions for query filtering.
-/

import Ledger.Core.Value
import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Query.Var
import Staple.String

namespace Ledger

namespace Query

inductive CmpOp where
  | eq
  | ne
  | lt
  | lte
  | gt
  | gte
  deriving Repr, BEq, Inhabited

inductive StrOp where
  | contains
  | startsWith
  | endsWith
  deriving Repr, BEq, Inhabited

inductive ArithOp where
  | add
  | sub
  | mul
  | div
  deriving Repr, BEq, Inhabited

inductive PredTerm where
  | value (v : Value)
  | entity (e : EntityId)
  | var (v : Var)
  deriving Repr, Inhabited

namespace PredTerm

def vars : PredTerm → List Var
  | .var v => [v]
  | _ => []

end PredTerm

inductive PredExpr where
  | term (t : PredTerm)
  | arith (op : ArithOp) (lhs rhs : PredExpr)
  deriving Repr, Inhabited

namespace PredExpr

def var (name : String) : PredExpr := .term (.var ⟨name⟩)
def int (n : Int) : PredExpr := .term (.value (.int n))
def float (f : Float) : PredExpr := .term (.value (.float f))
def str (s : String) : PredExpr := .term (.value (.string s))
def bool (b : Bool) : PredExpr := .term (.value (.bool b))
def instant (n : Nat) : PredExpr := .term (.value (.instant n))
def entity (e : EntityId) : PredExpr := .term (.entity e)

def add (a b : PredExpr) : PredExpr := .arith .add a b
def sub (a b : PredExpr) : PredExpr := .arith .sub a b
def mul (a b : PredExpr) : PredExpr := .arith .mul a b
def div (a b : PredExpr) : PredExpr := .arith .div a b

def vars : PredExpr → List Var
  | .term t => t.vars
  | .arith _ a b => a.vars ++ b.vars

end PredExpr

inductive Predicate where
  | compare (op : CmpOp) (lhs rhs : PredExpr)
  | string (op : StrOp) (lhs rhs : PredExpr)
  | and (preds : List Predicate)
  | or (preds : List Predicate)
  | not (pred : Predicate)
  deriving Repr, Inhabited

namespace Predicate

def eq (a b : PredExpr) : Predicate := .compare .eq a b
def ne (a b : PredExpr) : Predicate := .compare .ne a b
def lt (a b : PredExpr) : Predicate := .compare .lt a b
def lte (a b : PredExpr) : Predicate := .compare .lte a b
def gt (a b : PredExpr) : Predicate := .compare .gt a b
def gte (a b : PredExpr) : Predicate := .compare .gte a b

def contains (a b : PredExpr) : Predicate := .string .contains a b
def startsWith (a b : PredExpr) : Predicate := .string .startsWith a b
def endsWith (a b : PredExpr) : Predicate := .string .endsWith a b

def vars : Predicate → List Var
  | .compare _ a b => (a.vars ++ b.vars).eraseDups
  | .string _ a b => (a.vars ++ b.vars).eraseDups
  | .and ps => (ps.map vars).flatten.eraseDups
  | .or ps => (ps.map vars).flatten.eraseDups
  | .not p => p.vars

end Predicate

end Query

end Ledger
