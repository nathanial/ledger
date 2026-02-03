/-
  Ledger.Query.PredicateEval

  Evaluation for predicate expressions.
-/

import Ledger.Query.Predicate
import Ledger.Query.Binding
import Staple.String

namespace Ledger

namespace Query

private inductive NumVal where
  | int (v : Int)
  | float (v : Float)

private def numFromValue? : Value → Option NumVal
  | .int n => some (.int n)
  | .float f => some (.float f)
  | .instant n => some (.int n)
  | _ => none

private def numToFloat : NumVal → Float
  | .int n => Float.ofInt n
  | .float f => f

private def numToInt? : NumVal → Option Int
  | .int n => some n
  | .float _ => none

private def evalTerm (t : PredTerm) (b : Binding) : Option Value :=
  match t with
  | .value v => some v
  | .entity e => some (.ref e)
  | .var v =>
    match b.lookup v with
    | some (.value v') => some v'
    | some (.entity e) => some (.ref e)
    | _ => none

private def evalArith (op : ArithOp) (a b : NumVal) : Option Value :=
  let aF := numToFloat a
  let bF := numToFloat b
  if op == .div && bF == 0.0 then
    none
  else
    match op, numToInt? a, numToInt? b with
    | .add, some ai, some bi => some (.int (ai + bi))
    | .sub, some ai, some bi => some (.int (ai - bi))
    | .mul, some ai, some bi => some (.int (ai * bi))
    | .div, _, _ => some (.float (aF / bF))
    | .add, _, _ => some (.float (aF + bF))
    | .sub, _, _ => some (.float (aF - bF))
    | .mul, _, _ => some (.float (aF * bF))

private def evalExpr (e : PredExpr) (b : Binding) : Option Value :=
  match e with
  | .term t => evalTerm t b
  | .arith op lhs rhs =>
    match evalExpr lhs b, evalExpr rhs b with
    | some lv, some rv =>
      match numFromValue? lv, numFromValue? rv with
      | some ln, some rn => evalArith op ln rn
      | _, _ => none
    | _, _ => none

private def cmpNumbers (op : CmpOp) (a b : NumVal) : Bool :=
  match op, numToInt? a, numToInt? b with
  | .eq, some ai, some bi => ai == bi
  | .ne, some ai, some bi => ai != bi
  | .lt, some ai, some bi => ai < bi
  | .lte, some ai, some bi => ai <= bi
  | .gt, some ai, some bi => ai > bi
  | .gte, some ai, some bi => ai >= bi
  | _, _, _ =>
    let af := numToFloat a
    let bf := numToFloat b
    match op with
    | .eq => af == bf
    | .ne => af != bf
    | .lt => af < bf
    | .lte => af <= bf
    | .gt => af > bf
    | .gte => af >= bf

private def cmpStrings (op : CmpOp) (a b : String) : Bool :=
  match op with
  | .eq => a == b
  | .ne => a != b
  | .lt => a < b
  | .lte => a <= b
  | .gt => a > b
  | .gte => a >= b

private def evalCompare (op : CmpOp) (lv rv : Value) : Bool :=
  match numFromValue? lv, numFromValue? rv with
  | some ln, some rn => cmpNumbers op ln rn
  | _, _ =>
    match lv, rv with
    | .string ls, .string rs => cmpStrings op ls rs
    | _, _ =>
      match op with
      | .eq => lv == rv
      | .ne => lv != rv
      | _ => false

private def evalString (op : StrOp) (lv rv : Value) : Bool :=
  match lv, rv with
  | .string ls, .string rs =>
    match op with
    | .contains => Staple.String.containsSubstr ls rs
    | .startsWith => ls.startsWith rs
    | .endsWith => ls.endsWith rs
  | _, _ => false

partial def Predicate.eval (pred : Predicate) (b : Binding) : Bool :=
  match pred with
  | .compare op lhs rhs =>
    match evalExpr lhs b, evalExpr rhs b with
    | some lv, some rv => evalCompare op lv rv
    | _, _ => false
  | .string op lhs rhs =>
    match evalExpr lhs b, evalExpr rhs b with
    | some lv, some rv => evalString op lv rv
    | _, _ => false
  | .and ps => ps.all (Predicate.eval · b)
  | .or ps => ps.any (Predicate.eval · b)
  | .not p => !(Predicate.eval p b)

end Query

end Ledger
