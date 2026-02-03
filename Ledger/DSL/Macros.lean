/-
  Ledger.DSL.Macros

  Macro-based query DSL with Datomic-like syntax.
-/

import Lean
import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Query.AST
import Ledger.Query.Predicate

open Lean

namespace Ledger.DSL

declare_syntax_cat qvar
declare_syntax_cat qattr
declare_syntax_cat qterm
declare_syntax_cat qattrterm
declare_syntax_cat qpexpr
declare_syntax_cat qpred
declare_syntax_cat qclause
declare_syntax_cat qvarlist
declare_syntax_cat qrule

syntax "?" ident : qvar
syntax ":" ident "/" ident : qattr
syntax ":" ident : qattr

syntax "?" ident : qterm
syntax "_" : qterm
syntax "#e" num : qterm
syntax num : qterm
syntax str : qterm
syntax "#t" : qterm
syntax "#f" : qterm
syntax qattr : qterm

syntax "?" ident : qattrterm
syntax "_" : qattrterm
syntax qattr : qattrterm

syntax "?" ident : qpexpr
syntax "#e" num : qpexpr
syntax num : qpexpr
syntax str : qpexpr
syntax "#t" : qpexpr
syntax "#f" : qpexpr
syntax qattr : qpexpr
syntax "(" "+" qpexpr qpexpr ")" : qpexpr
syntax "(" "-" qpexpr qpexpr ")" : qpexpr
syntax "(" "*" qpexpr qpexpr ")" : qpexpr
syntax "(" "/" qpexpr qpexpr ")" : qpexpr

syntax "(" ">" qpexpr qpexpr ")" : qpred
syntax "(" ">=" qpexpr qpexpr ")" : qpred
syntax "(" "<" qpexpr qpexpr ")" : qpred
syntax "(" "<=" qpexpr qpexpr ")" : qpred
syntax "(" "=" qpexpr qpexpr ")" : qpred
syntax "(" "!=" qpexpr qpexpr ")" : qpred
syntax "(" "contains" qpexpr qpexpr ")" : qpred
syntax "(" "startsWith" qpexpr qpexpr ")" : qpred
syntax "(" "endsWith" qpexpr qpexpr ")" : qpred
syntax "(" "and" qpred+ ")" : qpred
syntax "(" "or" qpred+ ")" : qpred
syntax "(" "not" qpred ")" : qpred

syntax "[" qterm qattrterm qterm "]" : qclause
syntax "[" qpred "]" : qclause
syntax "(" "or" qclause+ ")" : qclause
syntax "(" "and" qclause+ ")" : qclause
syntax "(" "not" qclause ")" : qclause
syntax "(" ident qterm+ ")" : qclause

syntax "[" qvar* "]" : qvarlist
syntax "(" "rule" ident qvarlist qclause+ ")" : qrule

syntax "query!" "{" ":find" qvar+ ":where" qclause+ "}" : term
syntax "query!" "{" ":find" qvar+ ":where" qclause+ ":rules" qrule+ "}" : term

private def mkListTerm (elems : Array Syntax) : MacroM Syntax := do
  let mut acc : TSyntax `term ← `(List.nil)
  for e in elems.reverse do
    let e' : TSyntax `term := ⟨e⟩
    acc ← `(List.cons $e' $acc)
  return acc

private def attrToString (stx : Syntax) : MacroM String := do
  match stx with
  | `(qattr| : $ns:ident / $name:ident) =>
    pure s!":{ns.getId.toString}/{name.getId.toString}"
  | `(qattr| : $name:ident) =>
    pure s!":{name.getId.toString}"
  | _ =>
    Macro.throwError "invalid attribute syntax"

private def qvarToVar (stx : Syntax) : MacroM Syntax := do
  match stx with
  | `(qvar| ? $name:ident) =>
    let lit := Syntax.mkStrLit name.getId.toString
    `(Ledger.Var.ofName $lit)
  | _ =>
    Macro.throwError "invalid variable syntax"

macro_rules
  | `(qterm| ? $name:ident) => do
      let lit := Syntax.mkStrLit name.getId.toString
      `(Ledger.Term.var (Ledger.Var.ofName $lit))
  | `(qterm| _) => `(Ledger.Term.blank)
  | `(qterm| #e $n:num) => `(Ledger.Term.entity (Ledger.EntityId.mk (Int.ofNat $n)))
  | `(qterm| $n:num) => `(Ledger.Term.value (Ledger.Value.int (Int.ofNat $n)))
  | `(qterm| $s:str) => `(Ledger.Term.value (Ledger.Value.string $s))
  | `(qterm| #t) => `(Ledger.Term.value (Ledger.Value.bool Bool.true))
  | `(qterm| #f) => `(Ledger.Term.value (Ledger.Value.bool Bool.false))
  | `(qterm| $a:qattr) => do
      let s ← attrToString a
      let lit := Syntax.mkStrLit s
      `(Ledger.Term.value (Ledger.Value.keyword $lit))

macro_rules
  | `(qattrterm| ? $name:ident) => do
      let lit := Syntax.mkStrLit name.getId.toString
      `(Ledger.Term.var (Ledger.Var.ofName $lit))
  | `(qattrterm| _) => `(Ledger.Term.blank)
  | `(qattrterm| $a:qattr) => do
      let s ← attrToString a
      let lit := Syntax.mkStrLit s
      `(Ledger.Term.attr (Ledger.Attribute.mk $lit))

macro_rules
  | `(qpexpr| ? $name:ident) => do
      let lit := Syntax.mkStrLit name.getId.toString
      `(Ledger.Query.PredExpr.var $lit)
  | `(qpexpr| #e $n:num) => `(Ledger.Query.PredExpr.entity (Ledger.EntityId.mk (Int.ofNat $n)))
  | `(qpexpr| $n:num) => `(Ledger.Query.PredExpr.int (Int.ofNat $n))
  | `(qpexpr| $s:str) => `(Ledger.Query.PredExpr.str $s)
  | `(qpexpr| #t) => `(Ledger.Query.PredExpr.bool Bool.true)
  | `(qpexpr| #f) => `(Ledger.Query.PredExpr.bool Bool.false)
  | `(qpexpr| $a:qattr) => do
      let s ← attrToString a
      let lit := Syntax.mkStrLit s
      `(Ledger.Query.PredExpr.term (Ledger.Query.PredTerm.value (Ledger.Value.keyword $lit)))
  | `(qpexpr| ( + $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.PredExpr.add $a' $b')
  | `(qpexpr| ( - $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.PredExpr.sub $a' $b')
  | `(qpexpr| ( * $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.PredExpr.mul $a' $b')
  | `(qpexpr| ( / $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.PredExpr.div $a' $b')

macro_rules
  | `(qpred| ( > $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.Predicate.gt $a' $b')
  | `(qpred| ( >= $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.Predicate.gte $a' $b')
  | `(qpred| ( < $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.Predicate.lt $a' $b')
  | `(qpred| ( <= $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.Predicate.lte $a' $b')
  | `(qpred| ( = $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.Predicate.eq $a' $b')
  | `(qpred| ( != $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.Predicate.ne $a' $b')
  | `(qpred| ( contains $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.Predicate.contains $a' $b')
  | `(qpred| ( startsWith $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.Predicate.startsWith $a' $b')
  | `(qpred| ( endsWith $a:qpexpr $b:qpexpr )) => do
      let a' : TSyntax `term := ⟨a.raw⟩
      let b' : TSyntax `term := ⟨b.raw⟩
      `(Ledger.Query.Predicate.endsWith $a' $b')
  | `(qpred| ( and $ps:qpred* )) => do
      let list ← mkListTerm (ps.map (·.raw))
      let list' : TSyntax `term := ⟨list⟩
      `(Ledger.Query.Predicate.and $list')
  | `(qpred| ( or $ps:qpred* )) => do
      let list ← mkListTerm (ps.map (·.raw))
      let list' : TSyntax `term := ⟨list⟩
      `(Ledger.Query.Predicate.or $list')
  | `(qpred| ( not $p:qpred )) => do
      let p' : TSyntax `term := ⟨p.raw⟩
      `(Ledger.Query.Predicate.not $p')

macro_rules
  | `(qclause| [ $e:qterm $a:qattrterm $v:qterm ]) => do
      let e' : TSyntax `term := ⟨e.raw⟩
      let a' : TSyntax `term := ⟨a.raw⟩
      let v' : TSyntax `term := ⟨v.raw⟩
      `(Ledger.Clause.pattern { entity := $e', attr := $a', value := $v' })
  | `(qclause| [ $p:qpred ]) => do
      let p' : TSyntax `term := ⟨p.raw⟩
      `(Ledger.Clause.predicate $p')
  | `(qclause| (or $cs:qclause*)) => do
      let list ← mkListTerm (cs.map (·.raw))
      let list' : TSyntax `term := ⟨list⟩
      `(Ledger.Clause.or $list')
  | `(qclause| (and $cs:qclause*)) => do
      let list ← mkListTerm (cs.map (·.raw))
      let list' : TSyntax `term := ⟨list⟩
      `(Ledger.Clause.and $list')
  | `(qclause| (not $c:qclause)) => do
      let c' : TSyntax `term := ⟨c.raw⟩
      `(Ledger.Clause.not $c')
  | `(qclause| ($name:ident $args:qterm*)) => do
      let argsList ← mkListTerm (args.map (·.raw))
      let argsListTerm : TSyntax `term := ⟨argsList⟩
      let nameLit := Syntax.mkStrLit name.getId.toString
      `(Ledger.Clause.rule { name := $nameLit, args := $argsListTerm })

macro_rules
  | `(qrule| (rule $name:ident [$params:qvar*] $clauses:qclause*)) => do
      let paramTerms ← (params.map (·.raw)).mapM qvarToVar
      let paramsList ← mkListTerm paramTerms
      let clauseList ← mkListTerm (clauses.map (·.raw))
      let paramsListTerm : TSyntax `term := ⟨paramsList⟩
      let clauseListTerm : TSyntax `term := ⟨clauseList⟩
      let nameLit := Syntax.mkStrLit name.getId.toString
      `({ name := $nameLit, params := $paramsListTerm, body := $clauseListTerm })

macro_rules
  | `(term| query! { :find $vars:qvar* :where $clauses:qclause* }) => do
      let varTerms ← (vars.map (·.raw)).mapM qvarToVar
      let varsList ← mkListTerm varTerms
      let clauseList ← mkListTerm (clauses.map (·.raw))
      let varsListTerm : TSyntax `term := ⟨varsList⟩
      let clauseListTerm : TSyntax `term := ⟨clauseList⟩
      `( { find := $varsListTerm, where_ := $clauseListTerm } )
  | `(term| query! { :find $vars:qvar* :where $clauses:qclause* :rules $rules:qrule* }) => do
      let varTerms ← (vars.map (·.raw)).mapM qvarToVar
      let varsList ← mkListTerm varTerms
      let clauseList ← mkListTerm (clauses.map (·.raw))
      let rulesList ← mkListTerm (rules.map (·.raw))
      let varsListTerm : TSyntax `term := ⟨varsList⟩
      let clauseListTerm : TSyntax `term := ⟨clauseList⟩
      let rulesListTerm : TSyntax `term := ⟨rulesList⟩
      `( { find := $varsListTerm, where_ := $clauseListTerm, rules := $rulesListTerm } )

end Ledger.DSL
