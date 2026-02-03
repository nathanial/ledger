/-
  Ledger.Tests.Binding - Binding and relation semantics
-/

import Crucible
import Ledger

namespace Ledger.Tests.Binding

open Crucible
open Ledger

testSuite "Binding"

private def v (name : String) : Var := Var.ofName name

test "merge adds new vars" := do
  let b1 := Binding.ofList [(v "a", .value (.int 1))]
  let b2 := Binding.ofList [(v "b", .value (.int 2))]
  match b1.merge b2 with
  | none => throw <| IO.userError "Expected merge to succeed"
  | some b =>
    b.lookup (v "a") ≡ some (.value (.int 1))
    b.lookup (v "b") ≡ some (.value (.int 2))

test "merge keeps same value" := do
  let b1 := Binding.ofList [(v "a", .value (.int 1))]
  let b2 := Binding.ofList [(v "a", .value (.int 1))]
  match b1.merge b2 with
  | none => throw <| IO.userError "Expected merge to succeed"
  | some b =>
    b.lookup (v "a") ≡ some (.value (.int 1))

test "merge rejects conflicting value" := do
  let b1 := Binding.ofList [(v "a", .value (.int 1))]
  let b2 := Binding.ofList [(v "a", .value (.int 2))]
  match b1.merge b2 with
  | none => pure ()
  | some _ => throw <| IO.userError "Expected merge to fail"

test "project keeps only requested vars" := do
  let b := Binding.ofList [
    (v "a", .value (.int 1)),
    (v "b", .value (.int 2))
  ]
  let proj := b.project [v "b"]
  proj.lookup (v "a") ≡ none
  proj.lookup (v "b") ≡ some (.value (.int 2))

test "binding equality ignores insertion order" := do
  let b1 := Binding.ofList [
    (v "a", .value (.int 1)),
    (v "b", .value (.int 2))
  ]
  let b2 := Binding.ofList [
    (v "b", .value (.int 2)),
    (v "a", .value (.int 1))
  ]
  ensure (b1 == b2) "Expected bindings to be equal"

test "relation distinct removes duplicate bindings" := do
  let b1 := Binding.ofList [
    (v "a", .value (.int 1)),
    (v "b", .value (.int 2))
  ]
  let b2 := Binding.ofList [
    (v "b", .value (.int 2)),
    (v "a", .value (.int 1))
  ]
  let rel : Relation := ⟨[b1, b2]⟩
  rel.distinct.size ≡ 1

test "toValues follows requested order" := do
  let b := Binding.ofList [
    (v "a", .value (.int 1)),
    (v "b", .value (.int 2))
  ]
  let vals := b.toValues [v "b", v "a", v "c"]
  vals ≡ [some (.value (.int 2)), some (.value (.int 1)), none]

end Ledger.Tests.Binding
