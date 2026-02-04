/-
  Ledger Tests

  Main test runner that imports all test modules.
-/

import Crucible
import Tests.Core
import Tests.Database
import Tests.Retraction
import Tests.Query
import Tests.Binding
import Tests.Predicate
import Tests.Pull
import Tests.TimeTravel
import Tests.DSL
import Tests.Persistence
import Tests.Derive
import Tests.Performance
import Tests.RangeQuery
import Tests.Schema
import Tests.Aggregates
import Tests.Rules
import Tests.Macros
import Tests.TxFunctions

open Crucible

def main : IO Unit := do
  IO.println "╔══════════════════════════════════════╗"
  IO.println "║     Ledger Database Tests            ║"
  IO.println "╚══════════════════════════════════════╝"
  IO.println ""

  let exitCode ← runAllSuites

  IO.println ""
  if exitCode == 0 then
    IO.println "All tests passed!"
  else
    IO.println "Some tests failed"
    IO.Process.exit 1
