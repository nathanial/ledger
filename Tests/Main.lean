/-
  Ledger Tests

  Main test runner that imports all test modules.
-/

import Crucible
import Tests.Core
import Tests.Database
import Tests.Retraction
import Tests.Query
import Tests.Pull
import Tests.DSL
import Tests.Persistence

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
