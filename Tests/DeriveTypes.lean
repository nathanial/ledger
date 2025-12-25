/-
  Tests.DeriveTypes - Test structure definitions for derive tests

  These structures are defined in a separate file so that makeLedgerEntity
  can be called in Tests/Derive.lean after they are fully elaborated.
-/

import Ledger.Core.EntityId

namespace Ledger.Tests.DeriveTypes

open Ledger

/-- Simple test structure with basic types -/
structure TestPerson where
  name : String
  age : Nat
  active : Bool
  deriving Inhabited

/-- Test structure with entity reference -/
structure TestTask where
  title : String
  description : String
  priority : Nat
  assignee : EntityId
  deriving Inhabited

/-- Test structure with an id field (should be skipped) -/
structure TestItem where
  id : Nat
  name : String
  value : Int
  deriving Inhabited

end Ledger.Tests.DeriveTypes
