/-
  Simple test to debug makeLedgerEntity
-/

import Ledger
import Tests.DeriveTypes

open Ledger.Derive
open Ledger.Tests.DeriveTypes

-- Generate entity code - let's see if this works
makeLedgerEntity TestPerson

-- Let's try to use what was generated
#check TestPerson.attr_name
#check TestPerson.attributes
#check TestPerson.pullSpec
#check TestPerson.pull
#check TestPerson.createOps
#check TestPerson.retractionOps
