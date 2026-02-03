import Lake
open Lake DSL

package ledger where
  version := v!"0.1.0"

require batteries from git "https://github.com/leanprover-community/batteries" @ "v4.26.0"
require crucible from git "https://github.com/nathanial/crucible" @ "v0.0.9"
require staple from "../../util/staple"

@[default_target]
lean_lib Ledger where
  roots := #[`Ledger]

lean_lib Tests where
  globs := #[.submodules `Tests]

@[test_driver]
lean_exe tests where
  root := `Tests.Main
