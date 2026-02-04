/-
  Ledger.Persist.Compaction

  Snapshot + journal compaction utilities.
-/

import Ledger.Persist.Connection
import Ledger.Persist.Snapshot
import Ledger.Persist.JSONL
import Ledger.Persist.JSON

namespace Ledger.Persist.Compaction

open Ledger.Persist.JSON
open Ledger.Persist.JSONL
open Ledger.Persist.Snapshot

structure CompactionResult where
  snapshotPath : System.FilePath
  journalPath : System.FilePath
  keptEntries : Nat
  deriving Repr, Inhabited

/-- Compact a persistent connection by snapshotting and trimming journal. -/
def compact (pc : PersistentConnection) : IO (PersistentConnection × CompactionResult) := do
  pc.handle.flush

  let snap := Snapshot.fromConnection pc.conn
  let snapshotPath := Snapshot.defaultPath pc.journalPath
  let tmpSnapshotPath := System.FilePath.mk (snapshotPath.toString ++ ".tmp")
  Snapshot.write tmpSnapshotPath snap
  IO.FS.rename tmpSnapshotPath snapshotPath

  let kept ← readJournalSince pc.journalPath snap.basisT
  let keptLines := kept.toList.map txLogEntryToJson
  let content := String.intercalate "\n" keptLines
  IO.FS.writeFile pc.journalPath content

  let handle ← IO.FS.Handle.mk pc.journalPath .append
  let pc' := { pc with handle := handle }
  let result : CompactionResult := {
    snapshotPath := snapshotPath
    journalPath := pc.journalPath
    keptEntries := kept.size
  }
  return (pc', result)

end Ledger.Persist.Compaction
