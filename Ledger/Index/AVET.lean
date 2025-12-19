/-
  Ledger.Index.AVET

  Attribute-Value-Entity-Transaction index.
  Primary index for value lookups (e.g., "find entity where :person/email = 'foo@bar.com'").
  Especially useful for unique attributes.
-/

import Batteries.Data.RBMap
import Ledger.Core.Datom
import Ledger.Index.Types

namespace Ledger

/-- AVET index using RBMap for ordered access. -/
abbrev AVETIndex := Batteries.RBMap AVETKey Datom compare

namespace AVETIndex

/-- Create an empty AVET index. -/
def empty : AVETIndex := Batteries.RBMap.empty

/-- Create key from a datom. -/
def keyOf (d : Datom) : AVETKey :=
  { attr := d.attr
  , value := d.value
  , entity := d.entity
  , tx := d.tx }

/-- Insert a datom into the index. -/
def insertDatom (idx : AVETIndex) (d : Datom) : AVETIndex :=
  Batteries.RBMap.insert idx (keyOf d) d

/-- Get all datoms for an attribute and value (range scan).
    Useful for finding entities with a specific attribute value. -/
def datomsForAttrValue (a : Attribute) (v : Value) (idx : AVETIndex) : List Datom :=
  (Batteries.RBMap.toList idx).filterMap fun (k, d) =>
    if k.attr == a && k.value == v then some d else none

/-- Get entities with a specific attribute value.
    Primary use case: lookup by unique attribute. -/
def entitiesWithAttrValue (a : Attribute) (v : Value) (idx : AVETIndex) : List EntityId :=
  (datomsForAttrValue a v idx).map (·.entity)

/-- Get the first entity with a specific attribute value.
    Useful for unique attributes where only one entity should match. -/
def entityWithAttrValue (a : Attribute) (v : Value) (idx : AVETIndex) : Option EntityId :=
  (entitiesWithAttrValue a v idx).head?

/-- Get all datoms for an attribute (less efficient than AEVT for this). -/
def datomsForAttr (a : Attribute) (idx : AVETIndex) : List Datom :=
  (Batteries.RBMap.toList idx).filterMap fun (k, d) =>
    if k.attr == a then some d else none

/-- Get all datoms in the index. -/
def allDatoms (idx : AVETIndex) : List Datom :=
  (Batteries.RBMap.toList idx).map Prod.snd

/-- Count of datoms in the index. -/
def count (idx : AVETIndex) : Nat :=
  Batteries.RBMap.size idx

end AVETIndex

end Ledger
