/-
  Ledger.Core.Value

  Polymorphic value type supporting common data types used in the database.
-/

import Ledger.Core.EntityId

namespace Ledger

-- ByteArray doesn't have a Repr instance by default
instance : Repr ByteArray where
  reprPrec ba _ := s!"#bytes[{ba.size}]"

/-- Polymorphic value type for datom values.
    Supports common primitive types plus entity references. -/
inductive Value where
  /-- 64-bit signed integer -/
  | int (v : Int)
  /-- 64-bit floating point -/
  | float (v : Float)
  /-- UTF-8 string -/
  | string (v : String)
  /-- Boolean -/
  | bool (v : Bool)
  /-- Instant (Unix timestamp in milliseconds) -/
  | instant (v : Nat)
  /-- Reference to another entity -/
  | ref (v : EntityId)
  /-- Keyword/symbol (like Clojure keywords) -/
  | keyword (v : String)
  /-- Raw bytes -/
  | bytes (v : ByteArray)
  deriving Repr, Inhabited

namespace Value

/-- Type tag for ordering values of different types. -/
private def typeTag : Value → Nat
  | .int _ => 0
  | .float _ => 1
  | .string _ => 2
  | .bool _ => 3
  | .instant _ => 4
  | .ref _ => 5
  | .keyword _ => 6
  | .bytes _ => 7

/-- Compare two ByteArrays lexicographically. -/
private def compareByteArray (a b : ByteArray) : Ordering :=
  let rec go (i : Nat) : Ordering :=
    if i >= a.size && i >= b.size then .eq
    else if i >= a.size then .lt
    else if i >= b.size then .gt
    else
      let c := compare a.data[i]! b.data[i]!
      if c != .eq then c else go (i + 1)
  go 0

/-- Compare two floats, treating NaN specially. -/
private def compareFloat (a b : Float) : Ordering :=
  if a.isNaN && b.isNaN then .eq
  else if a.isNaN then .gt  -- NaN sorts last
  else if b.isNaN then .lt
  else if a < b then .lt
  else if a > b then .gt
  else .eq

instance : BEq Value where
  beq a b := match a, b with
    | .int x, .int y => x == y
    | .float x, .float y => x == y || (x.isNaN && y.isNaN)
    | .string x, .string y => x == y
    | .bool x, .bool y => x == y
    | .instant x, .instant y => x == y
    | .ref x, .ref y => x == y
    | .keyword x, .keyword y => x == y
    | .bytes x, .bytes y => x == y
    | _, _ => false

instance : Ord Value where
  compare a b :=
    let ta := typeTag a
    let tb := typeTag b
    if ta != tb then compare ta tb
    else match a, b with
      | .int x, .int y => compare x y
      | .float x, .float y => compareFloat x y
      | .string x, .string y => compare x y
      | .bool x, .bool y => compare x y
      | .instant x, .instant y => compare x y
      | .ref x, .ref y => compare x y
      | .keyword x, .keyword y => compare x y
      | .bytes x, .bytes y => compareByteArray x y
      | _, _ => .eq  -- unreachable

instance : LT Value where
  lt a b := compare a b == .lt

instance : LE Value where
  le a b := compare a b != .gt

instance : ToString Value where
  toString v := match v with
    | .int n => toString n
    | .float f => toString f
    | .string s => s!"\"{s}\""
    | .bool b => toString b
    | .instant t => s!"#inst {t}"
    | .ref e => s!"#ref {e}"
    | .keyword k => k
    | .bytes b => s!"#bytes[{b.size}]"

instance : Hashable Value where
  hash v := match v with
    | .int n => hash n
    | .float f => hash f.toUInt64
    | .string s => hash s
    | .bool b => hash b
    | .instant t => hash t
    | .ref e => hash e
    | .keyword k => hash k
    | .bytes b => hash b.data.toList

-- Convenience constructors
def ofInt (n : Int) : Value := .int n
def ofNat (n : Nat) : Value := .int n
def ofFloat (f : Float) : Value := .float f
def ofString (s : String) : Value := .string s
def ofBool (b : Bool) : Value := .bool b
def ofInstant (t : Nat) : Value := .instant t
def ofRef (e : EntityId) : Value := .ref e
def ofKeyword (k : String) : Value := .keyword k
def ofBytes (b : ByteArray) : Value := .bytes b

/-- Check if this value is a reference to another entity. -/
def isRef : Value → Bool
  | .ref _ => true
  | _ => false

/-- Extract entity reference if this is a ref value. -/
def asRef : Value → Option EntityId
  | .ref e => some e
  | _ => none

/-- Extract integer if this is an int value. -/
def asInt : Value → Option Int
  | .int n => some n
  | _ => none

/-- Extract string if this is a string value. -/
def asString : Value → Option String
  | .string s => some s
  | _ => none

end Value

end Ledger
