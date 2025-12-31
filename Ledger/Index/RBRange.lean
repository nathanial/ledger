/-
  Ledger.Index.RBRange

  Range query utilities for RBMap-based indexes.
  Provides efficient range iteration with early termination.
-/

import Batteries.Data.RBMap

namespace Ledger.RBRange

/-- Collect values from an RBMap while a predicate holds on the key.
    Uses ForIn with early termination for efficiency.

    The predicate should define a contiguous range in the sorted key order.
    Once the predicate returns false after returning true, iteration stops.

    Complexity: O(s + k) where s = elements before range, k = elements in range.
    Early termination when exiting range avoids full O(n) scan. -/
def collectWhile {K V : Type} [Ord K] (map : Batteries.RBMap K V compare)
    (inRange : K → Bool) : List V := Id.run do
  let mut result : Array V := #[]
  let mut started := false
  for (k, v) in map do
    if inRange k then
      started := true
      result := result.push v
    else if started then
      -- We've exited the range after being in it, stop iterating
      break
  return result.toList

/-- Collect key-value pairs from an RBMap while a predicate holds.
    Similar to collectWhile but returns pairs instead of just values. -/
def collectPairsWhile {K V : Type} [Ord K] (map : Batteries.RBMap K V compare)
    (inRange : K → Bool) : List (K × V) := Id.run do
  let mut result : Array (K × V) := #[]
  let mut started := false
  for (k, v) in map do
    if inRange k then
      started := true
      result := result.push (k, v)
    else if started then
      break
  return result.toList

/-- Collect values from an RBMap starting from a lower bound while predicate holds.
    The lower bound key is used to skip initial elements more efficiently.

    Note: This still iterates from the beginning due to RBMap API limitations,
    but terminates early once the range is exited.

    For true O(log n + k), would need direct tree navigation which requires
    RBNode access not exposed in the public RBMap API. -/
def collectFromWhile {K V : Type} [Ord K] (map : Batteries.RBMap K V compare)
    (lower : K) (inRange : K → Bool) : List V := Id.run do
  let mut result : Array V := #[]
  let mut started := false
  for (k, v) in map do
    -- Skip elements before the lower bound
    if !started then
      if compare k lower != .lt then
        started := true
    -- Once started, collect while in range
    if started then
      if inRange k then
        result := result.push v
      else
        break
  return result.toList

end Ledger.RBRange
