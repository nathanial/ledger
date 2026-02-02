/-
  Ledger.Persist.JSON

  JSON serialization for Ledger types (Value, Datom, TxLogEntry).
  Uses typed object format for unambiguous Value serialization.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Core.Datom
import Ledger.Db.TimeTravel
import Staple.Json

namespace Ledger.Persist.JSON

/-! ## Base64 Encoding -/

private def base64Chars : String :=
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

private def charAtIndex (s : String) (i : Nat) : Char :=
  s.data[i]!

/-- Encode a ByteArray to Base64 string -/
def base64Encode (data : ByteArray) : String := Id.run do
  if data.size == 0 then return ""

  let mut result : String := ""
  let mut i := 0

  while i + 2 < data.size do
    let b0 := data.data[i]!.toNat
    let b1 := data.data[i + 1]!.toNat
    let b2 := data.data[i + 2]!.toNat

    let c0 := b0 >>> 2
    let c1 := ((b0 &&& 0x03) <<< 4) ||| (b1 >>> 4)
    let c2 := ((b1 &&& 0x0F) <<< 2) ||| (b2 >>> 6)
    let c3 := b2 &&& 0x3F

    result := result.push (charAtIndex base64Chars c0)
    result := result.push (charAtIndex base64Chars c1)
    result := result.push (charAtIndex base64Chars c2)
    result := result.push (charAtIndex base64Chars c3)
    i := i + 3

  let remaining := data.size - i
  if remaining == 1 then
    let b0 := data.data[i]!.toNat
    let c0 := b0 >>> 2
    let c1 := (b0 &&& 0x03) <<< 4
    result := result.push (charAtIndex base64Chars c0)
    result := result.push (charAtIndex base64Chars c1)
    result := result ++ "=="
  else if remaining == 2 then
    let b0 := data.data[i]!.toNat
    let b1 := data.data[i + 1]!.toNat
    let c0 := b0 >>> 2
    let c1 := ((b0 &&& 0x03) <<< 4) ||| (b1 >>> 4)
    let c2 := (b1 &&& 0x0F) <<< 2
    result := result.push (charAtIndex base64Chars c0)
    result := result.push (charAtIndex base64Chars c1)
    result := result.push (charAtIndex base64Chars c2)
    result := result ++ "="

  return result

/-- Decode a single Base64 character to its value (0-63) -/
private def decodeBase64Char (c : Char) : Option UInt8 :=
  if c >= 'A' && c <= 'Z' then some (c.toNat - 'A'.toNat).toUInt8
  else if c >= 'a' && c <= 'z' then some (c.toNat - 'a'.toNat + 26).toUInt8
  else if c >= '0' && c <= '9' then some (c.toNat - '0'.toNat + 52).toUInt8
  else if c == '+' then some 62
  else if c == '/' then some 63
  else none

/-- Decode a Base64 string to ByteArray -/
def base64Decode (s : String) : Option ByteArray := do
  if s.isEmpty then return ByteArray.empty

  -- Remove padding
  let stripped := s.dropRightWhile (· == '=')

  let chars := stripped.toList
  let mut result := ByteArray.empty
  let mut i := 0

  while i + 3 < chars.length do
    let c0 ← decodeBase64Char chars[i]!
    let c1 ← decodeBase64Char chars[i + 1]!
    let c2 ← decodeBase64Char chars[i + 2]!
    let c3 ← decodeBase64Char chars[i + 3]!

    let b0 := (c0 <<< 2) ||| (c1 >>> 4)
    let b1 := (c1 <<< 4) ||| (c2 >>> 2)
    let b2 := (c2 <<< 6) ||| c3

    result := result.push b0
    result := result.push b1
    result := result.push b2
    i := i + 4

  -- Handle remaining characters
  let remaining := chars.length - i
  if remaining >= 2 then
    let c0 ← decodeBase64Char chars[i]!
    let c1 ← decodeBase64Char chars[i + 1]!
    let b0 := (c0 <<< 2) ||| (c1 >>> 4)
    result := result.push b0

    if remaining >= 3 then
      let c2 ← decodeBase64Char chars[i + 2]!
      let b1 := (c1 <<< 4) ||| (c2 >>> 2)
      result := result.push b1

  return result

/-! ## JSON String Escaping -/

/-- Escape a string for JSON output -/
def escapeString (s : String) : String := Id.run do
  let mut result := ""
  for c in s.toList do
    if c == '"' then result := result ++ "\\\""
    else if c == '\\' then result := result ++ "\\\\"
    else if c == '\n' then result := result ++ "\\n"
    else if c == '\r' then result := result ++ "\\r"
    else if c == '\t' then result := result ++ "\\t"
    else if c.toNat < 32 then
      -- Control characters as \uXXXX
      let hex := Nat.toDigits 16 c.toNat
      let padded := String.mk (List.replicate (4 - hex.length) '0' ++ hex)
      result := result ++ "\\u" ++ padded
    else
      result := result.push c
  return result

/-! ## Simple JSON Parser Helpers -/

/-- Parser state -/
structure ParseState where
  input : String
  pos : Nat
  deriving Repr

/-- Peek at the current character without advancing. -/
def ParseState.peek (s : ParseState) : Option Char :=
  if s.pos < s.input.length then some s.input.data[s.pos]! else none

/-- Advance the position by one character. -/
def ParseState.advance (s : ParseState) : ParseState :=
  { s with pos := s.pos + 1 }

/-- Skip whitespace characters (space, newline, carriage return, tab). -/
def ParseState.skipWhitespace (s : ParseState) : ParseState := Id.run do
  let mut s := s
  while s.peek.map (fun c => c == ' ' || c == '\n' || c == '\r' || c == '\t') |>.getD false do
    s := s.advance
  return s

/-- Expect a specific character and advance past it. Returns none if mismatch. -/
def ParseState.expect (s : ParseState) (c : Char) : Option ParseState :=
  if s.peek == some c then some s.advance else none

/-- Parse a JSON integer, returns value and new state -/
def parseJsonInt (s : ParseState) : Option (Int × ParseState) := do
  let s := s.skipWhitespace
  let mut neg := false
  let mut s := s
  if s.peek == some '-' then
    neg := true
    s := s.advance

  let mut n : Nat := 0
  let mut foundDigit := false
  while s.peek.map Char.isDigit |>.getD false do
    foundDigit := true
    n := n * 10 + (s.peek.get!.toNat - '0'.toNat)
    s := s.advance

  if !foundDigit then none
  let result : Int := if neg then -(n : Int) else n
  return (result, s)

/-- Parse JSON string content after the opening quote.
    Handles escape sequences (\\, \", \n, \r, \t, \uXXXX).
    Returns the decoded string and state positioned after the closing quote. -/
partial def parseJsonStringContent (s : ParseState) : Option (String × ParseState) :=
  go s ""
where
  go (s : ParseState) (result : String) : Option (String × ParseState) :=
    match s.peek with
    | none => none
    | some '"' => some (result, s.advance)
    | some '\\' =>
      let s := s.advance
      match s.peek with
      | some '"' => go s.advance (result.push '"')
      | some '\\' => go s.advance (result.push '\\')
      | some 'n' => go s.advance (result.push '\n')
      | some 'r' => go s.advance (result.push '\r')
      | some 't' => go s.advance (result.push '\t')
      | some 'u' =>
        let s := s.advance
        -- Parse 4 hex digits
        if s.pos + 4 <= s.input.length then
          let hex := (s.input.drop s.pos).take 4
          let s := { s with pos := s.pos + 4 }
          match hex.toNat? with
          | some n => go s (result.push (Char.ofNat n))
          | none => none
        else none
      | _ => none
    | some c => go s.advance (result.push c)

/-- Parse a complete JSON string (including quotes) -/
def parseJsonString (s : ParseState) : Option (String × ParseState) := do
  let s := s.skipWhitespace
  let s ← s.expect '"'
  parseJsonStringContent s

/-! ## Value Serialization -/

/-- Serialize a Value to JSON string -/
def valueToJson (v : Value) : String :=
  match v with
  | .int n => jsonStr! { "t" : "int", "v" : n }
  | .float f => jsonStr! { "t" : "float", "v" : f }
  | .string s => jsonStr! { "t" : "string", "v" : s }
  | .bool b => jsonStr! { "t" : "bool", "v" : b }
  | .instant n => jsonStr! { "t" : "instant", "v" : n }
  | .ref e => jsonStr! { "t" : "ref", "v" : e.id }
  | .keyword k => jsonStr! { "t" : "keyword", "v" : k }
  | .bytes data => jsonStr! { "t" : "bytes", "v" : base64Encode data }

/-- Skip over a JSON value (for parsing) -/
def skipJsonValue (s : ParseState) : ParseState := Id.run do
  let s := s.skipWhitespace
  match s.peek with
  | some '{' =>
    let mut s := s.advance
    let mut depth := 1
    while depth > 0 do
      match s.peek with
      | some '{' => depth := depth + 1; s := s.advance
      | some '}' => depth := depth - 1; s := s.advance
      | some '"' =>
        s := s.advance
        while s.peek != some '"' do
          if s.peek == some '\\' then s := s.advance.advance
          else s := s.advance
        s := s.advance
      | some _ => s := s.advance
      | none => break
    return s
  | some '"' =>
    let mut s := s.advance
    while s.peek != some '"' do
      if s.peek == some '\\' then s := s.advance.advance
      else s := s.advance
    return s.advance
  | some '[' =>
    let mut s := s.advance
    let mut depth := 1
    while depth > 0 do
      match s.peek with
      | some '[' => depth := depth + 1; s := s.advance
      | some ']' => depth := depth - 1; s := s.advance
      | some '"' =>
        s := s.advance
        while s.peek != some '"' do
          if s.peek == some '\\' then s := s.advance.advance
          else s := s.advance
        s := s.advance
      | some _ => s := s.advance
      | none => break
    return s
  | _ =>
    -- Number, bool, null
    let mut s := s
    while s.peek.map (fun c => c != ',' && c != '}' && c != ']' && c != ' ' && c != '\n') |>.getD false do
      s := s.advance
    return s

/-- Extract substring from input between two positions -/
def extractSubstring (input : String) (start finish : Nat) : String :=
  (input.drop start).take (finish - start)

/-- Parse a float from string -/
def parseFloat (str : String) : Option Float :=
  -- Simple float parsing: try to parse as int first, handle decimal
  let str := str.trim
  if str.isEmpty then none
  else
    -- Check for decimal point
    if str.any (· == '.') || str.any (· == 'e') || str.any (· == 'E') then
      -- Use a simple approach: split on '.' and handle
      let parts := str.splitOn "."
      match parts with
      | [intPart] =>
        -- Scientific notation without decimal
        intPart.toInt?.map (Float.ofInt ·)
      | [intPart, fracPart] =>
        let neg := intPart.startsWith "-"
        let intPartClean := if neg then intPart.drop 1 else intPart
        match intPartClean.toNat?, fracPart.takeWhile Char.isDigit |>.toNat? with
        | some i, some f =>
          let fracLen := (fracPart.takeWhile Char.isDigit).length
          let divisor := (10 : Float) ^ fracLen.toFloat
          let result := i.toFloat + f.toFloat / divisor
          some (if neg then -result else result)
        | _, _ => none
      | _ => none
    else
      str.toInt?.map (Float.ofInt ·)

/-- Deserialize a Value from JSON string -/
def valueFromJson (s : String) : Option Value := do
  let state : ParseState := { input := s, pos := 0 }
  let state := state.skipWhitespace
  let state ← state.expect '{'
  let state := state.skipWhitespace

  -- Parse "t":"<type>"
  let state ← state.expect '"'
  if state.peek != some 't' then none
  let state := state.advance
  let state ← state.expect '"'
  let state := state.skipWhitespace
  let state ← state.expect ':'
  let (typeStr, state) ← parseJsonString state
  let state := state.skipWhitespace
  let state ← state.expect ','
  let state := state.skipWhitespace

  -- Parse "v":<value>
  let state ← state.expect '"'
  if state.peek != some 'v' then none
  let state := state.advance
  let state ← state.expect '"'
  let state := state.skipWhitespace
  let state ← state.expect ':'
  let state := state.skipWhitespace

  match typeStr with
  | "int" =>
    let (n, _) ← parseJsonInt state
    return .int n
  | "float" =>
    let valueStart := state.pos
    let state := skipJsonValue state
    let valueStr := extractSubstring s valueStart state.pos
    let f ← parseFloat valueStr
    return .float f
  | "string" =>
    let (str, _) ← parseJsonString state
    return .string str
  | "bool" =>
    if state.peek == some 't' then return .bool true
    if state.peek == some 'f' then return .bool false
    none
  | "instant" =>
    let (n, _) ← parseJsonInt state
    if n < 0 then none else return .instant n.toNat
  | "ref" =>
    let (n, _) ← parseJsonInt state
    return .ref ⟨n⟩
  | "keyword" =>
    let (k, _) ← parseJsonString state
    return .keyword k
  | "bytes" =>
    let (b64, _) ← parseJsonString state
    let data ← base64Decode b64
    return .bytes data
  | _ => none

/-! ## Datom Serialization -/

/-- Serialize a Datom to JSON array: [entity, attr, value, tx, added] -/
def datomToJson (d : Datom) : String :=
  let e := d.entity.id
  let a := escapeString d.attr.name
  let v := valueToJson d.value
  let t := d.tx.id
  let added := d.added
  s!"[{e},\"{a}\",{v},{t},{added}]"

/-- Deserialize a Datom from JSON array string -/
def datomFromJson (s : String) : Option Datom := do
  let state : ParseState := { input := s, pos := 0 }
  let state := state.skipWhitespace
  let state ← state.expect '['
  let state := state.skipWhitespace

  -- Parse entity ID
  let (entityId, state) ← parseJsonInt state
  let state := state.skipWhitespace
  let state ← state.expect ','

  -- Parse attribute
  let (attrName, state) ← parseJsonString state
  let state := state.skipWhitespace
  let state ← state.expect ','

  -- Parse value (find the object boundaries)
  let state := state.skipWhitespace
  let valueStart := state.pos
  let state := skipJsonValue state
  let valueStr := extractSubstring s valueStart state.pos
  let value ← valueFromJson valueStr
  let state := state.skipWhitespace
  let state ← state.expect ','

  -- Parse tx ID
  let (txId, state) ← parseJsonInt state
  if txId < 0 then none
  let state := state.skipWhitespace
  let state ← state.expect ','

  -- Parse added
  let state := state.skipWhitespace
  let added ← if state.peek == some 't' then some true
              else if state.peek == some 'f' then some false
              else none

  return {
    entity := ⟨entityId⟩
    attr := ⟨attrName⟩
    value := value
    tx := ⟨txId.toNat⟩
    added := added
  }

/-! ## TxLogEntry Serialization -/

/-- Serialize a TxLogEntry to JSON -/
def txLogEntryToJson (entry : TxLogEntry) : String := Id.run do
  let mut datomsJson := ""
  for i in [:entry.datoms.size] do
    if i > 0 then datomsJson := datomsJson ++ ","
    datomsJson := datomsJson ++ datomToJson entry.datoms[i]!
  s!"\{\"txId\":{entry.txId.id},\"instant\":{entry.txInstant},\"datoms\":[{datomsJson}]}"

/-- Deserialize a TxLogEntry from JSON string -/
def txLogEntryFromJson (s : String) : Option TxLogEntry := do
  let state : ParseState := { input := s, pos := 0 }
  let state := state.skipWhitespace
  let state ← state.expect '{'
  let state := state.skipWhitespace

  let mut parsedTxId : Option Nat := none
  let mut parsedInstant : Option Nat := none
  let mut parsedDatoms : Option (Array Datom) := none
  let mut state := state

  while state.peek != some '}' && state.peek.isSome do
    -- Parse key
    let (key, state') ← parseJsonString state
    state := state'.skipWhitespace
    state ← state.expect ':'
    state := state.skipWhitespace

    match key with
    | "txId" =>
      let (n, state') ← parseJsonInt state
      if n < 0 then none
      parsedTxId := some n.toNat
      state := state'
    | "instant" =>
      let (n, state') ← parseJsonInt state
      if n < 0 then none
      parsedInstant := some n.toNat
      state := state'
    | "datoms" =>
      -- Parse array of datoms
      state ← state.expect '['
      state := state.skipWhitespace
      let mut arr : Array Datom := #[]
      while state.peek != some ']' do
        let datomStart := state.pos
        state := skipJsonValue state
        let datomStr := extractSubstring s datomStart state.pos
        let datom ← datomFromJson datomStr
        arr := arr.push datom
        state := state.skipWhitespace
        if state.peek == some ',' then
          state := state.advance.skipWhitespace
      state := state.advance  -- skip ']'
      parsedDatoms := some arr
    | _ =>
      -- Skip unknown field
      state := skipJsonValue state

    state := state.skipWhitespace
    if state.peek == some ',' then
      state := state.advance.skipWhitespace

  let txId ← parsedTxId
  let instant ← parsedInstant
  let datoms ← parsedDatoms

  return {
    txId := ⟨txId⟩
    txInstant := instant
    datoms := datoms
  }

end Ledger.Persist.JSON
