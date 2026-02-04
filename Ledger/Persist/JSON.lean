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

/-! ## JSON Parsing (via Staple.Json) -/

open Staple.Json.Value

private abbrev JValue := Staple.Json.Value

private def parseJsonValue (s : String) : Option JValue :=
  Staple.Json.parse? s

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

private def valueFromJsonValue (v : JValue) : Option Value := do
  let typeStr ← getStrField? "t" v
  let raw ← getField? "v" v
  match typeStr with
  | "int" =>
    let n ← getInt? raw
    return .int n
  | "float" =>
    let f ← getFloat? raw
    return .float f
  | "string" =>
    let s ← getStr? raw
    return .string s
  | "bool" =>
    let b ← getBool? raw
    return .bool b
  | "instant" =>
    let n ← getNat? raw
    return .instant n
  | "ref" =>
    let n ← getInt? raw
    return .ref ⟨n⟩
  | "keyword" =>
    let k ← getStr? raw
    return .keyword k
  | "bytes" =>
    let b64 ← getStr? raw
    let data ← base64Decode b64
    return .bytes data
  | _ => none

/-- Deserialize a Value from JSON string -/
def valueFromJson (s : String) : Option Value := do
  let v ← parseJsonValue s
  valueFromJsonValue v

/-! ## Datom Serialization -/

/-- Serialize a Datom to JSON array: [entity, attr, value, tx, added] -/
def datomToJson (d : Datom) : String :=
  let e := d.entity.id
  let a := escapeString d.attr.name
  let v := valueToJson d.value
  let t := d.tx.id
  let added := d.added
  s!"[{e},\"{a}\",{v},{t},{added}]"

def datomFromJsonValue (v : JValue) : Option Datom := do
  let arr ← getArr? v
  if arr.size < 5 then
    none
  else
    let entityVal ← arr[0]?
    let attrVal ← arr[1]?
    let valueVal ← arr[2]?
    let txVal ← arr[3]?
    let addedVal ← arr[4]?

    let entityId ← getInt? entityVal
    let attrName ← getStr? attrVal
    let value ← valueFromJsonValue valueVal
    let txId ← getNat? txVal
    let added ← getBool? addedVal

    return {
      entity := ⟨entityId⟩
      attr := ⟨attrName⟩
      value := value
      tx := ⟨txId⟩
      added := added
    }

/-- Deserialize a Datom from JSON array string -/
def datomFromJson (s : String) : Option Datom := do
  let v ← parseJsonValue s
  datomFromJsonValue v

/-! ## TxLogEntry Serialization -/

/-- Serialize a TxLogEntry to JSON -/
def txLogEntryToJson (entry : TxLogEntry) : String := Id.run do
  let mut datomsJson := ""
  for i in [:entry.datoms.size] do
    if i > 0 then datomsJson := datomsJson ++ ","
    datomsJson := datomsJson ++ datomToJson entry.datoms[i]!
  s!"\{\"txId\":{entry.txId.id},\"instant\":{entry.txInstant},\"datoms\":[{datomsJson}]}"

def txLogEntryFromJsonValue (v : JValue) : Option TxLogEntry := do
  let txId ← getNatField? "txId" v
  let instant ← getNatField? "instant" v
  let datomVals ← getArrField? "datoms" v

  let mut datoms : Array Datom := #[]
  for datomVal in datomVals do
    let datom ← datomFromJsonValue datomVal
    datoms := datoms.push datom

  return {
    txId := ⟨txId⟩
    txInstant := instant
    datoms := datoms
  }

/-- Deserialize a TxLogEntry from JSON string -/
def txLogEntryFromJson (s : String) : Option TxLogEntry := do
  let v ← parseJsonValue s
  txLogEntryFromJsonValue v

end Ledger.Persist.JSON
