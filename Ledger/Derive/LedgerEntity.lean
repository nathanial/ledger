/-
  Ledger.Derive.LedgerEntity

  Automatic code generation for entity-structure mapping.
  Generates attribute constants, pull specs, transaction builders, and more
  from a Lean structure definition.

  ## Usage

  ```lean
  -- File: MyTypes.lean
  structure Person where
    name : String
    age : Nat

  -- File: MyEntities.lean (separate file!)
  import MyTypes
  import Ledger.Derive.LedgerEntity

  open Ledger.Derive in
  makeLedgerEntity Person
  ```

  This generates:
  - `Person.attr_name`, `Person.attr_age` (attribute constants)
  - `Person.attributes` (list of all attributes)
  - `Person.pullSpec` (pull specification)
  - `Person.pull` (construct entity from database)
  - `Person.createOps` (transaction builder for creation)
  - `Person.retractionOps` (transaction builder for deletion)
  - `Person.set_name`, `Person.set_age` (per-field setters with cardinality-one enforcement)
  - `Person.updateOps` (full struct update with cardinality-one enforcement)
  - `Person.TxM.create` (TxM monad entity creation)
  - `Person.TxM.setName`, `Person.TxM.setAge` (TxM monad setters)
  - `Person.TxM.delete` (TxM monad entity deletion)

  ## Important Limitation

  **The `makeLedgerEntity` command MUST be used in a different file than where
  the structure is defined.** This is due to Lean 4's elaboration ordering.
-/

import Lean
import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Tx.Types
import Ledger.Pull.Pattern
import Ledger.Pull.Result
import Ledger.Pull.Executor
import Ledger.Db.Database
import Ledger.DSL.TxM

namespace Ledger.Derive

open Lean Elab Command Meta Parser

/-! ## Syntax Definitions -/

/-- Attribute name casing options -/
inductive AttrCasing where
  | camel   -- fieldName → fieldName (default)
  | kebab   -- fieldName → field-name
  deriving Inhabited, BEq

/-- Syntax variants for makeLedgerEntity -/
syntax "makeLedgerEntity" ident : command
syntax "makeLedgerEntity" ident "(" "attrPrefix" ":=" str ")" : command
syntax "makeLedgerEntity" ident "(" "attrCasing" ":=" ident ")" : command
syntax "makeLedgerEntity" ident "(" "attrPrefix" ":=" str ")" "(" "attrCasing" ":=" ident ")" : command

/-! ## Helper Functions -/

/-- Modify the first character of a string -/
private def modifyFirstChar (f : Char → Char) (s : String) : String :=
  match s.toList with
  | [] => s
  | c :: cs => String.ofList (f c :: cs)

/-- Convert struct name to lowercase prefix (e.g., "TestPerson" -> "testperson") -/
def toLowerPrefix (s : String) : String := s.toLower

/-- Convert camelCase to kebab-case (e.g., "passwordHash" -> "password-hash") -/
def toKebabCase (s : String) : String :=
  let chars := s.toList
  let result := chars.foldl (init := []) fun acc c =>
    if c.isUpper then
      if acc.isEmpty then [c.toLower]
      else acc ++ ['-', c.toLower]
    else
      acc ++ [c]
  String.mk result

/-- Apply casing transformation to a field name -/
def applyAttrCasing (casing : AttrCasing) (fieldName : String) : String :=
  match casing with
  | .camel => fieldName
  | .kebab => toKebabCase fieldName

/-- Convert field name to camelCase setter name (e.g., "title" -> "setTitle") -/
def toCamelCaseSetter (fieldName : String) : String :=
  "set" ++ modifyFirstChar Char.toUpper fieldName

/-! ## Field Analysis -/

/-- Information about a structure field for code generation -/
structure FieldInfo where
  name : Name
  typeName : Name
  isIdField : Bool := false
  deriving Inhabited

/-- Check if a field is an id field (derived from EntityId) -/
def isIdField (fieldName : Name) (typeName : Name) : Bool :=
  fieldName == `id && (typeName == ``Nat || typeName == ``Int || typeName == ``Ledger.EntityId)

/-- Check if a field should be skipped for attribute generation -/
def shouldSkipAttr (fieldName : Name) (typeName : Name) : Bool :=
  isIdField fieldName typeName

/-- Get the core type name from an expression -/
partial def getTypeName (e : Expr) : Name :=
  match e with
  | .const n _ => n
  | .app f _ => getTypeName f
  | _ => `unknown

/-! ## Code Generation -/

/-- Elaborate a code string as a command -/
def elaborateCodeString (code : String) : CommandElabM Unit := do
  let env ← getEnv
  let stx ← match runParserCategory env `command code with
    | .ok stx => pure stx
    | .error e => throwError s!"Failed to parse generated code: {e}\n\nCode:\n{code}"
  elabCommand stx

/-- Generate Value constructor expression for a field type -/
def valueConstructor (typeName : Name) (accessor : String) : String :=
  if typeName == ``String then s!"Ledger.Value.string {accessor}"
  else if typeName == ``Int then s!"Ledger.Value.int {accessor}"
  else if typeName == ``Nat then s!"Ledger.Value.int (Int.ofNat {accessor})"
  else if typeName == ``Bool then s!"Ledger.Value.bool {accessor}"
  else if typeName == ``Float then s!"Ledger.Value.float {accessor}"
  else if typeName == ``Ledger.EntityId then s!"Ledger.Value.ref {accessor}"
  else s!"Ledger.Value.string (toString {accessor})"

/-- Get the Lean type string for a field type (for function signatures) -/
def leanTypeName (typeName : Name) : String :=
  if typeName == ``String then "String"
  else if typeName == ``Int then "Int"
  else if typeName == ``Nat then "Nat"
  else if typeName == ``Bool then "Bool"
  else if typeName == ``Float then "Float"
  else if typeName == ``Ledger.EntityId then "Ledger.EntityId"
  else "String"

/-- Get the TxM setter function name for a field type -/
def txmSetterName (typeName : Name) : String :=
  if typeName == ``String then "Ledger.TxM.setStr"
  else if typeName == ``Int then "Ledger.TxM.setInt"
  else if typeName == ``Nat then "Ledger.TxM.setNat"
  else if typeName == ``Bool then "Ledger.TxM.setBool"
  else if typeName == ``Float then "Ledger.TxM.setFloat"
  else if typeName == ``Ledger.EntityId then "Ledger.TxM.setRef"
  else "Ledger.TxM.setStr"

/-- Get the TxM add function name for a field type -/
def txmAddName (typeName : Name) : String :=
  if typeName == ``String then "Ledger.TxM.addStr"
  else if typeName == ``Int then "Ledger.TxM.addInt"
  else if typeName == ``Nat then "Ledger.TxM.addNat"
  else if typeName == ``Bool then "Ledger.TxM.addBool"
  else if typeName == ``Float then "Ledger.TxM.addFloat"
  else if typeName == ``Ledger.EntityId then "Ledger.TxM.addRef"
  else "Ledger.TxM.addStr"

/-- Generate pull extraction code for a field type.
    Handles `.many` case by taking the first (most recent) value. -/
def pullExtraction (typeName : Name) (fieldName : String) (attrName : String) : String :=
  if typeName == ``String then
    s!"  let some {fieldName} := (match result.get? {attrName} with\n" ++
    s!"    | some (.scalar (.string s)) => some s\n" ++
    s!"    | some (.many ((.scalar (.string s)) :: _)) => some s\n" ++
    s!"    | _ => none) | none"
  else if typeName == ``Int then
    s!"  let some {fieldName} := (match result.get? {attrName} with\n" ++
    s!"    | some (.scalar (.int n)) => some n\n" ++
    s!"    | some (.many ((.scalar (.int n)) :: _)) => some n\n" ++
    s!"    | _ => none) | none"
  else if typeName == ``Nat then
    s!"  let some {fieldName}Val := (match result.get? {attrName} with\n" ++
    s!"    | some (.scalar (.int n)) => some n\n" ++
    s!"    | some (.many ((.scalar (.int n)) :: _)) => some n\n" ++
    s!"    | _ => none) | none\n" ++
    s!"  let {fieldName} := {fieldName}Val.toNat"
  else if typeName == ``Bool then
    s!"  let some {fieldName} := (match result.get? {attrName} with\n" ++
    s!"    | some (.scalar (.bool b)) => some b\n" ++
    s!"    | some (.many ((.scalar (.bool b)) :: _)) => some b\n" ++
    s!"    | _ => none) | none"
  else if typeName == ``Ledger.EntityId then
    s!"  let some {fieldName} := (match result.get? {attrName} with\n" ++
    s!"    | some (.ref e) => some e\n" ++
    s!"    | some (.scalar (.ref e)) => some e\n" ++
    s!"    | some (.many ((.ref e) :: _)) => some e\n" ++
    s!"    | some (.many ((.scalar (.ref e)) :: _)) => some e\n" ++
    s!"    | _ => none) | none"
  else
    s!"  let some {fieldName} := (match result.get? {attrName} with\n" ++
    s!"    | some (.scalar (.string s)) => some s\n" ++
    s!"    | some (.many ((.scalar (.string s)) :: _)) => some s\n" ++
    s!"    | _ => none) | none"

/-! ## Main Implementation -/

/-- Core implementation for generating LedgerEntity code -/
def makeLedgerEntityCore (structName : Ident) (prefixOverride : Option String := none) (casing : AttrCasing := .camel) : CommandElabM Unit := do
  let env ← getEnv

  -- Resolve the struct with helpful error
  let declName ← try
    liftCoreM <| Lean.resolveGlobalConstNoOverload structName
  catch _ =>
    throwError m!"makeLedgerEntity: Cannot find structure '{structName}'.\n\n" ++
      m!"Hint: The structure must be defined before calling makeLedgerEntity.\n" ++
      m!"If this structure is in the same file, you must move makeLedgerEntity " ++
      m!"to a separate file that imports this one."

  -- Get structure fields
  let fields := getStructureFields env declName
  if fields.isEmpty then
    throwError m!"makeLedgerEntity: '{structName}' has no fields or is not a structure."

  -- Determine the attribute prefix (use override if provided)
  let structStr := declName.getString!
  let nsPrefix := prefixOverride.getD (toLowerPrefix structStr)

  -- Analyze fields
  let mut fieldInfos : Array FieldInfo := #[]
  for fieldName in fields do
    -- Get field type
    let projName := declName ++ fieldName
    let some projInfo := env.find? projName
      | throwError s!"Cannot find projection {projName}"

    let fieldType ← liftTermElabM <| Meta.forallTelescopeReducing projInfo.type fun _ body =>
      pure body

    let typeName := getTypeName fieldType
    let isId := isIdField fieldName typeName

    fieldInfos := fieldInfos.push { name := fieldName, typeName := typeName, isIdField := isId }

  -- Separate regular fields from id field
  let regularFields := fieldInfos.filter (!·.isIdField)
  let hasIdField := fieldInfos.any (·.isIdField)

  -- Open namespace using the fully qualified name
  -- This ensures definitions are accessible as StructName.attr_field
  let namespaceCmd := s!"namespace {declName}"
  elaborateCodeString namespaceCmd

  -- ========================================
  -- 1. Generate attribute constants (only for regular fields, not id)
  -- ========================================
  for field in regularFields do
    let attrName := applyAttrCasing casing (toString field.name)
    let code := s!"/-- Attribute for {field.name} field -/\n" ++
      s!"def attr_{field.name} : Ledger.Attribute := ⟨\":{nsPrefix}/{attrName}\"⟩"
    elaborateCodeString code

  -- ========================================
  -- 2. Generate attributes list
  -- ========================================
  let attrList := String.intercalate ", " (regularFields.toList.map fun f => s!"attr_{f.name}")
  elaborateCodeString s!"/-- All attributes for this entity type -/\ndef attributes : List Ledger.Attribute := [{attrList}]"

  -- ========================================
  -- 3. Generate pullSpec
  -- ========================================
  let pullPatterns := String.intercalate ", " (regularFields.toList.map fun f => s!"Ledger.PullPattern.attr attr_{f.name}")
  elaborateCodeString s!"/-- Pull specification for retrieving this entity from the database -/\ndef pullSpec : Ledger.PullSpec := [{pullPatterns}]"

  -- ========================================
  -- 4. Generate pull function
  -- ========================================
  let extractionLines := String.intercalate "\n" (regularFields.toList.map fun f =>
    pullExtraction f.typeName (toString f.name) s!"attr_{f.name}")
  -- Include id field in struct construction if present (derived from EntityId)
  let idBinding := if hasIdField then "id := eid.id.toNat, " else ""
  let regularBindings := String.intercalate ", " (regularFields.toList.map fun f =>
    s!"{f.name} := {f.name}")
  let fieldBindings := idBinding ++ regularBindings

  let lbrace := "{"
  let rbrace := "}"

  let pullCode := s!"/-- Pull an entity from the database and construct the structure -/
def pull (db : Ledger.Db) (eid : Ledger.EntityId) : Option {declName} := do
  let result := Ledger.Pull.pull db eid pullSpec
{extractionLines}
  return {lbrace} {fieldBindings} {rbrace}"

  elaborateCodeString pullCode

  -- ========================================
  -- 5. Generate createOps (only for regular fields, not id)
  -- ========================================
  let txOpsList := regularFields.toList.map fun f =>
    let valExpr := valueConstructor f.typeName s!"entity.{f.name}"
    s!"Ledger.TxOp.add eid attr_{f.name} ({valExpr})"
  let txOpsStr := String.intercalate ",\n    " txOpsList

  let createOpsCode := s!"/-- Generate transaction operations to create an entity -/
def createOps (eid : Ledger.EntityId) (entity : {declName}) : Ledger.Transaction :=
  [ {txOpsStr} ]"

  elaborateCodeString createOpsCode

  -- ========================================
  -- 6. Generate retractionOps
  -- ========================================
  let retractionCode := "/-- Generate retraction operations for all attributes of an entity -/
def retractionOps (db : Ledger.Db) (eid : Ledger.EntityId) : List Ledger.TxOp :=
  attributes.filterMap fun attr =>
    db.getOne eid attr |>.map fun v => Ledger.TxOp.retract eid attr v"
  elaborateCodeString retractionCode

  -- ========================================
  -- 7. Generate per-field set_<field> functions
  -- ========================================
  for field in regularFields do
    let fieldStr := toString field.name
    let typeStr := leanTypeName field.typeName
    let valExpr := valueConstructor field.typeName "value"
    let setCode := s!"/-- Set the {fieldStr} field, retracting old value if present (cardinality-one) -/
def set_{fieldStr} (db : Ledger.Db) (eid : Ledger.EntityId) (value : {typeStr}) : List Ledger.TxOp :=
  let newVal := {valExpr}
  match db.getOne eid attr_{fieldStr} with
  | some oldVal => if oldVal == newVal then [] else [Ledger.TxOp.retract eid attr_{fieldStr} oldVal, Ledger.TxOp.add eid attr_{fieldStr} newVal]
  | none => [Ledger.TxOp.add eid attr_{fieldStr} newVal]"
    elaborateCodeString setCode

  -- ========================================
  -- 8. Generate updateOps (full struct update)
  -- ========================================
  let setterCalls := regularFields.toList.map fun f =>
    s!"set_{f.name} db eid entity.{f.name}"
  let settersStr := String.intercalate " ++\n    " setterCalls

  let updateOpsCode := s!"/-- Update all fields of an entity, retracting old values if present (cardinality-one) -/
def updateOps (db : Ledger.Db) (eid : Ledger.EntityId) (entity : {declName}) : List Ledger.TxOp :=
  {settersStr}"

  elaborateCodeString updateOpsCode

  -- ========================================
  -- 9. Generate TxM namespace with monadic helpers
  -- ========================================
  elaborateCodeString "namespace TxM"

  -- 9a. Generate TxM.create
  let txmCreateLines := regularFields.toList.map fun f =>
    let addFn := txmAddName f.typeName
    s!"  {addFn} eid attr_{f.name} entity.{f.name}"
  let txmCreateBody := String.intercalate "\n" txmCreateLines

  let txmCreateCode := s!"/-- Create an entity using TxM monad -/
def create (eid : Ledger.EntityId) (entity : {declName}) : Ledger.TxM Unit := do
{txmCreateBody}"
  elaborateCodeString txmCreateCode

  -- 9b. Generate TxM.set<Field> for each field
  for field in regularFields do
    let fieldStr := toString field.name
    let camelName := toCamelCaseSetter fieldStr
    let typeStr := leanTypeName field.typeName
    let setterFn := txmSetterName field.typeName
    let txmSetCode := s!"/-- Set {fieldStr} using TxM monad (cardinality-one) -/
def {camelName} (eid : Ledger.EntityId) (value : {typeStr}) : Ledger.TxM Unit :=
  {setterFn} eid attr_{fieldStr} value"
    elaborateCodeString txmSetCode

  -- 9c. Generate TxM.delete
  let txmDeleteCode := s!"/-- Delete an entity by retracting all its attributes -/
def delete (eid : Ledger.EntityId) : Ledger.TxM Unit :=
  Ledger.TxM.retractAttrs eid attributes"
  elaborateCodeString txmDeleteCode

  elaborateCodeString "end TxM"

  -- Close namespace
  let endNs := s!"end {declName}"
  elaborateCodeString endNs

/-! ## Command Elaborator -/

/-- Parse casing identifier to AttrCasing -/
def parseCasing (casingId : Ident) : CommandElabM AttrCasing := do
  match casingId.getId with
  | `camel => pure .camel
  | `kebab => pure .kebab
  | other => throwError s!"Unknown casing '{other}'. Expected 'camel' or 'kebab'."

elab_rules : command
  | `(command| makeLedgerEntity $structName:ident) =>
    makeLedgerEntityCore structName
  | `(command| makeLedgerEntity $structName:ident ( attrPrefix := $prefixLit:str )) => do
    let pfx := prefixLit.getString
    makeLedgerEntityCore structName (some pfx)
  | `(command| makeLedgerEntity $structName:ident ( attrCasing := $casingId:ident )) => do
    let casing ← parseCasing casingId
    makeLedgerEntityCore structName none casing
  | `(command| makeLedgerEntity $structName:ident ( attrPrefix := $prefixLit:str ) ( attrCasing := $casingId:ident )) => do
    let pfx := prefixLit.getString
    let casing ← parseCasing casingId
    makeLedgerEntityCore structName (some pfx) casing

end Ledger.Derive
