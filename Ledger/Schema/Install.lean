/-
  Ledger.Schema.Install

  Functions for installing schema attributes into the database.
  Schema is stored as entities with :db/* attributes.
-/

import Ledger.Core.EntityId
import Ledger.Core.Attribute
import Ledger.Core.Value
import Ledger.Tx.Types
import Ledger.Schema.Types
import Ledger.Index.Manager

namespace Ledger

namespace Schema

/-- Generate transaction operations to install an attribute schema.
    The schema is stored as an entity with :db/* attributes. -/
def installOps (eid : EntityId) (attrSchema : AttributeSchema) : Transaction :=
  let base : List TxOp := [
    .add eid SchemaAttr.ident (.keyword attrSchema.ident.name),
    .add eid SchemaAttr.valueType_ (.keyword attrSchema.valueType.toKeyword),
    .add eid SchemaAttr.cardinality (.keyword attrSchema.cardinality.toKeyword)
  ]
  let withUnique := match attrSchema.unique with
    | some u => base ++ [.add eid SchemaAttr.unique (.keyword u.toKeyword)]
    | none => base
  let withIndexed := if attrSchema.indexed
    then withUnique ++ [.add eid SchemaAttr.indexed (.bool true)]
    else withUnique
  let withComponent := if attrSchema.component
    then withIndexed ++ [.add eid SchemaAttr.isComponent (.bool true)]
    else withIndexed
  let withDoc := match attrSchema.doc with
    | some doc => withComponent ++ [.add eid SchemaAttr.doc (.string doc)]
    | none => withComponent
  withDoc

/-- Generate transaction operations to install multiple attribute schemas. -/
def installAllOps (schemas : List AttributeSchema) (startEid : EntityId) : Transaction :=
  let rec go (remaining : List AttributeSchema) (idx : Nat) (acc : Transaction) : Transaction :=
    match remaining with
    | [] => acc
    | attrSchema :: rest =>
      let eid := ⟨startEid.id + idx⟩
      go rest (idx + 1) (acc ++ installOps eid attrSchema)
  go schemas 0 []

/-- Load schema from database indexes.
    Scans for entities with :db/ident and reconstructs AttributeSchema. -/
def loadFromIndexes (indexes : Indexes) : Schema :=
  -- Find all entities with :db/ident
  let schemaEntities := indexes.entitiesWithAttr SchemaAttr.ident

  schemaEntities.foldl (init := Schema.empty) fun schema eid =>
    -- Extract schema components from indexes
    let identDatoms := indexes.datomsForEntityAttr eid SchemaAttr.ident
    let valueTypeDatoms := indexes.datomsForEntityAttr eid SchemaAttr.valueType_
    let cardinalityDatoms := indexes.datomsForEntityAttr eid SchemaAttr.cardinality
    let uniqueDatoms := indexes.datomsForEntityAttr eid SchemaAttr.unique
    let indexedDatoms := indexes.datomsForEntityAttr eid SchemaAttr.indexed
    let componentDatoms := indexes.datomsForEntityAttr eid SchemaAttr.isComponent
    let docDatoms := indexes.datomsForEntityAttr eid SchemaAttr.doc

    -- Helper to get most recent value
    let getLatestValue (datoms : List Datom) : Option Value :=
      let visible := datoms.filter (·.added)
      match visible.foldl (fun acc d => match acc with
        | none => some d
        | some prev => if d.tx.id > prev.tx.id then some d else acc) none with
      | some d => some d.value
      | none => none

    let ident := getLatestValue identDatoms
    let valueType := getLatestValue valueTypeDatoms
    let cardinality := getLatestValue cardinalityDatoms
    let unique := getLatestValue uniqueDatoms
    let indexed := getLatestValue indexedDatoms
    let component := getLatestValue componentDatoms
    let doc := getLatestValue docDatoms

    -- Parse and construct AttributeSchema
    match ident, valueType with
    | some (.keyword identStr), some (.keyword vtStr) =>
      match ValueType.fromKeyword vtStr with
      | some vt =>
        let card := match cardinality with
          | some (.keyword cardStr) => Cardinality.fromKeyword cardStr |>.getD .one
          | _ => .one
        let uniq := match unique with
          | some (.keyword uStr) => Unique.fromKeyword uStr
          | _ => none
        let idx := match indexed with
          | some (.bool b) => b
          | _ => false
        let comp := match component with
          | some (.bool b) => b
          | _ => false
        let docStr := match doc with
          | some (.string s) => some s
          | _ => none

        let attrSchema : AttributeSchema := {
          ident := ⟨identStr⟩
          valueType := vt
          cardinality := card
          unique := uniq
          indexed := idx
          component := comp
          doc := docStr
        }
        schema.insert attrSchema
      | none => schema  -- Invalid valueType, skip
    | _, _ => schema  -- Missing required fields, skip

end Schema

end Ledger
