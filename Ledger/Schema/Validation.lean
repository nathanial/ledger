/-
  Ledger.Schema.Validation

  Transaction validation against schema constraints.
-/

import Ledger.Core.Datom
import Ledger.Tx.Types
import Ledger.Schema.Types
import Ledger.Index.Manager

namespace Ledger

namespace SchemaValidation

/-- Check if a value matches the expected type for an attribute. -/
def validateType (attrSchema : AttributeSchema) (value : Value) : Except SchemaError Unit :=
  if attrSchema.valueType.matchesValue value then
    .ok ()
  else
    .error (.typeMismatch attrSchema.ident attrSchema.valueType value)

/-- Check cardinality-one constraint within a single transaction.
    For cardinality-one, adding multiple values for the same entity+attr in one tx is a violation. -/
def validateCardinalityInTx (entity : EntityId) (attrSchema : AttributeSchema)
    (pendingAdds : List TxOp) : Except SchemaError Unit :=
  match attrSchema.cardinality with
  | .many => .ok ()  -- No constraint for cardinality-many
  | .one =>
    -- Count how many adds for this entity+attr in the pending transaction
    let addCount := pendingAdds.filter (fun op =>
      match op with
      | .add e a _ => e == entity && a == attrSchema.ident
      | _ => false
    ) |>.length
    -- If we're adding multiple values in the same transaction, that's a violation
    if addCount > 1 then
      .error (.cardinalityViolation entity attrSchema.ident)
    else
      .ok ()

/-- Check uniqueness constraint.
    - identity: used for lookup (AVET indexed), duplicate values rejected
    - value: no two entities can have the same value -/
def validateUniqueness (indexes : Indexes) (entity : EntityId) (attrSchema : AttributeSchema)
    (value : Value) : Except SchemaError Unit :=
  match attrSchema.unique with
  | none => .ok ()
  | some .identity | some .value =>
    -- Check if any other entity has this value using AVET index
    let existing := indexes.entitiesWithAttrValue attrSchema.ident value
    -- Filter out the current entity (re-asserting same value is ok)
    let others := existing.filter (· != entity)
    match others.head? with
    | none => .ok ()
    | some other => .error (.uniquenessViolation attrSchema.ident value other entity)

/-- Validate a single TxOp against the schema. -/
def validateOp (config : SchemaConfig) (indexes : Indexes) (pendingAdds : List TxOp)
    (op : TxOp) : Except SchemaError Unit := do
  match op with
  | .retract _ _ _ =>
    -- Retractions don't need schema validation
    .ok ()
  | .retractEntity _ =>
    -- Entity retractions don't need schema validation
    .ok ()
  | .call _ _ =>
    -- Calls should be expanded before validation
    .ok ()
  | .add entity attr value =>
    match config.schema.get? attr with
    | none =>
      -- Attribute not in schema
      if config.strictMode then
        .error (.undefinedAttribute attr)
      else
        .ok ()  -- Permissive mode: allow undefined attributes
    | some attrSchema =>
      -- Validate type
      validateType attrSchema value
      -- Validate cardinality within transaction
      validateCardinalityInTx entity attrSchema pendingAdds
      -- Validate uniqueness against existing data
      validateUniqueness indexes entity attrSchema value

/-- Validate an entire transaction against the schema.
    Returns the first error encountered, if any. -/
def validateTransaction (config : SchemaConfig) (indexes : Indexes)
    (tx : Transaction) : Except SchemaError Unit := do
  -- Get all add operations for cardinality checking
  let adds := tx.filter (fun op => match op with | .add _ _ _ => true | _ => false)
  -- Validate each operation
  for op in tx do
    validateOp config indexes adds op

end SchemaValidation

end Ledger
