# API Reference

Complete API documentation for Ledger.

## Core Types

### EntityId

Unique identifier for entities.

```lean
structure EntityId where
  id : Int
```

| Function | Type | Description |
|----------|------|-------------|
| `EntityId.mk` | `Int -> EntityId` | Create from integer |
| `EntityId.null` | `EntityId` | Null/invalid entity |
| `EntityId.isTemp` | `EntityId -> Bool` | True if negative (temporary) |

**Source**: `Ledger/Core/EntityId.lean`

### TxId

Transaction identifier.

```lean
structure TxId where
  id : Nat
```

| Function | Type | Description |
|----------|------|-------------|
| `TxId.genesis` | `TxId` | Initial transaction (id = 0) |
| `TxId.next` | `TxId -> TxId` | Next transaction ID |

**Source**: `Ledger/Core/EntityId.lean`

### Attribute

Property name for datoms.

```lean
structure Attribute where
  name : String
```

| Function | Type | Description |
|----------|------|-------------|
| `Attribute.mk` | `String -> Attribute` | Create attribute |
| `Attribute.keyword` | `String -> String -> Attribute` | Create as `:ns/name` |
| `Attribute.dbIdent` | `Attribute` | Built-in `:db/ident` |
| `Attribute.dbDoc` | `Attribute` | Built-in `:db/doc` |
| `Attribute.dbTxInstant` | `Attribute` | Built-in `:db/txInstant` |

**Source**: `Ledger/Core/Attribute.lean`

### Value

Data values in datoms.

```lean
inductive Value where
  | int : Int -> Value
  | float : Float -> Value
  | string : String -> Value
  | bool : Bool -> Value
  | instant : Nat -> Value
  | ref : EntityId -> Value
  | keyword : String -> Value
  | bytes : ByteArray -> Value
```

**Constructors**:

| Function | Type | Description |
|----------|------|-------------|
| `Value.ofInt` | `Int -> Value` | Integer value |
| `Value.ofFloat` | `Float -> Value` | Float value |
| `Value.ofString` | `String -> Value` | String value |
| `Value.ofBool` | `Bool -> Value` | Boolean value |
| `Value.ofInstant` | `Nat -> Value` | Timestamp (ms) |
| `Value.ofRef` | `EntityId -> Value` | Entity reference |
| `Value.ofKeyword` | `String -> Value` | Keyword value |
| `Value.ofBytes` | `ByteArray -> Value` | Binary data |

**Extractors**:

| Function | Type | Description |
|----------|------|-------------|
| `Value.asInt` | `Value -> Option Int` | Extract integer |
| `Value.asFloat` | `Value -> Option Float` | Extract float |
| `Value.asString` | `Value -> Option String` | Extract string |
| `Value.asBool` | `Value -> Option Bool` | Extract boolean |
| `Value.asInstant` | `Value -> Option Nat` | Extract timestamp |
| `Value.asRef` | `Value -> Option EntityId` | Extract reference |
| `Value.asKeyword` | `Value -> Option String` | Extract keyword |
| `Value.asBytes` | `Value -> Option ByteArray` | Extract bytes |

**Source**: `Ledger/Core/Value.lean`

### Datom

Fundamental fact unit.

```lean
structure Datom where
  entity : EntityId
  attr : Attribute
  value : Value
  tx : TxId
  added : Bool
```

| Function | Type | Description |
|----------|------|-------------|
| `Datom.assert` | `EntityId -> Attribute -> Value -> TxId -> Datom` | Create assertion |
| `Datom.retract` | `EntityId -> Attribute -> Value -> TxId -> Datom` | Create retraction |

**Source**: `Ledger/Core/Datom.lean`

---

## Database Operations

### Db

Immutable database snapshot.

```lean
structure Db where
  basisT : TxId
  indexes : Indexes
  nextEntityId : EntityId
```

#### Creation

| Function | Type | Description |
|----------|------|-------------|
| `Db.empty` | `Db` | Empty database |
| `Db.size` | `Db -> Nat` | Datom count |

#### Entity ID Allocation

| Function | Type | Description |
|----------|------|-------------|
| `Db.allocEntityId` | `Db -> EntityId * Db` | Allocate single ID |
| `Db.allocEntityIds` | `Db -> Nat -> List EntityId * Db` | Allocate multiple IDs |

#### Transactions

| Function | Type | Description |
|----------|------|-------------|
| `Db.transact` | `Db -> Transaction -> Nat -> Except TxError (Db * TxReport)` | Process transaction |

#### Entity Queries (EAVT)

| Function | Type | Description |
|----------|------|-------------|
| `Db.entity` | `Db -> EntityId -> List Datom` | All datoms for entity |
| `Db.get` | `Db -> EntityId -> Attribute -> List Value` | All visible values |
| `Db.getOne` | `Db -> EntityId -> Attribute -> Option Value` | Most recent visible value |

#### Attribute Queries (AEVT)

| Function | Type | Description |
|----------|------|-------------|
| `Db.datomsWithAttr` | `Db -> Attribute -> List Datom` | All datoms with attribute |
| `Db.entitiesWithAttr` | `Db -> Attribute -> List EntityId` | Entities having attribute |

#### Value Queries (AVET)

| Function | Type | Description |
|----------|------|-------------|
| `Db.entitiesWithAttrValue` | `Db -> Attribute -> Value -> List EntityId` | Find by attr=value |
| `Db.entityWithAttrValue` | `Db -> Attribute -> Value -> Option EntityId` | Find unique by attr=value |

#### Reverse References (VAET)

| Function | Type | Description |
|----------|------|-------------|
| `Db.referencingEntities` | `Db -> EntityId -> List EntityId` | Entities referencing target |
| `Db.referencingDatoms` | `Db -> EntityId -> List Datom` | Datoms referencing target |
| `Db.referencingViaAttr` | `Db -> EntityId -> Attribute -> List EntityId` | References via attribute |

#### General

| Function | Type | Description |
|----------|------|-------------|
| `Db.datoms` | `Db -> List Datom` | All datoms |

**Source**: `Ledger/Db/Database.lean`

---

### Connection

Mutable database reference with history.

```lean
structure Connection where
  db : Db
  txLog : TxLog
```

#### Creation

| Function | Type | Description |
|----------|------|-------------|
| `Connection.create` | `Connection` | New connection with empty db |
| `Connection.current` | `Connection -> Db` | Current database snapshot |
| `Connection.basisT` | `Connection -> TxId` | Current basis transaction |

#### Entity ID Allocation

| Function | Type | Description |
|----------|------|-------------|
| `Connection.allocEntityId` | `Connection -> EntityId * Connection` | Allocate ID |
| `Connection.allocEntityIds` | `Connection -> Nat -> List EntityId * Connection` | Allocate multiple |

#### Transactions

| Function | Type | Description |
|----------|------|-------------|
| `Connection.transact` | `Connection -> Transaction -> Nat -> Except TxError (Connection * TxReport)` | Process transaction |

#### Time Travel

| Function | Type | Description |
|----------|------|-------------|
| `Connection.asOf` | `Connection -> TxId -> Db` | Database at transaction |
| `Connection.since` | `Connection -> TxId -> List Datom` | Datoms since transaction |
| `Connection.txData` | `Connection -> TxId -> Option TxLogEntry` | Transaction data |
| `Connection.entityHistory` | `Connection -> EntityId -> List Datom` | Entity's full history |
| `Connection.attrHistory` | `Connection -> EntityId -> Attribute -> List Datom` | Attribute history |
| `Connection.allTxIds` | `Connection -> List TxId` | All transaction IDs |

**Source**: `Ledger/Db/Connection.lean`

---

## Transaction Types

### TxOp

Transaction operations.

```lean
inductive TxOp where
  | add : EntityId -> Attribute -> Value -> TxOp
  | retract : EntityId -> Attribute -> Value -> TxOp
```

### Transaction

A list of operations: `List TxOp`

### TxError

Transaction errors.

```lean
inductive TxError where
  | factNotFound : EntityId -> Attribute -> Value -> TxError
  | custom : String -> TxError
  | schemaViolation : String -> TxError
```

### TxReport

Transaction result.

```lean
structure TxReport where
  txId : TxId
  txData : Array Datom
  txInstant : Nat
```

**Source**: `Ledger/Tx/Types.lean`

---

## Query Engine

### Query Types

#### Var

Logic variable.

```lean
structure Var where
  name : String
```

| Function | Type | Description |
|----------|------|-------------|
| `Var.ofName` | `String -> Var` | Create (without "?") |

#### Term

Query term.

```lean
inductive Term where
  | entity : EntityId -> Term
  | attr : Attribute -> Term
  | value : Value -> Term
  | var : Var -> Term
  | blank : Term
```

Helpers:
| Function | Type | Description |
|----------|------|-------------|
| `Term.e` | `Int -> Term` | Entity term |
| `Term.a` | `String -> Term` | Attribute term |
| `Term.int` | `Int -> Term` | Int value term |
| `Term.str` | `String -> Term` | String value term |
| `Term.v` | `String -> Term` | Variable term |

#### Pattern

Datom pattern.

```lean
structure Pattern where
  entity : Term
  attr : Term
  value : Term
```

#### Clause

Query clause.

```lean
inductive Clause where
  | pattern : Pattern -> Clause
  | and : List Clause -> Clause
  | or : List Clause -> Clause
  | not : Pattern -> Clause
```

#### Query

Complete query.

```lean
structure Query where
  find : List Var
  where_ : List Clause
```

**Source**: `Ledger/Query/AST.lean`

### QueryBuilder

Fluent query builder.

| Function | Type | Description |
|----------|------|-------------|
| `QueryBuilder.new` | `QueryBuilder` | Create builder |
| `.find` | `QueryBuilder -> String -> QueryBuilder` | Add find variable |
| `.findAll` | `QueryBuilder -> List String -> QueryBuilder` | Add multiple find vars |
| `.where_` | `QueryBuilder -> String -> String -> String -> QueryBuilder` | Add pattern (e, a, v) |
| `.whereEntity` | `QueryBuilder -> EntityId -> String -> String -> QueryBuilder` | Pattern with entity |
| `.whereInt` | `QueryBuilder -> String -> String -> Int -> QueryBuilder` | Pattern with int value |
| `.whereStr` | `QueryBuilder -> String -> String -> String -> QueryBuilder` | Pattern with string |
| `.whereRef` | `QueryBuilder -> String -> String -> EntityId -> QueryBuilder` | Pattern with ref |
| `.whereKeyword` | `QueryBuilder -> String -> String -> String -> QueryBuilder` | Pattern with keyword |
| `.build` | `QueryBuilder -> Query` | Build query |
| `.run` | `QueryBuilder -> Db -> QueryResult` | Execute query |
| `.runRaw` | `QueryBuilder -> Db -> Relation` | Execute raw |

**Shortcut**: `DSL.query : QueryBuilder`

**Source**: `Ledger/DSL/QueryBuilder.lean`

### Query Execution

| Function | Type | Description |
|----------|------|-------------|
| `Query.execute` | `Query -> Db -> QueryResult` | Execute query |
| `Query.executeRaw` | `Query -> Db -> Relation` | Execute returning bindings |

### QueryResult

```lean
structure QueryResult where
  columns : List Var
  rows : Relation
```

| Function | Type | Description |
|----------|------|-------------|
| `.size` | `QueryResult -> Nat` | Number of rows |
| `.isEmpty` | `QueryResult -> Bool` | Check if empty |
| `.toTuples` | `QueryResult -> List (List Value)` | Convert to value lists |

**Source**: `Ledger/Query/Executor.lean`

---

## Pull API

### PullPattern

```lean
inductive PullPattern where
  | attr : Attribute -> PullPattern
  | wildcard : PullPattern
  | nested : Attribute -> List PullPattern -> PullPattern
  | reverse : Attribute -> List PullPattern -> PullPattern
  | limited : Attribute -> Nat -> PullPattern
  | withDefault : Attribute -> String -> PullPattern
```

### PullBuilder

Fluent pull builder.

| Function | Type | Description |
|----------|------|-------------|
| `PullBuilder.new` | `PullBuilder` | Create builder |
| `.attr` | `PullBuilder -> String -> PullBuilder` | Add attribute |
| `.attrs` | `PullBuilder -> List String -> PullBuilder` | Add multiple |
| `.all` | `PullBuilder -> PullBuilder` | Add wildcard |
| `.nested` | `PullBuilder -> String -> List String -> PullBuilder` | Nested pull |
| `.nestedWith` | `PullBuilder -> String -> PullBuilder -> PullBuilder` | Nested with builder |
| `.reverse` | `PullBuilder -> String -> List String -> PullBuilder` | Reverse reference |
| `.reverseWith` | `PullBuilder -> String -> PullBuilder -> PullBuilder` | Reverse with builder |
| `.limited` | `PullBuilder -> String -> Nat -> PullBuilder` | Pull with limit |
| `.withDefault` | `PullBuilder -> String -> String -> PullBuilder` | Pull with default |
| `.build` | `PullBuilder -> PullSpec` | Build specification |
| `.run` | `PullBuilder -> Db -> EntityId -> PullResult` | Execute on entity |
| `.runMany` | `PullBuilder -> Db -> List EntityId -> List PullResult` | Execute on multiple |

**Shortcut**: `DSL.pull : PullBuilder`

**Source**: `Ledger/DSL/PullBuilder.lean`

### Pull Execution

| Function | Type | Description |
|----------|------|-------------|
| `Pull.pull` | `Db -> EntityId -> PullSpec -> PullResult` | Pull with spec |
| `Pull.pullMany` | `Db -> List EntityId -> PullSpec -> List PullResult` | Pull multiple |
| `Pull.pullAttr` | `Db -> EntityId -> Attribute -> Option PullValue` | Pull single attr |
| `Pull.pullWildcard` | `Db -> EntityId -> List (Attribute * PullValue)` | Pull all |

**Source**: `Ledger/Pull/Executor.lean`

---

## Transaction DSL

### TxBuilder

Fluent transaction builder.

| Function | Type | Description |
|----------|------|-------------|
| `TxBuilder.new` | `TxBuilder` | Create builder |
| `.add` | `TxBuilder -> EntityId -> String -> Value -> TxBuilder` | Add assertion |
| `.addStr` | `TxBuilder -> EntityId -> String -> String -> TxBuilder` | Add string |
| `.addInt` | `TxBuilder -> EntityId -> String -> Int -> TxBuilder` | Add int |
| `.addBool` | `TxBuilder -> EntityId -> String -> Bool -> TxBuilder` | Add bool |
| `.addFloat` | `TxBuilder -> EntityId -> String -> Float -> TxBuilder` | Add float |
| `.addRef` | `TxBuilder -> EntityId -> String -> EntityId -> TxBuilder` | Add reference |
| `.addKeyword` | `TxBuilder -> EntityId -> String -> String -> TxBuilder` | Add keyword |
| `.retract` | `TxBuilder -> EntityId -> String -> Value -> TxBuilder` | Add retraction |
| `.retractStr` | `TxBuilder -> EntityId -> String -> String -> TxBuilder` | Retract string |
| `.retractInt` | `TxBuilder -> EntityId -> String -> Int -> TxBuilder` | Retract int |
| `.retractRef` | `TxBuilder -> EntityId -> String -> EntityId -> TxBuilder` | Retract ref |
| `.retractKeyword` | `TxBuilder -> EntityId -> String -> String -> TxBuilder` | Retract keyword |
| `.build` | `TxBuilder -> Transaction` | Build transaction |
| `.run` | `TxBuilder -> Db -> Except TxError (Db * TxReport)` | Execute on Db |
| `.runOn` | `TxBuilder -> Connection -> Except TxError (Connection * TxReport)` | Execute on Connection |

**Shortcut**: `DSL.tx : TxBuilder`

### EntityBuilder

Build attributes for a single entity.

| Function | Type | Description |
|----------|------|-------------|
| `.str` | `EntityBuilder -> String -> String -> EntityBuilder` | Add string |
| `.int` | `EntityBuilder -> String -> Int -> EntityBuilder` | Add int |
| `.bool` | `EntityBuilder -> String -> Bool -> EntityBuilder` | Add bool |
| `.float` | `EntityBuilder -> String -> Float -> EntityBuilder` | Add float |
| `.ref` | `EntityBuilder -> String -> EntityId -> EntityBuilder` | Add ref |
| `.keyword` | `EntityBuilder -> String -> String -> EntityBuilder` | Add keyword |
| `.done` | `EntityBuilder -> TxBuilder` | Return to TxBuilder |

Usage:
```lean
DSL.tx
  |>.entity alice
    |>.str ":person/name" "Alice"
    |>.int ":person/age" 30
  |>.done
```

**Source**: `Ledger/DSL/TxBuilder.lean`

---

## DSL Combinators

High-level convenience functions.

### Entity Operations

| Function | Type | Description |
|----------|------|-------------|
| `DSL.createEntity` | `Db -> String -> Value -> Except TxError (Db * EntityId)` | Create with one attr |
| `DSL.createEntityWith` | `Db -> List (String * Value) -> Except TxError (Db * EntityId)` | Create with multiple |
| `DSL.updateAttr` | `Db -> EntityId -> String -> Value -> Value -> Except TxError Db` | Retract old, add new |
| `DSL.setAttr` | `Db -> EntityId -> String -> Value -> Except TxError Db` | Add without retract |
| `DSL.removeAttr` | `Db -> EntityId -> String -> Value -> Except TxError Db` | Retract value |

### Query Shortcuts

| Function | Type | Description |
|----------|------|-------------|
| `DSL.entitiesWithAttr` | `Db -> String -> List EntityId` | Entities with attribute |
| `DSL.entitiesWithAttrValueStr` | `Db -> String -> String -> List EntityId` | Find by string value |
| `DSL.entitiesWithAttrValueInt` | `Db -> String -> Int -> List EntityId` | Find by int value |
| `DSL.entityWithAttrValueStr` | `Db -> String -> String -> Option EntityId` | Unique string lookup |
| `DSL.allWith` | `Db -> String -> List EntityId` | Deprecated alias for entitiesWithAttr |
| `DSL.findByStr` | `Db -> String -> String -> List EntityId` | Deprecated alias for entitiesWithAttrValueStr |
| `DSL.findByInt` | `Db -> String -> Int -> List EntityId` | Deprecated alias for entitiesWithAttrValueInt |
| `DSL.findOneByStr` | `Db -> String -> String -> Option EntityId` | Deprecated alias for entityWithAttrValueStr |
| `DSL.findEntitiesWith` | `Db -> String -> List EntityId` | Deprecated alias for entitiesWithAttr |
| `DSL.findEntitiesWhereInt` | `Db -> String -> Int -> List EntityId` | Deprecated alias for entitiesWithAttrValueInt |
| `DSL.findEntitiesWhereStr` | `Db -> String -> String -> List EntityId` | Deprecated alias for entitiesWithAttrValueStr |
| `DSL.findEntityByStr` | `Db -> String -> String -> Option EntityId` | Deprecated alias for entityWithAttrValueStr |

### Attribute Access

| Function | Type | Description |
|----------|------|-------------|
| `DSL.getValue` | `Db -> EntityId -> String -> Option Value` | Get single value |
| `DSL.getString` | `Db -> EntityId -> String -> Option String` | Get string |
| `DSL.getInt` | `Db -> EntityId -> String -> Option Int` | Get int |
| `DSL.getBool` | `Db -> EntityId -> String -> Option Bool` | Get bool |
| `DSL.getRef` | `Db -> EntityId -> String -> Option EntityId` | Get reference |
| `DSL.attrStr` | `Db -> EntityId -> String -> Option String` | Alias for getString |
| `DSL.attrInt` | `Db -> EntityId -> String -> Option Int` | Alias for getInt |
| `DSL.attrRef` | `Db -> EntityId -> String -> Option EntityId` | Alias for getRef |
| `DSL.attrAll` | `Db -> EntityId -> String -> List Value` | Get all values |

### Reference Traversal

| Function | Type | Description |
|----------|------|-------------|
| `DSL.follow` | `Db -> EntityId -> String -> Option EntityId` | Follow reference |
| `DSL.followAndGet` | `Db -> EntityId -> String -> String -> Option Value` | Follow and get attr |
| `DSL.referencedBy` | `Db -> EntityId -> List EntityId` | Reverse references |
| `DSL.referencedByVia` | `Db -> EntityId -> String -> List EntityId` | Reverse via attr |

### Connection Operations

| Function | Type | Description |
|----------|------|-------------|
| `DSL.createEntityConn` | `Connection -> String -> Value -> Except TxError (Connection * EntityId)` | Create on connection |
| `DSL.updateAttrConn` | `Connection -> EntityId -> String -> Value -> Value -> Except TxError Connection` | Update on connection |

**Source**: `Ledger/DSL/Combinators.lean`

---

## makeLedgerEntity Code Generation

The `makeLedgerEntity` macro generates database helpers from Lean structures.

### Usage

```lean
-- File: MyTypes.lean
structure Person where
  id : Nat           -- id field is skipped (derived from EntityId)
  name : String
  email : String
  age : Nat

-- File: MyEntities.lean (must be separate file!)
import MyTypes
import Ledger.Derive.LedgerEntity

open Ledger.Derive in
makeLedgerEntity Person
-- or with custom prefix:
makeLedgerEntity Person (attrPrefix := "person")
```

### Generated Functions

For a structure `Person` with fields `name`, `email`, `age`:

#### Attribute Constants

| Generated | Type | Value |
|-----------|------|-------|
| `Person.attr_name` | `Attribute` | `":person/name"` |
| `Person.attr_email` | `Attribute` | `":person/email"` |
| `Person.attr_age` | `Attribute` | `":person/age"` |
| `Person.attributes` | `List Attribute` | All attributes |

#### Pull Specification

| Generated | Type | Description |
|-----------|------|-------------|
| `Person.pullSpec` | `PullSpec` | Pull pattern for all fields |
| `Person.pull` | `Db -> EntityId -> Option Person` | Pull and construct struct |

#### Transaction Builders

| Generated | Type | Description |
|-----------|------|-------------|
| `Person.createOps` | `EntityId -> Person -> Transaction` | Create new entity |
| `Person.retractionOps` | `Db -> EntityId -> List TxOp` | Retract all attributes |
| `Person.updateOps` | `Db -> EntityId -> Person -> List TxOp` | Update all fields (cardinality-one) |

#### Per-Field Setters (Cardinality-One)

| Generated | Type | Description |
|-----------|------|-------------|
| `Person.set_name` | `Db -> EntityId -> String -> List TxOp` | Set name (retracts old) |
| `Person.set_email` | `Db -> EntityId -> String -> List TxOp` | Set email (retracts old) |
| `Person.set_age` | `Db -> EntityId -> Nat -> List TxOp` | Set age (retracts old) |

### Per-Field Setter Behavior

The `set_<field>` functions enforce cardinality-one semantics:

```lean
-- If person already has a name "Alice":
let ops := Person.set_name db eid "Bob"
-- ops = [TxOp.retract eid :person/name "Alice", TxOp.add eid :person/name "Bob"]

-- If value is unchanged:
let ops := Person.set_name db eid "Alice"
-- ops = []  (no-op)

-- If no existing value:
let ops := Person.set_name db eid "Carol"
-- ops = [TxOp.add eid :person/name "Carol"]
```

### Supported Field Types

| Lean Type | Value Constructor | Notes |
|-----------|-------------------|-------|
| `String` | `Value.string` | |
| `Int` | `Value.int` | |
| `Nat` | `Value.int (Int.ofNat x)` | Stored as Int |
| `Bool` | `Value.bool` | |
| `Float` | `Value.float` | |
| `EntityId` | `Value.ref` | Reference to another entity |

### Important Notes

1. **Separate file requirement**: `makeLedgerEntity` must be called in a different file than the structure definition (Lean elaboration ordering).

2. **The `id` field is skipped**: Fields named `id` with type `Nat`, `Int`, or `EntityId` are not stored as attributes - they're derived from the `EntityId`.

3. **Attribute prefix**: Defaults to lowercase struct name. Override with `attrPrefix := "custom"`.

**Source**: `Ledger/Derive/LedgerEntity.lean`

---

## See Also

- [Architecture](architecture.md) - Core concepts
- [Getting Started](getting-started.md) - Tutorial
- [Design Decisions](design-decisions.md) - Design philosophy
