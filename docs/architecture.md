# Ledger Architecture

This document explains the core concepts and architecture of Ledger.

## Core Concepts

### Datoms

The fundamental unit of information in Ledger is a **datom** - an immutable 5-tuple:

```
(entity, attribute, value, transaction, added)
```

| Component | Type | Description |
|-----------|------|-------------|
| `entity` | `EntityId` | The entity this fact is about |
| `attribute` | `Attribute` | The property being described |
| `value` | `Value` | The value of the property |
| `transaction` | `TxId` | When this fact was recorded |
| `added` | `Bool` | `true` for assertion, `false` for retraction |

Example datoms:

```
(1, :person/name, "Alice", 100, true)   -- Alice's name was asserted in tx 100
(1, :person/age, 30, 100, true)         -- Alice's age was asserted in tx 100
(1, :person/age, 30, 105, false)        -- Alice's age was retracted in tx 105
(1, :person/age, 31, 105, true)         -- New age asserted in tx 105
```

### EntityId

An `EntityId` uniquely identifies an entity in the database.

```lean
structure EntityId where
  id : Int
  deriving BEq, Ord, Hashable
```

- **Positive IDs**: Permanent entities allocated by the database
- **Negative IDs**: Temporary IDs resolved during transaction processing
- `EntityId.null`: Represents an invalid/null entity

Allocate entity IDs before using them:

```lean
let (entityId, db) := db.allocEntityId
let (manyIds, db) := db.allocEntityIds 10
```

### Attribute

Attributes identify properties of entities. By convention, attributes use keyword-style names with namespaces:

```lean
structure Attribute where
  name : String
  deriving BEq, Ord, Hashable

-- Examples
Attribute.mk ":person/name"
Attribute.mk ":person/age"
Attribute.mk ":order/items"
```

Built-in attributes:
- `Attribute.dbIdent` - Entity identifier (`:db/ident`)
- `Attribute.dbDoc` - Documentation (`:db/doc`)
- `Attribute.dbTxInstant` - Transaction timestamp (`:db/txInstant`)

### Value

Values represent the data stored in datoms. Ledger supports 8 value types:

```lean
inductive Value where
  | int : Int → Value           -- 64-bit signed integer
  | float : Float → Value       -- 64-bit floating point
  | string : String → Value     -- UTF-8 string
  | bool : Bool → Value         -- Boolean
  | instant : Nat → Value       -- Unix timestamp (milliseconds)
  | ref : EntityId → Value      -- Reference to another entity
  | keyword : String → Value    -- Keyword/symbol
  | bytes : ByteArray → Value   -- Raw bytes
```

Constructors and extractors:

```lean
-- Construction
Value.ofInt 42
Value.ofString "hello"
Value.ofRef entityId

-- Extraction
(Value.int 42).asInt    -- some 42
(Value.ref e).asRef     -- some e
(Value.string "x").asInt -- none
```

### TxId

Transaction IDs are monotonically increasing identifiers:

```lean
structure TxId where
  id : Nat
  deriving BEq, Ord
```

- `TxId.genesis`: The initial transaction ID (0)
- `TxId.next`: Get the next transaction ID

Every datom records which transaction created it, enabling time-travel queries.

## The Four Indexes

Ledger maintains four indexes over **current visible facts** (assertions minus retractions), each optimized for different query patterns:

### EAVT (Entity-Attribute-Value-Transaction)

**Primary use**: Entity lookups - "What do I know about entity X?"

```
Key order: Entity → Attribute → Value → Transaction
```

Operations:
- `db.entity e` - Get all current datoms for an entity
- `db.get e attr` - Get values for an entity's attribute
- `db.getOne e attr` - Get single value (most recent)

### AEVT (Attribute-Entity-Value-Transaction)

**Primary use**: Attribute queries - "Which entities have attribute X?"

```
Key order: Attribute → Entity → Value → Transaction
```

Operations:
- `db.datomsWithAttr attr` - Get all current datoms with attribute
- `db.entitiesWithAttr attr` - Get entities having attribute

### AVET (Attribute-Value-Entity-Transaction)

**Primary use**: Value lookups - "Which entities have attribute X = value Y?"

```
Key order: Attribute → Value → Entity → Transaction
```

Operations:
- `db.entitiesWithAttrValue attr value` - Find entities matching attr=value
- `db.entityWithAttrValue attr value` - Find single entity (unique lookup)

### VAET (Value-Attribute-Entity-Transaction)

**Primary use**: Reverse references - "Who references entity X?"

```
Key order: Value → Attribute → Entity → Transaction
```

Only stores datoms where the value is a reference (`Value.ref`).

Operations:
- `db.referencingEntities target` - Get entities referencing target
- `db.referencingDatoms target` - Get datoms referencing target
- `db.referencingViaAttr target attr` - Get entities referencing via specific attribute

## Current View and History

Ledger keeps both:
- **Current indexes** (`Db.indexes`): only visible facts.
- **History indexes** (`Db.historyIndexes`): all datoms, including retractions.

To make current updates fast, Ledger also maintains a `currentFacts` map keyed by
`(entity, attribute, value)` so inserts/retractions can update the current indexes
without range scans.

## Immutability Model

### Database as a Value

The `Db` type represents an immutable database snapshot:

```lean
structure Db where
  basisT : TxId                             -- Most recent transaction
  indexes : Indexes                         -- Current visible facts
  historyIndexes : Indexes                  -- Full history
  currentFacts : Std.HashMap FactKey Datom  -- Current fact map
  nextEntityId : EntityId                   -- Next available entity ID
```

Transactions produce new database values:

```lean
let db := Db.empty
let (db', report) := db.transact transaction  -- db is unchanged, db' is new
```

This enables:
- **Consistent reads**: Queries against a `Db` always see the same data
- **Safe concurrency**: Multiple readers can query different snapshots
- **Easy testing**: Compare database states before/after operations

### Connection for Mutable State

The `Connection` type wraps a database with a transaction log:

```lean
structure Connection where
  db : Db           -- Current database state
  txLog : TxLog     -- Full transaction history
```

Use `Connection` when you need:
- Mutable state across multiple transactions
- Time-travel queries
- Transaction history

```lean
let conn := Connection.create
let (conn, report) := conn.transact tx
let currentDb := conn.current
```

## Time Travel

Ledger preserves complete transaction history, enabling queries at any point in time.

### Point-in-Time Queries

Get the database as it existed at a specific transaction:

```lean
let historicalDb := conn.asOf txId
let oldValue := historicalDb.getOne entity attr
```

### Changes Since

Get all datoms asserted or retracted since a transaction:

```lean
let changes := conn.since txId
for datom in changes do
  if datom.added then
    IO.println s!"Added: {datom}"
  else
    IO.println s!"Retracted: {datom}"
```

### Entity History

Get the complete history of an entity or attribute:

```lean
-- All datoms ever recorded for an entity
let history := conn.entityHistory entityId

-- History of a specific attribute
let nameHistory := conn.attrHistory entityId (Attribute.mk ":person/name")
```

### Transaction Data

Retrieve the datoms from a specific transaction:

```lean
match conn.txData txId with
| some entry =>
    IO.println s!"Transaction {txId} had {entry.datoms.size} datoms"
| none =>
    IO.println "Transaction not found"
```

## Transaction Processing

Transactions are lists of operations applied atomically:

```lean
inductive TxOp where
  | add : EntityId → Attribute → Value → TxOp      -- Assert a fact
  | retract : EntityId → Attribute → Value → TxOp  -- Retract a fact
```

Processing a transaction:
1. Allocate a new `TxId`
2. Convert each `TxOp` to a `Datom`
3. Insert datoms into `historyIndexes`
4. Update `currentFacts` and current `indexes` (remove prior fact, insert newest)
5. Return new database and transaction report

```lean
let tx : Transaction := [
  .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
  .add alice (Attribute.mk ":person/age") (Value.int 30)
]

match db.transact tx with
| .ok (db', report) =>
    IO.println s!"Transaction {report.txId} committed {report.txData.size} datoms"
| .error e =>
    IO.println s!"Transaction failed: {e}"
```

## See Also

- [Getting Started](getting-started.md) - Tutorial with examples
- [Design Decisions](design-decisions.md) - Why Ledger works this way
- [API Reference](api-reference.md) - Complete API documentation
