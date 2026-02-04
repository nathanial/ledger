# Ledger

A Datomic-inspired fact-based database for Lean 4.

Ledger implements an immutable, time-traveling database where all data is stored as facts (datoms). It supports Datalog-style queries, a Pull API for hierarchical entity retrieval, and four index types for efficient access patterns. Queries operate on the current visible facts; full history is preserved for time travel.

## Documentation

See the [docs/](docs/) folder for comprehensive documentation:

- [Architecture](docs/architecture.md) - Core concepts, indexes, and the immutability model
- [Getting Started](docs/getting-started.md) - Tutorial with working examples
- [Design Decisions](docs/design-decisions.md) - Cardinality model and Datomic comparison
- [API Reference](docs/api-reference.md) - Complete API documentation

## Installation

Add to your `lakefile.lean`:

```lean
require ledger from git "https://github.com/nathanial/ledger" @ "master"
```

Then run:

```bash
lake update
lake build
```

## Quick Start

```lean
import Ledger

-- Create a database and allocate an entity ID
let db := Db.empty
let (alice, db) := db.allocEntityId

-- Build and execute a transaction
let tx := DSL.tx
  |>.addStr alice ":person/name" "Alice"
  |>.addInt alice ":person/age" 30
  |>.addStr alice ":person/email" "alice@example.com"
let .ok (db, _) := tx.run db | panic! "Transaction failed"

-- Query: direct attribute access
let name := db.getOne alice (Attribute.mk ":person/name")
-- => some (Value.string "Alice")

-- Query: find entities by value
let alices := db.entitiesWithAttrValue
  (Attribute.mk ":person/name")
  (Value.string "Alice")
-- => [alice]

-- Query: Datalog-style
let result := DSL.query
  |>.find "name"
  |>.where_ "e" ":person/name" "name"
  |>.whereInt "e" ":person/age" 30
  |>.run db
```

## Features

### Datoms

All data is stored as datoms - immutable 5-tuples:

```
(entity, attribute, value, transaction, added)
```

Retractions (`added = false`) remove a fact from the current view while preserving history.

### Four Indexes

| Index | Order | Use Case |
|-------|-------|----------|
| EAVT | Entity -> Attribute -> Value -> Tx | Entity lookup |
| AEVT | Attribute -> Entity -> Value -> Tx | Attribute queries |
| AVET | Attribute -> Value -> Entity -> Tx | Value lookups |
| VAET | Value -> Attribute -> Entity -> Tx | Reverse references |

### Time Travel

Query the database at any point in history:

```lean
let conn := Connection.create
-- ... transactions ...

-- Get database as of a specific transaction
let historicalDb := conn.asOf txId

-- Get changes since a transaction
let changes := conn.since txId

-- Get full history of an entity
let history := conn.entityHistory entityId
```

### Current View vs History

- **Current view**: All queries and indexes are built from visible facts only (retracted facts are removed).
- **History**: Full transaction history is preserved for `asOf`, `since`, and entity history queries.

### Pull API

Retrieve hierarchical entity data:

```lean
let result := DSL.pull
  |>.attr ":person/name"
  |>.attr ":person/age"
  |>.nested ":person/company" [":company/name", ":company/city"]
  |>.run db alice
```

### Query Engine

Datalog-style queries with pattern matching:

```lean
let result := DSL.query
  |>.findAll ["name", "age"]
  |>.where_ "e" ":person/name" "name"
  |>.where_ "e" ":person/age" "age"
  |>.whereStr "e" ":person/city" "New York"
  |>.run db
```

### Transaction DSL

Fluent builders for ergonomic transactions:

```lean
let tx := DSL.tx
  |>.entity alice
    |>.str ":person/name" "Alice"
    |>.int ":person/age" 30
    |>.ref ":person/company" acmeCorp
  |>.done
  |>.addStr acmeCorp ":company/name" "Acme Corp"
```

### Entity Code Generation

Define typed entity structures and generate database operations:

```lean
structure DbTask where
  id : Nat
  title : String
  status : String
  assignee : EntityId

makeLedgerEntity DbTask "task"
-- Generates: attr_*, createOps, pull, set_*, updateOps
```

Per-field setters automatically handle cardinality-one semantics:

```lean
-- Automatically retracts old value before adding new one
let tx := DbTask.set_status db taskId "done"
```

## Architecture

```
Ledger/
├── Core/           # EntityId, Attribute, Value, Datom
├── Index/          # EAVT, AEVT, AVET, VAET indexes
├── Tx/             # Transaction types and processing
├── Db/             # Database, Connection, TimeTravel
├── Query/          # AST, Binding, Unify, Executor
├── Pull/           # Pattern, Result, Executor
└── DSL/            # TxBuilder, QueryBuilder, PullBuilder
```

## Building & Testing

```bash
# Build the library
lake build

# Run tests
lake test
```

## Design Notes

Ledger differs from Datomic in its cardinality model:

- **Datomic**: Cardinality is schema-declared; cardinality-one attributes auto-retract old values
- **Ledger**: Cardinality is query-time; `getOne` returns most recent, `get` returns all visible values

See [Design Decisions](docs/design-decisions.md) for the full rationale.

## License

MIT License - see [LICENSE](LICENSE) for details.

## References

- [Datomic](https://www.datomic.com/) - The original fact-based database
- [Datascript](https://github.com/tonsky/datascript) - Immutable in-memory database for Clojure/ClojureScript
