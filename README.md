# Ledger

A Datomic-like fact-based database for Lean 4.

Ledger implements an immutable, time-traveling database where all data is stored as facts (datoms). It supports Datalog-style queries, a pull API for entity retrieval, and multiple index types for efficient access patterns.

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

-- Create a connection
let conn ← Connection.create

-- Transact some data
conn.transact [
  TxData.add tempId "person/name" (Value.string "Alice"),
  TxData.add tempId "person/age" (Value.int 30)
]

-- Query the database
let db ← conn.db
let results ← db.query do
  find [?name, ?age]
  where_ [
    [?e, "person/name", ?name],
    [?e, "person/age", ?age]
  ]
```

## Features

### Datoms

All data is stored as datoms: `[entity, attribute, value, transaction, added?]`

### Indexes

Four index types for different access patterns:

| Index | Order | Use Case |
|-------|-------|----------|
| EAVT | Entity → Attribute → Value → Tx | Entity lookup |
| AEVT | Attribute → Entity → Value → Tx | Attribute queries |
| AVET | Attribute → Value → Entity → Tx | Value lookups |
| VAET | Value → Attribute → Entity → Tx | Reverse references |

### Time Travel

Access the database at any point in history:

```lean
let historicalDb ← conn.asOf txId
let pastResults ← historicalDb.query ...
```

### Pull API

Retrieve entity trees with pattern specifications:

```lean
let person ← db.pull [
  "person/name",
  "person/age",
  { "person/friends" => ["person/name"] }
] entityId
```

### Query Engine

Datalog-style queries with unification:

```lean
db.query do
  find [?name]
  where_ [
    [?e, "person/name", ?name],
    [?e, "person/age", ?age],
    [(> ?age 21)]
  ]
```

## Architecture

```
Ledger/
├── Core/           # EntityId, Attribute, Value, Datom
├── Index/          # EAVT, AEVT, AVET, VAET indexes
├── Tx/             # Transaction types and processing
├── Db/             # Database, TimeTravel, Connection
├── Query/          # AST, Binding, Unify, Executor
├── Pull/           # Pattern, Result, Executor
└── DSL/            # Query, Pull, Tx builders
```

## Building & Testing

```bash
# Build the library
lake build

# Run tests
lake test
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## References

- [Datomic](https://www.datomic.com/) - The original fact-based database
- [Datascript](https://github.com/tonsky/datascript) - Immutable in-memory database
