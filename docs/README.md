# Ledger Documentation

Ledger is a Datomic-inspired fact-based database for Lean 4. It stores data as immutable facts (datoms) and provides time-travel queries, Datalog-style queries, and a hierarchical Pull API.

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](architecture.md) | Core concepts, indexes, and the immutability model |
| [Getting Started](getting-started.md) | Tutorial with working examples |
| [Design Decisions](design-decisions.md) | Cardinality model and Datomic comparison |
| [API Reference](api-reference.md) | Complete API documentation |

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

-- Query using direct access
let name := db.getOne alice (Attribute.mk ":person/name")
-- => some (Value.string "Alice")

-- Query using DSL helpers
let age := DSL.getInt db alice ":person/age"
-- => some 30

-- Find entities by attribute value
let entities := db.entitiesWithAttrValue
  (Attribute.mk ":person/name")
  (Value.string "Alice")
-- => [alice]
```

## Key Features

- **Immutable Database Values**: Each transaction produces a new snapshot
- **Time Travel**: Query the database as it existed at any point in history
- **Four-Index System**: EAVT, AEVT, AVET, VAET for efficient querying
- **Datalog Queries**: Pattern-based queries with logic variables
- **Pull API**: Hierarchical data retrieval with nested references
- **Schema-Free**: Use any attributes without prior declaration

## Installation

Add to your `lakefile.lean`:

```lean
require ledger from git
  "https://github.com/yourusername/ledger" @ "main"
```

Then import in your code:

```lean
import Ledger
```
