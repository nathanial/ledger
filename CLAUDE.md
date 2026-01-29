# CLAUDE.md

Datomic-inspired fact-based database for Lean 4. Stores data as immutable datoms (entity, attribute, value, transaction, added) with four index types and Datalog-style queries.

## Build & Test

```bash
lake build
lake test
```

## Dependencies

- batteries (v4.26.0) - Lean standard library extensions
- crucible - Test framework
- staple - Macros

## Module Structure

```
Ledger/
├── Core/           # EntityId, Attribute, Value, Datom - fundamental types
├── Index/          # EAVT, AEVT, AVET, VAET indexes + Manager
├── Tx/             # Transaction types and processing
├── Db/             # Database, Connection, TimeTravel
├── Query/          # AST, Binding, Unify, Executor, IndexSelect
├── Pull/           # Pattern, Result, Executor - hierarchical retrieval
├── DSL/            # TxBuilder, QueryBuilder, PullBuilder, TxM, Combinators
├── Derive/         # LedgerEntity macro for code generation
└── Persist/        # JSON/JSONL serialization, persistent Connection
```

## Key Patterns

**Datom structure**: `(EntityId, Attribute, Value, TxId, Bool)` - the Bool indicates added (true) or retracted (false).

**Four indexes** for different access patterns:
- EAVT: Entity lookup ("get all attributes of entity X")
- AEVT: Attribute queries ("get all entities with attribute Y")
- AVET: Value lookups ("find entities where attr=value")
- VAET: Reverse references ("what entities reference entity X")

**DSL builders** - Fluent API for transactions, queries, and pulls:
```lean
DSL.tx |>.addStr entity ":attr" "value"
DSL.query |>.find "x" |>.where_ "e" ":attr" "x" |>.run db
DSL.pull |>.attr ":name" |>.run db entity
```

**Entity code generation** via `makeLedgerEntity`:
```lean
structure Task where
  id : Nat
  title : String
makeLedgerEntity Task "task"  -- generates attr_*, createOps, pull, set_*, updateOps
```

## Cardinality Model

Unlike Datomic, cardinality is query-time rather than schema-declared:
- `getOne` returns most recent value
- `get` returns all visible values

## Time Travel

```lean
conn.asOf txId      -- database snapshot at transaction
conn.since txId     -- changes since transaction
conn.entityHistory entityId  -- full history of an entity
```
