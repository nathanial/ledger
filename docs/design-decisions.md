# Design Decisions

This document explains key design decisions in Ledger, particularly how it differs from Datomic and why.

## Schema-Free Design

Ledger operates in **schema-free mode**: any attribute can be used without prior declaration.

### Why Schema-Free?

1. **Rapid prototyping**: Add attributes without migration steps
2. **Flexible data**: Different entities can have different attributes
3. **Gradual typing**: Start loose, add constraints later
4. **Simpler core**: No schema validation in the transaction path

### Tradeoffs

| Benefit | Cost |
|---------|------|
| No upfront schema design | No type checking on values |
| Easy attribute addition | No cardinality enforcement |
| Heterogeneous entities | No uniqueness constraints |
| Simpler transactions | Validation is application's responsibility |

### Future Direction

A schema system is planned (see [ROADMAP.md](../ROADMAP.md)) that will add optional:
- Type declarations for attributes
- Cardinality constraints (one vs many)
- Uniqueness constraints
- Index optimization hints

## Cardinality Model

This is the most significant difference from Datomic.

### The Core Difference

In **Datomic**, cardinality is declared in the schema:
- `:db.cardinality/one` - Only one value allowed; new assertions auto-retract old
- `:db.cardinality/many` - Multiple values allowed

In **Ledger**, cardinality is a query-time choice:
- `get` - Returns all visible values (cardinality-many semantics)
- `getOne` - Returns the most recently asserted visible value (cardinality-one semantics)

### Example: The Difference in Action

**Datomic with cardinality-one:**
```clojure
;; Schema declares :person/name as cardinality one
(d/transact conn [{:person/name "Alice"}])  ; tx1
(d/transact conn [{:person/name "Alicia"}]) ; tx2

;; Query returns only "Alicia"
;; "Alice" was automatically retracted by tx2
```

**Ledger:**
```lean
-- No schema declaration
tx1 := [.add alice ":person/name" "Alice"]
tx2 := [.add alice ":person/name" "Alicia"]

-- After both transactions:
db.getOne alice ":person/name"  -- => some "Alicia" (most recent)
db.get alice ":person/name"     -- => ["Alice", "Alicia"] (both!)
```

### Why This Design?

#### 1. Maximum Information Preservation

Every assertion is stored as a fact. Nothing is implicitly discarded:

```lean
-- You can always see what happened
let history := conn.attrHistory alice (Attribute.mk ":person/name")
-- Shows: tx1 added "Alice", tx2 added "Alicia"
```

#### 2. Query-Time Flexibility

The same data can be interpreted differently depending on context:

```lean
-- Treat as single-valued (take most recent)
let currentName := db.getOne alice ":person/name"

-- Treat as multi-valued (all names ever used)
let allNames := db.get alice ":person/name"
```

#### 3. Explicit Over Implicit

Retractions are always intentional acts:

```lean
-- To "replace" a value, explicitly retract and add:
let tx := DSL.tx
  |>.retractStr alice ":person/name" "Alice"
  |>.addStr alice ":person/name" "Alicia"
```

This makes it clear in the transaction log what happened.

#### 4. Simpler Transaction Processing

No lookup required during assertion:
- Datomic cardinality-one: Must find existing value, create retraction, then add
- Ledger: Just add the new assertion

#### 5. Audit/Compliance Friendly

Full history is always available:
- No implicit deletions
- Every state transition is explicit
- "Why did this change?" is always answerable

### When to Use Which

**Use `getOne` when:**
- The attribute logically has one value (name, email, status)
- You want the "current" value
- You're migrating from a cardinality-one Datomic schema

**Use `get` when:**
- The attribute logically has multiple values (tags, roles, aliases)
- You want all values ever asserted (that weren't retracted)
- You're building a history view

### The Explicit Update Pattern

For single-valued attributes, use explicit retract+add:

```lean
-- Helper function for cardinality-one updates
def updateOne (db : Db) (e : EntityId) (attr : String) (old new : Value)
    : Except TxError Db := do
  DSL.updateAttr db e attr old new

-- Or build it manually
let tx := DSL.tx
  |>.retractStr e ":person/status" "active"
  |>.addStr e ":person/status" "inactive"
```

### Common Pitfall

Forgetting to retract when updating:

```lean
-- WRONG: This creates multiple values!
tx1 := [.add alice ":person/email" "alice@old.com"]
tx2 := [.add alice ":person/email" "alice@new.com"]
-- db.get returns BOTH emails

-- CORRECT: Retract old, add new
tx2 := [
  .retract alice ":person/email" "alice@old.com",
  .add alice ":person/email" "alice@new.com"
]
```

## Comparison with Datomic

### Similarities

| Feature | Ledger | Datomic |
|---------|--------|---------|
| Datom structure | 5-tuple (e, a, v, tx, added) | Same |
| Four indexes | EAVT, AEVT, AVET, VAET | Same |
| Immutable database | Yes | Yes |
| Time travel | Yes | Yes |
| Transaction log | Yes | Yes |
| Pull API | Yes | Yes |
| Datalog queries | Yes | Yes |

### Differences

| Feature | Ledger | Datomic |
|---------|--------|---------|
| Schema | Optional (future) | Required |
| Cardinality | Query-time choice | Schema-declared |
| Auto-retraction | Never | Cardinality-one does |
| Retraction validation | Accepted silently | Validates fact exists |
| Uniqueness | Not enforced | Schema-declared |
| Value types | 8 types | Similar + BigInt, BigDec, URI |
| Persistence | In-memory only | Durable storage |
| Distribution | Single-process | Distributed |

### Migration from Datomic

If migrating from Datomic:

1. **Cardinality-many attributes**: Work identically, use `get`
2. **Cardinality-one attributes**: Use `getOne`, add explicit retractions on update
3. **Unique attributes**: Enforce in application logic for now
4. **Schema**: Not required, but document your conventions

## Retraction Semantics

### Current Behavior

Retractions require the exact value:

```lean
-- Only retracts if this exact value exists
.retract alice ":person/age" (Value.int 30)
```

Retracting a non-existent fact is currently **silently accepted**. This is noted as technical debt in the ROADMAP.

### Why Exact Values?

Datomic also requires exact values for retraction. This enables:
- Precise control over what's removed
- Concurrent updates without lost updates
- Clear audit trail of what was retracted

### Retract vs. "Clear"

Ledger doesn't have a "clear all values for attribute" operation. To clear:

```lean
-- Get all values, retract each
let values := db.get entity attr
let ops := values.map fun v => .retract entity attr v
let tx := ops
```

## Query Design

### Why Datalog?

Datalog provides:
- Declarative pattern matching
- Automatic join optimization
- Recursive queries (future)
- Familiar to Datomic users

### Pattern-Based Queries

Queries match against datoms using patterns:

```lean
-- Pattern: [?e :person/name ?name]
-- Matches datoms where attribute is :person/name
-- Binds entity to ?e, value to ?name
```

### Query-Time Filtering

Queries see the current database state:
- Only visible values (not retracted) are matched
- Historical queries require `conn.asOf`

## Pull API Design

### Why Pull?

The Pull API addresses a common pattern: "Get me this entity with related data."

Without Pull:
```lean
let name := db.getOne person ":person/name"
let companyId := db.getOne person ":person/company"
let companyName := db.getOne companyId.get! ":company/name"
-- Multiple queries, manual reference following
```

With Pull:
```lean
let result := DSL.pull
  |>.attr ":person/name"
  |>.nested ":person/company" [":company/name"]
  |>.run db person
-- Single operation, declarative specification
```

### Reverse References

Pull can follow references backwards:

```lean
-- Who works at this company?
let result := DSL.pull
  |>.attr ":company/name"
  |>.reverse ":person/company" [":person/name"]
  |>.run db company
```

This uses the VAET index for efficient reverse lookups.

## Future Directions

### Planned: Schema System

Will add optional schema with:
- Type declarations
- Cardinality enforcement (one/many)
- Uniqueness constraints
- Custom validators

### Planned: Retraction Validation

Will validate that retracted facts exist, with options for:
- Strict mode (error on non-existent)
- Lenient mode (current behavior)

### Implemented: Cardinality-One Helpers via makeLedgerEntity

The `makeLedgerEntity` macro now generates per-field setters that automatically handle cardinality-one semantics:

```lean
-- Define your entity structure
structure Person where
  id : Nat
  name : String
  email : String
  age : Nat

-- Generate helpers (in a separate file)
makeLedgerEntity Person

-- Now you can use:
-- Person.set_name db eid "Alice"   -- retracts old value if present, then adds
-- Person.set_email db eid "alice@new.com"
-- Person.updateOps db eid person   -- updates all fields at once
```

The generated `set_<field>` functions:
1. Check for an existing value with `db.getOne`
2. If a value exists and differs, emit a `retract` then `add`
3. If no value exists, just emit an `add`
4. If the value is unchanged, emit nothing (no-op)

This solves the "forgot to retract" problem at the code generation level rather than the database level, keeping the core database simple and schema-free while providing convenience for application code.

See [API Reference](api-reference.md#makeledgerentity-code-generation) for complete documentation.

## See Also

- [Architecture](architecture.md) - Core concepts
- [Getting Started](getting-started.md) - Tutorial
- [API Reference](api-reference.md) - Complete API
- [ROADMAP.md](../ROADMAP.md) - Planned features
