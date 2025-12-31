# Ledger Roadmap

This document outlines potential improvements, new features, code cleanup opportunities, and technical debt identified in the Ledger codebase.

---

## Feature Proposals

### [Priority: High] Schema System with Attribute Definitions

**Description:** Implement a schema system allowing attributes to be declared with types, cardinality, uniqueness constraints, and indexing preferences.

**Rationale:** Currently Ledger is schema-free which provides flexibility but lacks type safety, cardinality enforcement (one vs. many), uniqueness constraints, and the ability to optimize indexes for specific attributes. A schema system would enable:
- Compile-time type checking for attribute values
- Cardinality validation (`:db.cardinality/one` vs `:db.cardinality/many`)
- Unique value constraints (`:db.unique/identity`, `:db.unique/value`)
- Component relationships for cascading operations
- Better query optimization through attribute metadata

**Affected Files:**
- New file: `Ledger/Schema/Types.lean`
- New file: `Ledger/Schema/Validation.lean`
- Modify: `Ledger/Db/Database.lean` (add schema to Db structure)
- Modify: `Ledger/Tx/Types.lean` (add schema validation errors)
- Modify: `Ledger/DSL/TxBuilder.lean` (add schema-aware builders)

**Estimated Effort:** Large

**Dependencies:** None

---

### [Priority: Medium] Enhanced Persistence Layer

**Description:** Extend the existing JSONL-based persistence with snapshots and memory-mapped indexes.

**Current State:** Basic persistence is implemented in `Ledger/Persist/`:
- `JSONL.lean` - Append-only transaction log with `appendEntry` and `replayJournal`
- `JSON.lean` - JSON serialization for datoms and values
- `Connection.lean` - Persistent connection management

**Remaining Work:**
- Periodic snapshots for faster recovery on large logs
- Memory-mapped indexes for large datasets
- Compaction of old transaction logs

**Affected Files:**
- New file: `Ledger/Persist/Snapshot.lean`
- New file: `Ledger/Persist/Compaction.lean`
- Modify: `Ledger/Persist/Connection.lean`

**Estimated Effort:** Medium

**Dependencies:** None

---

### [Priority: High] Aggregation Functions in Queries

**Description:** Add support for aggregate functions like `count`, `sum`, `avg`, `min`, `max` in queries.

**Rationale:** The query engine currently only supports pattern matching and returns raw bindings. Aggregation is essential for analytics:
```lean
db.query do
  find [?count]
  where_ [[?e, ":person/age", ?age]]
  aggregate [(count ?e)]
```

**Affected Files:**
- Modify: `Ledger/Query/AST.lean` (add aggregate clause)
- New file: `Ledger/Query/Aggregates.lean`
- Modify: `Ledger/Query/Executor.lean` (process aggregates)
- Modify: `Ledger/DSL/QueryBuilder.lean` (add aggregate builders)

**Estimated Effort:** Medium

**Dependencies:** None

---

### [Priority: Medium] Predicate Expressions in Queries

**Description:** Add support for predicate expressions in where clauses like `[(> ?age 21)]`, `[(= ?status "active")]`.

**Rationale:** The README shows predicate syntax but the current implementation in `Query/AST.lean` only supports pattern matching. Predicates would enable:
- Comparisons: `>`, `<`, `>=`, `<=`, `=`, `!=`
- String operations: `contains`, `starts-with`, `ends-with`
- Arithmetic: `+`, `-`, `*`, `/`
- Boolean logic: `and`, `or`, `not`

**Affected Files:**
- Modify: `Ledger/Query/AST.lean` (add predicate clause type)
- New file: `Ledger/Query/Predicate.lean`
- Modify: `Ledger/Query/Executor.lean` (evaluate predicates)
- Modify: `Ledger/DSL/QueryBuilder.lean` (add predicate builders)

**Estimated Effort:** Medium

**Dependencies:** None

---

### [Priority: Medium] Entity Retraction (Cascading Delete)

**Description:** Implement whole-entity retraction that removes all facts about an entity, optionally cascading to component entities.

**Rationale:** Currently, retractions must specify exact attribute-value pairs. There is no way to:
- Retract all facts about an entity at once
- Handle component relationships (where child entities should be retracted with parent)
- Clean up dangling references

**Affected Files:**
- Modify: `Ledger/Tx/Types.lean` (add `TxOp.retractEntity`)
- Modify: `Ledger/Db/Database.lean` (implement entity retraction)
- Modify: `Ledger/DSL/TxBuilder.lean` (add `retractEntity` builder)

**Estimated Effort:** Medium

**Dependencies:** Schema System (for component relationships)

---

### [Priority: Medium] Transaction Functions

**Description:** Allow custom functions to run within transactions for complex multi-step operations with validation.

**Rationale:** Datomic supports transaction functions that can read current database state and produce operations atomically. This enables:
- Increment/decrement operations
- Compare-and-swap patterns
- Custom validation logic
- Conditional transactions

**Affected Files:**
- New file: `Ledger/Tx/Functions.lean`
- Modify: `Ledger/Tx/Types.lean` (add function invocation TxOp)
- Modify: `Ledger/Db/Database.lean` (execute transaction functions)

**Estimated Effort:** Large

**Dependencies:** None

---

### [Priority: Medium] Rules and Recursive Queries

**Description:** Implement Datalog rules for defining derived relationships and recursive queries.

**Rationale:** Rules would enable powerful graph queries like "find all ancestors" or "compute transitive closure". Example:
```lean
rule "ancestor" [?a, ?d] :=
  [?a, ":person/parent", ?d]
  or
  [?a, ":person/parent", ?p], ancestor(?p, ?d)
```

**Affected Files:**
- New file: `Ledger/Query/Rules.lean`
- Modify: `Ledger/Query/AST.lean` (add rule definitions)
- Modify: `Ledger/Query/Executor.lean` (implement rule evaluation)

**Estimated Effort:** Large

**Dependencies:** None

---

### [Priority: Medium] Query Macro DSL

**Description:** Implement a Lean 4 macro-based DSL for writing queries with syntax closer to Datomic's.

**Rationale:** The current builder pattern is verbose. A macro DSL would provide cleaner syntax:
```lean
ledger_query! {
  :find [?name ?age]
  :where [[?e :person/name ?name]
          [?e :person/age ?age]
          [(> ?age 21)]]
}
```

**Note:** Entity code generation via `makeLedgerEntity` macro already exists in `Ledger/Derive/LedgerEntity.lean`, providing attribute constants, pull specs, transaction builders, and TxM monadic helpers. A query DSL would complement this.

**Affected Files:**
- New file: `Ledger/DSL/Macros.lean`
- Modify: `Ledger.lean` (export macro)

**Estimated Effort:** Medium

**Dependencies:** None

---

### [Priority: Low] Full-Text Search Index

**Description:** Add a full-text search index for string attributes.

**Rationale:** The AVET index supports exact value matching but not text search. Full-text search would enable:
- Tokenized text indexing
- Fuzzy matching
- Relevance scoring

**Affected Files:**
- New file: `Ledger/Index/Fulltext.lean`
- Modify: `Ledger/Index/Manager.lean` (manage fulltext index)
- Modify: `Ledger/Query/AST.lean` (add fulltext clause)

**Estimated Effort:** Large

**Dependencies:** Schema System (to mark attributes as fulltext-indexed)

---

### [Priority: Low] Range Queries on Indexes

**Description:** Implement true range queries on RBMap indexes instead of filtering.

**Rationale:** Currently, index queries like `datomsForEntity` iterate the entire index and filter (`filterMap`). The RBMap structure supports efficient range queries but they are not used.

**Affected Files:**
- Modify: `Ledger/Index/EAVT.lean`
- Modify: `Ledger/Index/AEVT.lean`
- Modify: `Ledger/Index/AVET.lean`
- Modify: `Ledger/Index/VAET.lean`

**Estimated Effort:** Medium

**Dependencies:** None

---

### [Priority: Low] Database Listeners/Watchers

**Description:** Implement a mechanism to subscribe to database changes.

**Rationale:** Applications often need to react to data changes:
- UI updates when data changes
- Triggering side effects (notifications, cache invalidation)
- Synchronization with external systems

**Affected Files:**
- New file: `Ledger/Db/Listeners.lean`
- Modify: `Ledger/Db/Connection.lean` (add listener management)

**Estimated Effort:** Medium

**Dependencies:** None

---

### [Priority: Low] Database Statistics and Query Planning

**Description:** Maintain statistics about data distribution and use them for query optimization.

**Rationale:** The current query executor orders patterns by selectivity using a simple heuristic (bound term count). Statistics would enable:
- Cardinality estimation
- Cost-based query planning
- Adaptive query optimization

**Affected Files:**
- New file: `Ledger/Stats/Statistics.lean`
- Modify: `Ledger/Query/IndexSelect.lean` (use statistics)
- Modify: `Ledger/Db/Database.lean` (maintain statistics)

**Estimated Effort:** Large

**Dependencies:** None

---

## Code Improvements

### ~~[Completed] Implement Efficient Range Queries in Indexes~~

**Status:** ✅ Completed

Implemented range query optimization using `ForIn` with early termination:
- Created `Ledger/Index/RBRange.lean` with `collectWhile`, `collectPairsWhile`, `collectFromWhile`
- Added minimum bound constructors and match predicates to all key types in `Types.lean`
- Updated all 4 index files (EAVT, AEVT, AVET, VAET) to use range queries
- Added 12 correctness tests in `Tests/RangeQuery.lean`

**Complexity:** O(s + k) where s = elements before range, k = elements in range. Early termination avoids full O(n) scan.

---

### ~~[Completed] Implement Proper Negation in Query Executor~~

**Status:** ✅ Completed

Implemented proper negation-as-failure semantics in the query executor:
- Refactored `executeClause` to accept an input `Relation` parameter for context threading
- Each clause type now transforms input bindings: pattern joins, and chains, or unions, not filters
- Negation evaluates inner clause with each input binding; keeps bindings where inner produces no matches
- 4 new tests in `Tests/Query.lean` verify negation behavior

**Implementation:** For each binding in the input relation, the negated clause is evaluated. If it produces no matches, the binding passes (negation succeeds).

---

### ~~[Completed] Add DecidableEq Instance for Datom~~

**Status:** ✅ Completed

Added `DecidableEq` instances:
- `DecidableEq` for `ByteArray` (required by Value)
- `DecidableEq` for `Float` (axiom-based, uses bit representation)
- `DecidableEq` for `Value` (derived)
- `DecidableEq` for `Datom` (derived, removed explicit BEq instance)

4 new tests in `Tests/Core.lean` verify the implementation.

---

### [Priority: Medium] Remove Partial Annotations in Pull Executor

**Current State:** The `pullNestedEntity` and `pullPatternRec` functions in `Ledger/Pull/Executor.lean` are marked as `partial`. While cycle detection and depth limits prevent infinite recursion at runtime, the Lean type system cannot verify termination.

**Proposed Change:**
- Use fuel-based recursion with a Nat counter
- Or restructure to use `decreasing_by` with well-founded recursion on visited set size

**Benefits:**
- Remove `partial` annotation
- Total functions are easier to reason about
- Better for potential formal verification

**Affected Files:**
- `Ledger/Pull/Executor.lean` (lines 81-147, mutual block with `partial` annotations)

**Estimated Effort:** Medium

---

### [Priority: Medium] Improve Error Handling in DSL.withNewEntity

**Current State:** In `Ledger/DSL/TxBuilder.lean`, the `withNewEntity` function silently returns a dummy TxReport on error instead of propagating the error.

**Proposed Change:** Return `Except TxError (Db × EntityId × TxReport)` or use the Except monad properly.

**Benefits:**
- Proper error handling
- No silent failures
- Consistent API design

**Affected Files:**
- `Ledger/DSL/TxBuilder.lean` (lines 134-139)

**Estimated Effort:** Small

---

### [Priority: Medium] Add Hashable Instance for Datom

**Current State:** `Datom` lacks a `Hashable` instance, limiting its use in hash-based collections.

**Proposed Change:** Implement `Hashable` for Datom by combining hashes of its components.

**Benefits:**
- Enable use in HashMap/HashSet
- Performance improvement for certain operations
- More flexible data structure options

**Affected Files:**
- `Ledger/Core/Datom.lean`

**Estimated Effort:** Small

---

### [Priority: Medium] Consolidate Key Types with Datom Comparison Functions

**Current State:** There are four separate key types (EAVTKey, AEVTKey, AVETKey, VAETKey) in `Ledger/Index/Types.lean` that duplicate ordering logic that also exists in `Ledger/Core/Datom.lean` (compareEAVT, compareAEVT, etc.).

**Proposed Change:** Consider:
- Use newtype wrappers over Datom with different Ord instances
- Or generate key types and their instances with metaprogramming
- Or consolidate comparison logic to avoid duplication

**Benefits:**
- Reduced code duplication
- Single source of truth for ordering semantics
- Easier maintenance

**Affected Files:**
- `Ledger/Index/Types.lean`
- `Ledger/Core/Datom.lean`

**Estimated Effort:** Medium

---

### [Priority: Medium] Add Ord Instance for Datom

**Current State:** `Datom` has comparison functions (`compareEAVT`, etc.) but no `Ord` instance. The indexes use separate key types instead.

**Proposed Change:** Add an `Ord` instance for Datom (perhaps defaulting to EAVT order) to enable direct use in sorted collections.

**Benefits:**
- Simplify index implementation
- Enable Datom use in sorted containers directly
- Better API ergonomics

**Affected Files:**
- `Ledger/Core/Datom.lean`

**Estimated Effort:** Small

---

### [Priority: Low] Use Array Instead of List in Relation

**Current State:** `Relation` in `Ledger/Query/Binding.lean` uses `List Binding` internally.

**Proposed Change:** Use `Array Binding` for better performance with random access and modifications.

**Benefits:**
- O(1) random access
- Better cache locality
- More efficient joins

**Affected Files:**
- `Ledger/Query/Binding.lean` (lines 96-142)
- `Ledger/Query/Executor.lean`

**Estimated Effort:** Small

---

### [Priority: Low] Cache getAllAttrs Result in Pull.pullWildcard

**Current State:** `getAllAttrs` in `Ledger/Pull/Executor.lean` iterates through entity datoms and deduplicates attributes on every wildcard pull.

**Proposed Change:** Consider caching attribute lists per entity, or using a set for deduplication.

**Benefits:**
- Faster wildcard pulls
- Reduced memory allocations
- Better performance for repeated pulls

**Affected Files:**
- `Ledger/Pull/Executor.lean` (lines 66-77)

**Estimated Effort:** Small

---

## Code Cleanup

### [Priority: High] Add Documentation Comments to All Public APIs

**Issue:** Many public functions lack documentation comments (docstrings). While some have brief comments, a consistent documentation standard would improve usability.

**Location:** Throughout all modules, particularly:
- `Ledger/Query/Executor.lean`
- `Ledger/Pull/Executor.lean`
- `Ledger/DSL/*.lean`

**Action Required:**
1. Add docstrings to all public `def` and `structure` declarations
2. Document parameters, return values, and usage examples
3. Use consistent formatting

**Estimated Effort:** Medium

---

### [Priority: Medium] Standardize Naming Convention for Query Functions

**Issue:** Inconsistent naming between modules. For example:
- `findByAttrValue` vs `datomsForAttrValue`
- `entitiesWithAttr` vs `findEntitiesWith`

**Location:**
- `Ledger/Db/Database.lean`
- `Ledger/DSL/Combinators.lean`
- `Ledger/DSL/QueryBuilder.lean`

**Action Required:**
1. Establish naming convention (verb-first vs noun-first)
2. Rename functions for consistency
3. Add deprecation aliases for backward compatibility

**Estimated Effort:** Small

---

### ~~[Completed] Extract Test Helpers into Separate Module~~

**Status:** ✅ Completed

Tests have been refactored into topic-specific modules:
- `Tests/Core.lean` - Core type tests
- `Tests/Database.lean` - Database operations
- `Tests/Retraction.lean` - Fact retraction tests
- `Tests/Query.lean` - Query engine tests
- `Tests/Pull.lean` - Pull API tests
- `Tests/DSL.lean` - DSL builder tests
- `Tests/Persistence.lean` - Persistence layer tests
- `Tests/Derive.lean` - Code generation tests
- `Tests/Performance.lean` - Performance benchmarks

`Tests/Main.lean` now imports all modules and runs all suites via Crucible.

---

### [Priority: Medium] Add Docstrings to Inductive Constructors

**Issue:** Inductive types like `Value`, `TxOp`, `PullPattern`, `Clause` have constructor documentation in some cases but not all.

**Location:**
- `Ledger/Core/Value.lean`
- `Ledger/Tx/Types.lean`
- `Ledger/Pull/Pattern.lean`
- `Ledger/Query/AST.lean`

**Action Required:**
1. Add docstrings to all constructors explaining their purpose
2. Include usage examples where appropriate

**Estimated Effort:** Small

---

### [Priority: Low] Remove Redundant BEq Instances

**Issue:** Several types derive `DecidableEq` but also define explicit `BEq` instances that are equivalent. For example, `EntityId` derives both `DecidableEq` and defines a `BEq` instance.

**Location:**
- `Ledger/Core/EntityId.lean` (lines 19-20)
- `Ledger/Core/Attribute.lean` (lines 19-20)
- `Ledger/Index/Types.lean` (multiple key types)

**Action Required:**
1. Remove redundant `BEq` instances where `DecidableEq` is derived
2. Or remove `DecidableEq` derivation if only `BEq` is needed

**Estimated Effort:** Small

---

### [Priority: Low] Add Module-Level Documentation

**Issue:** While files have header comments, they lack structured module documentation that would appear in generated docs.

**Location:** All `.lean` files

**Action Required:**
1. Add `/-! ... -/` module documentation at the top of each file
2. Include module purpose, key types, and usage examples
3. Add `@[inherit_doc]` where appropriate

**Estimated Effort:** Small

---

### [Priority: Low] Consolidate Helper Functions

**Issue:** Some helper functions are duplicated or very similar across modules:
- `filterVisible` in `Ledger/Db/Database.lean` vs `filterVisibleAt` in `Ledger/Db/TimeTravel.lean`
- `sameFact` and `groupByFact` in `TimeTravel.lean` could be in Core

**Location:**
- `Ledger/Db/Database.lean`
- `Ledger/Db/TimeTravel.lean`

**Action Required:**
1. Move shared utilities to a common module (e.g., `Ledger/Core/Util.lean`)
2. Reuse rather than reimplement similar logic
3. Document the shared utilities

**Estimated Effort:** Small

---

### [Priority: Low] Use Type Aliases for Clarity

**Issue:** Some types could benefit from clearer aliases:
- `List TxOp` is abbreviated to `Transaction` - good
- `List Datom` is not aliased - could be `DatomSeq` or similar
- `List (Attribute x PullValue)` repeated in Pull code

**Location:**
- `Ledger/Core/Datom.lean`
- `Ledger/Pull/Result.lean`

**Action Required:**
1. Introduce type aliases for commonly used compound types
2. Update usage sites

**Estimated Effort:** Small

---

## Technical Debt

### ~~[Completed] Index Performance (Full Scans)~~

**Status:** ✅ Completed

Resolved by implementing range queries with early termination. See "Implement Efficient Range Queries in Indexes" in Code Improvements section.

---

### ~~[Completed] No Validation of Retractions~~

**Status:** ✅ Completed

Implemented retraction validation in `Db.transact`:
- Added `isFactAsserted` helper function to check if a fact is currently asserted
- Retractions now verify the fact exists in pre-transaction state
- Returns `TxError.factNotFound` with entity, attribute, and value if fact doesn't exist
- 4 new tests in `Tests/Retraction.lean` verify the behavior

---

### [Priority: Medium] Query Executor OR Clause Implementation

**Issue:** The `.or` clause implementation in `Ledger/Query/Executor.lean` (lines 89-91) simply concatenates results without proper handling of variable scoping or deduplication across branches.

**Location:** `Ledger/Query/Executor.lean`

**Impact:**
- Potential duplicate results
- Unexpected variable binding behavior

**Remediation:** Implement proper union semantics with deduplication based on find variables.

---

### [Priority: Medium] TimeTravel.asOf Rebuilds Indexes

**Issue:** `Connection.asOf` in `Ledger/Db/Connection.lean` rebuilds all indexes from scratch by iterating through the transaction log and reinserting datoms.

**Location:** `Ledger/Db/Connection.lean` (lines 77-91)

**Impact:**
- O(n) time for every time-travel query
- Memory allocation for each query
- Not practical for frequent historical queries

**Remediation:** Consider persistent data structures or snapshot caching for common time-travel points.

---

### [Priority: Medium] Binding.merge is O(n^2)

**Issue:** The `merge` function in `Ledger/Query/Binding.lean` does linear lookup for each entry being merged, resulting in O(n*m) complexity.

**Location:** `Ledger/Query/Binding.lean` (lines 79-83)

**Impact:** Slow query execution for queries with many variables.

**Remediation:** Use a hash-based map for bindings instead of association list.

---

### [Priority: Low] Missing Test Coverage

**Issue:** Some features lack test coverage:
- OR clauses in queries
- Limited pull patterns
- TxError handling
- Edge cases in time-travel (empty history, single transaction)

**Location:** `Tests/Main.lean`

**Impact:** Potential bugs in untested code paths.

**Remediation:** Add comprehensive tests for all query clause types and edge cases.

---

### [Priority: Low] Unused TxError Variants

**Issue:** `TxError.invalidEntity` is defined but never used in the codebase.

**Location:** `Ledger/Tx/Types.lean` (line 41)

**Impact:** Dead code that may confuse developers.

**Remediation:** Either use the variant for actual validation, or remove it.

---

## Completed Features

The following major features have been implemented since the initial roadmap:

### Persistence Layer (Basic)
- **Location:** `Ledger/Persist/`
- JSONL append-only transaction log
- JSON serialization for datoms and values
- Journal replay for crash recovery

### Entity Code Generation
- **Location:** `Ledger/Derive/LedgerEntity.lean`
- `makeLedgerEntity` macro generates attribute constants, pull specs, transaction builders
- Per-field setters with cardinality-one semantics
- TxM monadic helpers for entity CRUD

### Test Suite Refactoring
- Tests split into topic-specific modules (Core, Database, Query, Pull, DSL, Persistence, Derive, Performance)
- Uses Crucible test framework

### Range Query Optimization
- **Location:** `Ledger/Index/RBRange.lean` and all index files
- Uses `ForIn` with early termination for O(s + k) complexity
- Avoids full list allocation from `RBMap.toList`
- 12 correctness tests in `Tests/RangeQuery.lean`

### DecidableEq for Core Types
- **Location:** `Ledger/Core/Value.lean`, `Ledger/Core/Datom.lean`
- Added `DecidableEq` for `ByteArray`, `Float`, `Value`, and `Datom`
- Enables use in type-theoretic proofs and more Lean 4 idioms
- 4 tests in `Tests/Core.lean`

### Retraction Validation
- **Location:** `Ledger/Db/Database.lean`
- Validates that facts exist before allowing retraction
- Returns `TxError.factNotFound` for invalid retractions
- 4 tests in `Tests/Retraction.lean`

### Proper Query Negation
- **Location:** `Ledger/Query/Executor.lean`
- Refactored `executeClause` to accept input `Relation` for context threading
- Implemented negation-as-failure: filter bindings where inner clause produces no matches
- All clause types (pattern, and, or, not) transform input bindings correctly
- 4 tests in `Tests/Query.lean`

---

## Summary

This roadmap identifies improvements across several categories:

| Category | High | Medium | Low | Total |
|----------|------|--------|-----|-------|
| Features | 2 | 7 | 4 | 13 |
| Code Improvements | 1 | 4 | 3 | 8 |
| Code Cleanup | 1 | 2 | 5 | 8 |
| Technical Debt | 0 | 3 | 2 | 5 |

**Priority Focus:**
1. **Correctness:** OR clause semantics (negation now complete ✅)
2. **Features:** Schema system, aggregation, and enhanced persistence would significantly expand use cases
3. **Developer Experience:** Documentation, naming consistency, and macro DSL
