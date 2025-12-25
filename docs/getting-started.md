# Getting Started with Ledger

This tutorial walks through the essential features of Ledger with working examples.

## Installation

Add Ledger to your `lakefile.lean`:

```lean
require ledger from git
  "https://github.com/yourusername/ledger" @ "main"
```

Import in your code:

```lean
import Ledger
```

## Creating a Database

Start with an empty database:

```lean
let db := Db.empty
```

Or use a `Connection` for mutable state with history:

```lean
let conn := Connection.create
```

## Creating Entities

### Allocating Entity IDs

Before asserting facts about an entity, allocate an ID:

```lean
-- Single entity
let (alice, db) := db.allocEntityId

-- Multiple entities
let (ids, db) := db.allocEntityIds 3
let bob := ids[0]!
let carol := ids[1]!
let dave := ids[2]!
```

With a Connection:

```lean
let (alice, conn) := conn.allocEntityId
```

### Building Transactions

Use the `TxBuilder` DSL for ergonomic transaction building:

```lean
let tx := DSL.tx
  |>.addStr alice ":person/name" "Alice"
  |>.addInt alice ":person/age" 30
  |>.addStr alice ":person/email" "alice@example.com"
  |>.addBool alice ":person/active" true

let .ok (db, report) := tx.run db | panic! "Transaction failed"
```

Or use the raw API:

```lean
let tx : Transaction := [
  .add alice (Attribute.mk ":person/name") (Value.string "Alice"),
  .add alice (Attribute.mk ":person/age") (Value.int 30)
]

let .ok (db, report) := db.transact tx | panic! "Transaction failed"
```

### Entity Builder for Single Entities

Build all attributes for one entity fluently:

```lean
let tx := DSL.tx
  |>.entity alice
    |>.str ":person/name" "Alice"
    |>.int ":person/age" 30
    |>.bool ":person/active" true
  |>.done

let .ok (db, _) := tx.run db | panic! "Failed"
```

## Querying Data

### Direct Attribute Access

The simplest way to read data:

```lean
-- Get single value (most recently asserted)
let name := db.getOne alice (Attribute.mk ":person/name")
-- => some (Value.string "Alice")

-- Get all values for an attribute
let allNames := db.get alice (Attribute.mk ":person/name")
-- => [Value.string "Alice"]
```

DSL helpers with type extraction:

```lean
let name := DSL.getString db alice ":person/name"  -- Option String
let age := DSL.getInt db alice ":person/age"       -- Option Int
let active := DSL.getBool db alice ":person/active" -- Option Bool
```

### Finding Entities by Value

Find entities where an attribute has a specific value:

```lean
-- Find all entities with :person/name = "Alice"
let alices := db.findByAttrValue
  (Attribute.mk ":person/name")
  (Value.string "Alice")

-- Find unique entity (e.g., by email)
let user := db.findOneByAttrValue
  (Attribute.mk ":person/email")
  (Value.string "alice@example.com")
```

DSL shortcuts:

```lean
let users := DSL.findByStr db ":person/name" "Alice"
let user := DSL.findOneByStr db ":person/email" "alice@example.com"
```

### Finding Entities with an Attribute

```lean
-- All entities that have a :person/name attribute
let people := db.entitiesWithAttr (Attribute.mk ":person/name")

-- DSL version
let people := DSL.allWith db ":person/name"
```

### Datalog Queries

For complex queries, use the query builder:

```lean
-- Find names of people aged 30
let result := DSL.query
  |>.find "name"
  |>.where_ "e" ":person/name" "name"
  |>.whereInt "e" ":person/age" 30
  |>.run db

for row in result.rows do
  match row[Var.ofName "name"]? with
  | some (Value.string name) => IO.println s!"Found: {name}"
  | _ => pure ()
```

Query with multiple patterns:

```lean
-- Find people in the same city as Alice
let result := DSL.query
  |>.findAll ["name", "city"]
  |>.where_ "alice" ":person/name" "_"      -- bind alice
  |>.whereStr "alice" ":person/name" "Alice"
  |>.where_ "alice" ":person/city" "city"   -- get alice's city
  |>.where_ "e" ":person/city" "city"       -- find others in same city
  |>.where_ "e" ":person/name" "name"       -- get their names
  |>.run db
```

### Pull API

Retrieve hierarchical data with the Pull API:

```lean
-- Pull specific attributes
let result := DSL.pull
  |>.attr ":person/name"
  |>.attr ":person/age"
  |>.attr ":person/email"
  |>.run db alice

-- Pull all attributes
let result := DSL.pull
  |>.all
  |>.run db alice
```

Pull with nested references:

```lean
-- Pull person with their company details
let result := DSL.pull
  |>.attr ":person/name"
  |>.nested ":person/company" [":company/name", ":company/city"]
  |>.run db alice
```

Pull reverse references:

```lean
-- Pull company with all employees
let result := DSL.pull
  |>.attr ":company/name"
  |>.reverse ":person/company" [":person/name", ":person/role"]
  |>.run db companyId
```

## Updating and Retracting

### Updating Values

To update an attribute, retract the old value and add the new one:

```lean
let tx := DSL.tx
  |>.retractInt alice ":person/age" 30
  |>.addInt alice ":person/age" 31

let .ok (db, _) := tx.run db | panic! "Failed"
```

DSL helper for updates:

```lean
let .ok db := DSL.updateAttr db alice ":person/age"
  (Value.int 30)  -- old value
  (Value.int 31)  -- new value
  | panic! "Failed"
```

### Retracting Facts

Remove a fact from the database:

```lean
let tx := DSL.tx
  |>.retractStr alice ":person/email" "alice@example.com"

let .ok (db, _) := tx.run db | panic! "Failed"
```

**Important**: Retractions require the exact value. See [Design Decisions](design-decisions.md) for details on the cardinality model.

## Working with References

References create relationships between entities:

```lean
-- Create a company and employee
let (company, db) := db.allocEntityId
let (employee, db) := db.allocEntityId

let tx := DSL.tx
  |>.addStr company ":company/name" "Acme Corp"
  |>.addStr employee ":person/name" "Alice"
  |>.addRef employee ":person/company" company  -- Reference!

let .ok (db, _) := tx.run db | panic! "Failed"
```

### Following References

```lean
-- Get the company entity Alice works for
let companyId := DSL.getRef db alice ":person/company"

-- Follow reference and get an attribute
let companyName := DSL.followAndGet db alice ":person/company" ":company/name"
```

### Reverse Reference Queries

Find entities that reference a given entity:

```lean
-- Who references this company?
let employees := db.referencingEntities company

-- Who references this company via :person/company?
let employees := db.referencingViaAttr company (Attribute.mk ":person/company")

-- DSL version
let employees := DSL.referencedByVia db company ":person/company"
```

## Time Travel Queries

Using a `Connection`, you can query historical states:

### Point-in-Time Queries

```lean
let conn := Connection.create
let (alice, conn) := conn.allocEntityId

-- Transaction 1: Set age to 30
let tx1 := [.add alice (Attribute.mk ":person/age") (Value.int 30)]
let .ok (conn, report1) := conn.transact tx1 | panic! "Failed"
let tx1Id := report1.txId

-- Transaction 2: Update age to 31
let tx2 := [
  .retract alice (Attribute.mk ":person/age") (Value.int 30),
  .add alice (Attribute.mk ":person/age") (Value.int 31)
]
let .ok (conn, _) := conn.transact tx2 | panic! "Failed"

-- Current database shows age 31
let currentAge := conn.current.getOne alice (Attribute.mk ":person/age")
-- => some (Value.int 31)

-- Historical database shows age 30
let historicalDb := conn.asOf tx1Id
let pastAge := historicalDb.getOne alice (Attribute.mk ":person/age")
-- => some (Value.int 30)
```

### Viewing History

```lean
-- All datoms ever recorded for an entity
let history := conn.entityHistory alice

-- History of a specific attribute
let ageHistory := conn.attrHistory alice (Attribute.mk ":person/age")
for datom in ageHistory do
  let op := if datom.added then "asserted" else "retracted"
  IO.println s!"Tx {datom.tx.id}: {op} {datom.value}"
```

### Changes Since a Transaction

```lean
let changes := conn.since tx1Id
for datom in changes do
  IO.println s!"{datom}"
```

## Complete Example: Task Manager

Here's a complete example building a simple task manager:

```lean
import Ledger

def main : IO Unit := do
  -- Initialize
  let conn := Connection.create

  -- Create users
  let (alice, conn) := conn.allocEntityId
  let (bob, conn) := conn.allocEntityId

  let userTx := DSL.tx
    |>.addStr alice ":user/name" "Alice"
    |>.addStr alice ":user/email" "alice@example.com"
    |>.addStr bob ":user/name" "Bob"
    |>.addStr bob ":user/email" "bob@example.com"

  let .ok (conn, _) := userTx.runOn conn | panic! "Failed to create users"

  -- Create tasks
  let (task1, conn) := conn.allocEntityId
  let (task2, conn) := conn.allocEntityId

  let taskTx := DSL.tx
    |>.addStr task1 ":task/title" "Write documentation"
    |>.addKeyword task1 ":task/status" "in-progress"
    |>.addRef task1 ":task/assignee" alice
    |>.addStr task2 ":task/title" "Review PR"
    |>.addKeyword task2 ":task/status" "pending"
    |>.addRef task2 ":task/assignee" bob

  let .ok (conn, _) := taskTx.runOn conn | panic! "Failed to create tasks"

  -- Query: Find all in-progress tasks
  let inProgress := DSL.query
    |>.findAll ["task", "title"]
    |>.where_ "task" ":task/title" "title"
    |>.whereKeyword "task" ":task/status" "in-progress"
    |>.run conn.current

  IO.println "In-progress tasks:"
  for row in inProgress.rows do
    match row[Var.ofName "title"]? with
    | some (Value.string title) => IO.println s!"  - {title}"
    | _ => pure ()

  -- Query: Find tasks assigned to Alice with assignee details
  let aliceTasks := DSL.query
    |>.findAll ["task", "title"]
    |>.where_ "task" ":task/title" "title"
    |>.whereRef "task" ":task/assignee" alice
    |>.run conn.current

  IO.println "\nAlice's tasks:"
  for row in aliceTasks.rows do
    match row[Var.ofName "title"]? with
    | some (Value.string title) => IO.println s!"  - {title}"
    | _ => pure ()

  -- Complete a task
  let completeTx := DSL.tx
    |>.retractKeyword task1 ":task/status" "in-progress"
    |>.addKeyword task1 ":task/status" "done"

  let .ok (conn, _) := completeTx.runOn conn | panic! "Failed to complete task"

  -- View task history
  IO.println "\nTask 1 status history:"
  let statusHistory := conn.attrHistory task1 (Attribute.mk ":task/status")
  for datom in statusHistory do
    let op := if datom.added then "set to" else "changed from"
    IO.println s!"  Tx {datom.tx.id}: {op} {datom.value}"
```

## Next Steps

- [Architecture](architecture.md) - Understand the underlying data model
- [Design Decisions](design-decisions.md) - Learn about cardinality and the Datomic comparison
- [API Reference](api-reference.md) - Complete API documentation
