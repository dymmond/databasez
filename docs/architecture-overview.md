# Architecture Overview

Databasez is built around a small set of objects:

- `Database` for pool lifecycle and task-local connection routing.
- `Connection` for query execution and transaction orchestration.
- `Transaction` for context/decorator/manual transaction control.
- Backend classes (`DatabaseBackend`, `ConnectionBackend`, `TransactionBackend`) for dialect-specific behavior.

## Component relationships

```mermaid
flowchart TD
    A["Database"] --> B["Connection"]
    B --> C["Transaction"]
    A --> D["DatabaseBackend"]
    D --> E["ConnectionBackend"]
    E --> F["TransactionBackend"]
    D --> G["SQLAlchemy AsyncEngine"]
```

## Query lifecycle

```mermaid
sequenceDiagram
    participant App as Application code
    participant DB as Database
    participant Conn as Connection
    participant Backend as ConnectionBackend
    participant Engine as AsyncEngine

    App->>DB: fetch_all/execute/iterate
    DB->>DB: resolve task-local or global connection
    DB->>Conn: open context
    Conn->>Backend: acquire()
    Backend->>Engine: connect + execute
    Engine-->>Backend: result
    Backend-->>Conn: parsed rows/rowcount
    Conn-->>DB: result
    DB-->>App: return value
    Conn->>Backend: release()
```

## Multiloop model

Databasez is loop-aware. If the same `Database` is used from a different event loop, it can create a sub-database for that loop and proxy operations safely.

```mermaid
flowchart LR
    L1["Loop A"] --> DB["Database (root)"]
    L2["Loop B"] --> SUB["Sub-database (auto-created)"]
    DB --> SUB
    DB --> G["Global force_rollback connection"]
    SUB --> G
```

For deeper details:

- [Core Concepts](./core-concepts.md)
- [Connections & Transactions](./connections-and-transactions.md)
- [Extra drivers and overwrites](./extra-drivers-and-overwrites.md)
