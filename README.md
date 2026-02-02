# Storage Engine B+ Tree

A professional-grade storage engine implementation using B+ Trees, supporting multiple data types and generic scan operators.

## 🏗️ Project Structure
- `cmd/`: Application entry points.
- `pkg/btree/`: B+ Tree core implementation.
- `pkg/storage/`: Storage engine layer, table management, and cursor.
- `pkg/types/`: Comparable type system (Int, Varchar, Float, Bool, Date).
- `pkg/query/`: Generic scan operators and conditions.
- `pkg/errors/`: Custom error definitions.
- `examples/`: Code examples of how to use the engine.
- `docs/`: Tutorials and architecture documentation.

## 🚀 Features
- **Generic B+ Tree**: High-performance indexing structure.
- **Multi-Type Support**: Built-in support for multiple SQL types.
- **Rich Queries**: Support for `=`, `!=`, `>`, `<`, `>=`, `<=`, and `BETWEEN`.
- **Unique Constraints**: Enforcement of unique keys for primary indexes.
- **Professional Layout**: Optimized for scalability and maintainability.

## 🛠️ Getting Started

### Prerequisites
- Go 1.25.6 or higher.

### Installation
```bash
go mod download
```

### Build & Run
```bash
make build
make run
```

### Testing
```bash
make test
```

## 📄 Documentation
See the `docs/` folder for more detailed information.
# storage-engine
