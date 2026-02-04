# Transformando Storage Engine em Banco de Dados Relacional

## 🎯 O Que Você Já Tem

Seu storage engine atual possui:

- ✅ **Storage Layer**: B+Tree + Heap + WAL
- ✅ **Concurrency**: MVCC com Snapshot Isolation
- ✅ **Transactions**: ACID com Commit/Rollback
- ✅ **Durability**: Checkpoint + Recovery
- ✅ **Indexing**: Primary + Secondary indices

**Isso representa ~40% de um DBMS relacional!**

---

## 🏗️ Componentes Necessários

### 1. **SQL Parser & Lexer**

**O que faz**: Transforma SQL em árvore sintática (AST)

```sql
SELECT name, price 
FROM products 
WHERE price > 100 
ORDER BY name LIMIT 10
```

↓ Parser ↓

```go
type SelectStmt struct {
    Columns   []string          // ["name", "price"]
    Table     string            // "products"
    Where     *FilterExpr       // price > 100
    OrderBy   []OrderByExpr     // name ASC
    Limit     int              // 10
}
```

**Ferramentas**:
- `yacc`/`goyacc` (geradores de parser)
- `lex`/`golex` (lexer)
- Ou bibliotecas: `vitess` (parser SQL do YouTube)

**Implementação**:
```
pkg/sql/
  ├── lexer.go       # Tokenização
  ├── parser.go      # Grammar SQL
  ├── ast.go         # Árvore sintática
  └── validator.go   # Validação semântica
```

---

### 2. **Query Planner (Otimizador)**

**O que faz**: Converte AST em plano de execução otimizado

```
SQL: SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.country = 'BR'

Plano Ruim (O(n²)):
NestedLoopJoin
  ├── SeqScan(orders)
  └── SeqScan(customers WHERE country='BR')

Plano Otimizado (O(n log n)):
HashJoin
  ├── SeqScan(customers WHERE country='BR')  -- Filtra primeiro!
  └── IndexScan(orders, idx_customer_id)     -- Usa índice!
```

**Componentes**:
- **Cost Model**: Estima custo de operações (scans, joins)
- **Rule-Based Optimizer**: Push-down de filtros, index selection
- **Statistics**: Histogramas, cardinality estimation

**Implementação**:
```
pkg/planner/
  ├── planner.go     # Geração de planos
  ├── optimizer.go   # Otimização de planos
  ├── cost.go        # Modelo de custo
  └── stats.go       # Estatísticas de tabelas
```

---

### 3. **Query Executor**

**O que faz**: Executa o plano de consulta

**Operadores necessários**:

#### a) **Scan Operators**
- `SeqScan`: Varredura sequencial da tabela
- `IndexScan`: Busca via B+Tree (você já tem!)
- `BitmapScan`: Combina múltiplos índices

#### b) **Join Operators**
- `NestedLoopJoin`: Loop sobre tabelas (simples, O(n²))
- `HashJoin`: Hash table (rápido, O(n+m))
- `MergeJoin`: Merge de tabelas ordenadas (O(n+m))

#### c) **Aggregate Operators**
- `GroupBy`: Agrupamento com hash table
- `Aggregate`: SUM, COUNT, AVG, MIN, MAX
- `Distinct`: Remoção de duplicatas

#### d) **Sort & Limit**
- `Sort`: Ordenação (external merge sort)
- `Limit`: Limita resultados

**Implementação**:
```
pkg/executor/
  ├── executor.go        # Interface executor
  ├── scan.go           # SeqScan, IndexScan
  ├── join.go           # NestedLoop, Hash, Merge
  ├── aggregate.go      # GroupBy, SUM, COUNT
  ├── sort.go           # External merge sort
  └── projection.go     # SELECT columns
```

**Exemplo de Executor**:
```go
type Executor interface {
    Open() error
    Next() (Row, error)  // Iterator pattern
    Close() error
}

type HashJoinExecutor struct {
    left, right Executor
    hashTable   map[Comparable][]Row
    condition   JoinCondition
}

func (hj *HashJoinExecutor) Next() (Row, error) {
    // Build phase: hash da tabela menor
    // Probe phase: busca na hash table
}
```

---

### 4. **Schema Management (DDL)**

**O que faz**: Gerencia definições de tabelas

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    age INT CHECK (age >= 18),
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (company_id) REFERENCES companies(id)
);
```

**Componentes**:
- **Catalog/System Tables**: Metadados persistidos
  - `pg_tables`: Lista de tabelas
  - `pg_columns`: Colunas e tipos
  - `pg_indexes`: Índices
  - `pg_constraints`: Constraints

- **Schema Validator**: Valida constraints
  - Primary Key: Unicidade
  - Foreign Key: Integridade referencial
  - Check: Validações customizadas
  - Not Null: Obrigatoriedade

**Implementação**:
```
pkg/catalog/
  ├── catalog.go       # System catalog
  ├── schema.go        # Table definitions
  ├── constraints.go   # PK, FK, CHECK
  └── types.go         # Data types
```

**Exemplo**:
```go
type TableSchema struct {
    Name        string
    Columns     []ColumnDef
    Constraints []Constraint
}

type ColumnDef struct {
    Name     string
    Type     DataType
    Nullable bool
    Default  interface{}
}

type Constraint struct {
    Type ConstraintType  // PK, FK, UNIQUE, CHECK
    Columns []string
    RefTable string      // Para FK
}
```

---

### 5. **JOIN Implementation**

**Algoritmos principais**:

#### **Hash Join** (mais comum)
```go
func HashJoin(left, right Executor, condition JoinCondition) []Row {
    // Build phase
    hashTable := make(map[interface{}][]Row)
    for {
        row, err := left.Next()
        if err == io.EOF { break }
        key := extractKey(row, condition.LeftColumn)
        hashTable[key] = append(hashTable[key], row)
    }
    
    // Probe phase
    results := []Row{}
    for {
        row, err := right.Next()
        if err == io.EOF { break }
        key := extractKey(row, condition.RightColumn)
        if matches, ok := hashTable[key]; ok {
            for _, match := range matches {
                results = append(results, merge(match, row))
            }
        }
    }
    return results
}
```

#### **Nested Loop Join** (simples)
```go
func NestedLoopJoin(left, right Executor) []Row {
    results := []Row{}
    for {
        leftRow, _ := left.Next()
        for {
            rightRow, _ := right.Next()
            if matches(leftRow, rightRow) {
                results = append(results, merge(leftRow, rightRow))
            }
        }
    }
    return results
}
```

---

### 6. **Network Layer (Server)**

**O que faz**: Aceita conexões de clientes via TCP

**Componentes**:
- **Protocol Handler**: PostgreSQL Wire Protocol ou MySQL Protocol
- **Session Manager**: Uma sessão por conexão
- **Connection Pool**: Reutilização de conexões

**Implementação**:
```
pkg/server/
  ├── server.go       # TCP listener
  ├── session.go      # Session state
  ├── protocol.go     # Wire protocol
  └── auth.go         # Authentication
```

**Exemplo**:
```go
type Server struct {
    listener net.Listener
    engine   *storage.StorageEngine
}

func (s *Server) Start(port int) {
    ln, _ := net.Listen("tcp", fmt.Sprintf(":%d", port))
    for {
        conn, _ := ln.Accept()
        go s.handleConnection(conn)
    }
}

func (s *Server) handleConnection(conn net.Conn) {
    session := NewSession(conn, s.engine)
    for {
        query := session.ReadQuery()
        result := s.executeQuery(query)
        session.WriteResult(result)
    }
}
```

---

### 7. **Authentication & Authorization**

**O que faz**: Controle de acesso

```sql
CREATE USER alice WITH PASSWORD 'secret';
GRANT SELECT, INSERT ON products TO alice;
REVOKE DELETE ON orders FROM alice;
```

**Componentes**:
- **User Management**: Criar/deletar usuários
- **Permission System**: Tabela de ACL (Access Control List)
- **Role-Based Access**: Grupos de permissões

**Implementação**:
```
pkg/auth/
  ├── auth.go         # Authentication
  ├── users.go        # User management
  └── acl.go          # Access control
```

---

### 8. **Advanced Features**

#### **Views**
```sql
CREATE VIEW expensive_products AS
SELECT * FROM products WHERE price > 1000;
```

#### **Triggers**
```sql
CREATE TRIGGER update_timestamp
BEFORE UPDATE ON users
FOR EACH ROW
SET NEW.updated_at = NOW();
```

#### **Stored Procedures**
```sql
CREATE PROCEDURE update_stock(product_id INT, qty INT)
BEGIN
    UPDATE products SET stock = stock - qty WHERE id = product_id;
END;
```

---

## 🗺️ Arquitetura Final

```
┌──────────────────────────────────────────────────────────┐
│                     CLIENT (psql, app)                   │
└────────────────────┬─────────────────────────────────────┘
                     │ TCP/PostgreSQL Protocol
┌────────────────────▼─────────────────────────────────────┐
│                   NETWORK LAYER                          │
│  • Connection Handler  • Session Manager  • Auth         │
└────────────────────┬─────────────────────────────────────┘
                     │
┌────────────────────▼─────────────────────────────────────┐
│                   SQL LAYER                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │  Parser  │→ │ Planner  │→ │ Executor │              │
│  └──────────┘  └──────────┘  └──────────┘              │
│     Lexer       Optimizer     Operators                  │
└────────────────────┬─────────────────────────────────────┘
                     │
┌────────────────────▼─────────────────────────────────────┐
│                TRANSACTION LAYER                         │
│  • Transaction Manager  • MVCC  • Lock Manager           │
└────────────────────┬─────────────────────────────────────┘
                     │
┌────────────────────▼─────────────────────────────────────┐
│               STORAGE ENGINE (Você já tem!)              │
│  • B+Tree  • Heap  • WAL  • Checkpoint                   │
└──────────────────────────────────────────────────────────┘
```

---

## 📋 Roadmap de Implementação

### **Phase 1: Foundation (2-3 meses)**
- [ ] SQL Parser básico (SELECT, INSERT, UPDATE, DELETE)
- [ ] Query Executor simples (SeqScan, IndexScan)
- [ ] Schema management (CREATE TABLE, DROP TABLE)

### **Phase 2: Joins & Aggregation (2-3 meses)**
- [ ] Hash Join / Nested Loop Join
- [ ] GROUP BY, SUM, COUNT, AVG
- [ ] ORDER BY, LIMIT, OFFSET

### **Phase 3: Optimizer (1-2 meses)**
- [ ] Cost model básico
- [ ] Index selection
- [ ] Join order optimization

### **Phase 4: Network (1 mês)**
- [ ] TCP server
- [ ] PostgreSQL wire protocol
- [ ] Session management

### **Phase 5: Advanced (3+ meses)**
- [ ] Foreign Keys
- [ ] Views
- [ ] Triggers
- [ ] Stored Procedures

---

## 🛠️ Ferramentas & Referências

### **Projetos Open Source para Estudar**:
1. **SQLite** - Código limpo, ótimo para aprender
2. **DuckDB** - OLAP moderno em C++
3. **TinySQL** (PingCAP) - Educational SQL database em Go
4. **CockroachDB** - Distributed SQL em Go

### **Livros Essenciais**:
- *Database Internals* - Alex Petrov
- *Database System Concepts* - Silberschatz
- *Architecture of a Database System* - Hellerstein

### **Cursos**:
- CMU 15-445 (Database Systems) - Andy Pavlo
- Stanford CS346 (Database System Implementation)

---

## 💡 Decisões de Design

### **1. Protocol: PostgreSQL ou Custom?**
- ✅ **PostgreSQL**: Compatibilidade com clientes existentes (psql, pgAdmin)
- ❌ **Custom**: Mais simples, mas precisa cliente próprio

### **2. SQL Dialect: Qual seguir?**
- PostgreSQL: Mais features
- MySQL: Mais popular
- SQLite: Mais simples

### **3. Executor: Volcano Model ou Push-Based?**
- **Volcano (Iterator)**: Simples, fácil de debugar
- **Push-Based**: Mais rápido, mais complexo

---

## 🎯 MVP (Minimum Viable Product)

Para um **MVP funcional**, implemente:

1. ✅ SQL Parser (SELECT, INSERT, WHERE)
2. ✅ Simple Executor (SeqScan, IndexScan, Filter)
3. ✅ No Joins (apenas single-table queries)
4. ✅ No Aggregations
5. ✅ TCP Server básico

**Exemplo de query suportada**:
```sql
CREATE TABLE users (id INT, name VARCHAR);
INSERT INTO users VALUES (1, 'Alice');
SELECT * FROM users WHERE id = 1;
```

Isso já seria um **banco de dados relacional funcional**!

---

## 📊 Estimativa de Esforço

| Componente | Complexidade | Tempo Estimado |
|------------|--------------|----------------|
| SQL Parser | ⭐⭐⭐ | 2-3 semanas |
| Query Executor | ⭐⭐⭐⭐ | 1-2 meses |
| Join Operators | ⭐⭐⭐⭐ | 3-4 semanas |
| Query Optimizer | ⭐⭐⭐⭐⭐ | 2-3 meses |
| Network Layer | ⭐⭐ | 2-3 semanas |
| **TOTAL** | | **6-9 meses** |

---

## 🚀 Next Steps

1. Decida o **SQL dialect** (recomendo PostgreSQL subset)
2. Implemente **parser básico** com `goyacc`
3. Crie **executor simples** (SeqScan + Filter)
4. Adicione **TCP server** para testes
5. Expanda gradualmente com JOINs e agregações
