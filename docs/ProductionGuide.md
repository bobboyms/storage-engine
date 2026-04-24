# Production Guide

Guia honesto pra usar esse storage engine em produção. Lê isso **antes** de colocar dados reais dentro.

---

## 1. Configuração mínima recomendada

```go
import (
    "github.com/bobboyms/storage-engine/pkg/crypto"
    "github.com/bobboyms/storage-engine/pkg/storage"
    "github.com/bobboyms/storage-engine/pkg/wal"
)

// 1. Chave mestra de criptografia — VEM DE FORA (KMS, env var, HSM).
masterKey := []byte(os.Getenv("DB_MASTER_KEY")) // 32 bytes

// 2. Keystore pra derivar DEKs por recurso
ks, err := crypto.NewKeyStore("./db/keys.json", masterKey)
heapCipher, _ := ks.GetOrCreateDEK("heap")
btreeCipher, _ := ks.GetOrCreateDEK("btree")
walCipher, _ := ks.GetOrCreateDEK("wal")

// 3. Cria heap e índices v2 (page-based, TDE uniforme)
hm, _ := storage.NewHeapForTable(storage.HeapFormatV2, "./db/data.heap", heapCipher)

tm := storage.NewEncryptedTableMenager(btreeCipher)
tm.NewTable("accounts", []storage.Index{
    {Name: "id", Primary: true, Type: storage.TypeInt},
}, 3, hm)

// 4. WAL com fsync estrito (default em DefaultOptions)
walOpts := wal.DefaultOptions() // SyncEveryWrite — cada Put fsync'd antes de retornar
walOpts.Cipher = walCipher
ww, _ := wal.NewWALWriter("./db/wal.log", walOpts)

// 5. Engine de PRODUÇÃO — exige WAL, faz auto-recovery no startup
se, err := storage.NewProductionStorageEngine(tm, ww)
if err != nil {
    log.Fatal(err) // trata erro de recovery com cuidado
}
defer se.Close()

// 6. Uso normal
se.Put("accounts", "id", types.IntKey(1), `{"id":1,"balance":1000}`)
```

Pontos críticos:

- **NewProductionStorageEngine** em vez de `NewStorageEngine`. Diferença: (a) exige WAL, (b) faz auto-recovery.
- **wal.DefaultOptions** — `SyncEveryWrite`. Se trocar por `PerformanceOptions`, você aceita janela de perda (documente no SLA do seu produto).
- **Heap v2 e BTree v2** — as únicas combinações recomendadas. O runtime do engine agora assume estado page-based + WAL; o caminho `.chk` legado ficou fora do recovery normal.
- **TDE completo exige três ciphers**: heap, índice e WAL. `NewEncryptedTableMenager` faz os índices automáticos herdarem o cipher de índice; se você passar `Index.Tree` manualmente, abra essa tree com `NewBTreeForIndex(..., btreeCipher)`.

---

## 2. Garantias de durability

| Cenário | Garantia |
|---|---|
| `Put()` retorna sem erro, depois `kill -9` | ✅ Dado persistido (WAL fsync) |
| `Put()` retorna sem erro, depois power loss | ✅ Dado persistido (WAL fsync + dir fsync) |
| Reopen após crash → `NewProductionStorageEngine` | ✅ Todos os `Put`s confirmados visíveis |
| Reopen após crash → `NewStorageEngine` **sem** `Recover` | ❌ Dados aparentemente perdidos (tree/heap em BufferPool sumiu) |
| Torn write no meio de uma página | ✅ Detectado via CRC32 do pagestore → `ErrChecksumMismatch` (ALERTAR, não silenciar) |
| Tampering de bytes cifrados | ✅ Detectado via AES-GCM auth tag → `ErrDecryptFailed` |

---

## 3. Observabilidade — O QUE MONITORAR

O engine NÃO expõe metrics hoje. Você precisa instrumentar na sua camada. Mínimo viável:

### Métricas críticas

| Métrica | Por quê |
|---|---|
| Taxa de `Put()` erros | Detecta WAL full, disk full, key conflicts |
| Latência p99 do `Put()` | Com SyncEveryWrite, latência = fsync. Picos = disk lento |
| Latência p99 do `Get()` | BufferPool cache miss implica disk read |
| BufferPool hit rate | Se < 80%, buffer pool pequeno pro workload |
| Tamanho do WAL ativo + segmentos | Deve ficar limitado por `MaxSegmentBytes`, checkpoint e `RetentionSegments` |
| Recovery duration on startup | Deve acompanhar o WAL desde o último checkpoint; monitore se crescer |

### Erros que DEVEM gerar alerta

- `ErrChecksumMismatch` no read — corrupção de disco
- `ErrDecryptFailed` no read — chave errada, tampering, ou corrupção
- Qualquer erro em `se.Close()` — fsync pode ter falhado, dados em risco
- Erro em `NewProductionStorageEngine` — recovery quebrou, **estado inconsistente**

---

## 4. Limitações conhecidas

### Recovery (Fase 6 — ARIES-lite)

**O que fazemos:**
- Replay idempotente do WAL (tree.Upsert / heap.Write com novo RecordID)
- Tree e heap convergem pra estado correto após replay
- Resultado é CORRETO pra uso normal
- `FuzzyCheckpoint()` grava registro de checkpoint, rotaciona o WAL e remove segmentos antigos já cobertos pelo checkpoint.

**O que NÃO fazemos:**
- **Undo phase** (rollback de transações não-commitadas). Hoje assumimos commit-por-Put. Se seu caso de uso envolve transações explícitas com rollback pós-crash, esse engine NÃO serve.
- **Page-level redo via pageLSN** — infra instalada (heap v2 escreve pageLSN), mas recovery não lê ainda (faz logical redo idempotente). Orfandos de heap são reciclados por `Vacuum`.

### Concorrência

- **BTree v2 fixed-key** usa latch crabbing top-down com split preventivo.
- **BTree v2 variable-key (varchar)** também usa latch crabbing top-down.
- **Heap v2 writers serializam** na página ativa (um writer por vez enche a mesma página).
- Readers paralelos funcionam bem em ambos.

### Tamanho

- **Entry do WAL**: sem limite técnico (cruza páginas)
- **Record do Heap v2**: ~8KB (não cabe mais que uma página). Docs maiores precisam de overflow pages (não implementado).
- **VarcharKey na B+ tree v2**: OK, layout de slots variáveis

### Operacional

- **Backup consistente**: `se.Close()` → copiar o diretório inteiro → reabrir. **NÃO** copie com engine rodando sem snapshot do FS.
- **WAL lifecycle**: `wal.DefaultOptions()` habilita rotação por tamanho (`MaxSegmentBytes`) e retenção segura (`RetentionSegments`). Configure `ArchiveDir` se quiser arquivar segmentos removidos; restaure com `wal.RestoreArchivedSegments`.
- **Key rotation da master key**: `keystore.RotateMasterKey` suportado sem reescrever dados (apenas re-wrap das DEKs)

---

## 5. Teste antes de produção

Mínimo:

```bash
# Unit tests + race detector
go test ./pkg/... -race

# Crash simulation
go test ./pkg/storage/ -race -run TestRecovery

# Stress (opcional, exige ajustes)
go test ./pkg/btree/v2/ -race -run Concurrent
```

### Recomendado antes de ir pra prod

- **Chaos testing**: kill -9 em loop enquanto workload roda. Verifica recovery em todas as condições.
- **Fault injection**: corrupção artificial de bytes em arquivos + reopen. Deve retornar erro claro (não silenciar).
- **Capacity planning**: meça latência p99 com seu workload real (não benchmarks sintéticos).

---

## 6. Roadmap pra maturar

| Feature | Prioridade | Esforço |
|---|---|---|
| Page-level redo com pageLSN no Recover | 🟡 média | 1 semana |
| Hardening do latch crabbing em variable-key | 🟢 baixa | 1-2 semanas |
| Backup/restore API | 🟢 baixa | 1 semana |
| Metrics/observabilidade built-in | 🟢 baixa | 1-2 semanas |
| Replication | ⚪ opcional | semanas-meses |

---

## 7. Quando NÃO usar esse engine

Seja honesto com seu cenário. **Não use se:**

- Você precisa de **transações multi-operação com rollback** (BEGIN/COMMIT/ROLLBACK com abort pós-crash).
- Workload de **altíssima concorrência de writes** com **VarcharKey** e distribuição muito heterogênea de tamanhos de chave. O path usa latch crabbing, mas ainda merece hardening adicional.
- Você precisa de **replicação nativa**.
- Volume de dados **> 100 GB**. Faltam otimizações de larga escala (free space map, compression, partitioning).
- Você é um **produto B2B hospedando dados críticos de terceiros**. Use PostgreSQL como backend e construa sua camada de features em cima.

Esse engine é **adequado** pra:
- Sistemas internos com volume modesto (< 10 GB por tabela)
- Protótipos que precisam de MVCC + TDE + durability básica
- Bases embedded (SQLite-like) em Go
- Projetos educacionais / research

---

## 8. Suporte e contato

Esse é um projeto educacional sendo trilhado pra uso real. Se for colocar em produção, considere:
- Code review de alguém com experiência em storage engines
- Jepsen-style testing contra seu workload específico
- Plano de rollback pra PostgreSQL se algo sair errado
