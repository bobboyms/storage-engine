# Plano de Migração: Arquitetura Page-Based

> **Status:** proposta / not iniciado
> **Escopo:** refatoração estrutural do storage engine
> **Duração estimada:** 4-8 semanas de trabalho concentrado (referência, not promessa)
> **Tamanho do blast radius:** ~3.000 linhas tocadas de um total de ~5.200 LOC (fora testes)

---

## 1. Por que este documento existe

Hoje o engine armazena data em **records de tamanho variável** gravados em append-only nos files `.data` do heap. Índices B+ tree vivem **100% em memória** e são serializados em checkpoints (`.chk`).

Bancos de produção (PostgreSQL, Oracle, InnoDB, SQL Server) usam um modelo diferente: **pages de tamanho fixo** (tipicamente 4KB, 8KB ou 16KB) como unidade fundamental de I/O, cache e encryptiongem.

Migrar pra esse modelo desbloqueia:

- **Criptografia uniforme** — um único ponto encryption TUDO que vai pro disco (heap, index, WAL, checkpoints). Fecha o buraco discutido em [encryption_at_rest](./encryption_at_rest.md) (se criado).
- **Buffer pool** — cache de pages quentes em RAM, com LRU e dirty tracking. Hoje not existe cache; cada `Read` faz `seek+read` no disco.
- **Índices persistentes sem re-serializar tudo** — hoje um checkpoint reescreve a tree inteira. Page-based atualiza apenas pages sujas.
- **Recovery tipo ARIES** — redo/undo por page permite recovery correto sob failures parciais.
- **Concorrência mais fina** — latches por page em vez de mutex por manager.

**Trade-off honesto:** é um refactor grande. Este documento existe pra você decidir **se** (e **quando**) vale o esforço. Not é pra executar cegamente.

---

## 2. Estado atual (baseline)

| Componente | Arquivo | LOC | Como persiste hoje |
|---|---|---|---|
| Heap | `pkg/heap/heap.go` | 683 | Segments append-only, records de tamanho variável |
| B+ Tree | `pkg/btree/btree.go` + `node.go` | 743 | **Somente em memória** |
| WAL | `pkg/wal/*.go` | ~490 | Log append-only, entries de tamanho variável |
| Checkpoint | `pkg/storage/checkpoint*.go` | ~410 | Serializa tree inteira em `.chk` |
| Storage engine | `pkg/storage/engine.go` + outros | ~2300 | Orquestra os anteriores |

**Observação crítica:** o engine tem ~350 chamadas aos construtores `NewHeapManager` / `NewWALWriter` / `NewStorageEngine` espalhadas em 30+ files (medido anteriormente). Qualquer mudança de API tem custo de cascata.

---

## 3. Estado-alvo (resumo)

```
┌─────────────────────────────────────────────────────────┐
│                     Storage Engine                       │
│                                                          │
│   ┌──────────┐   ┌──────────┐   ┌──────────────────┐   │
│   │   Heap   │   │  B+Tree  │   │   WAL / Checkpt  │   │
│   │ Access   │   │  Access  │   │  (page-based)    │   │
│   └────┬─────┘   └─────┬────┘   └────────┬─────────┘   │
│        │               │                  │             │
│        ▼               ▼                  ▼             │
│   ┌─────────────────────────────────────────────┐      │
│   │            Buffer Pool (LRU)                │      │
│   │     fetchPage / markDirty / flush           │      │
│   └──────────────────┬──────────────────────────┘      │
│                      │                                  │
│                      ▼                                  │
│   ┌─────────────────────────────────────────────┐      │
│   │     PageFile + PageCipher (TDE)             │      │
│   │   ReadPage(id) / WritePage(id, bytes)       │      │
│   └─────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────┘
```

**Princípios invaribefore ao longo do refactor:**

1. **Testes passam no fim de cada fase.** Se uma fase quebra testes, ela not terminou.
2. **API pública de `storage.Engine` not muda.** Quem chama `engine.InsertRow` not sente diferença.
3. **Opt-in por tabela.** Tabelas existentes continuam no formato antigo até serem explicitamente migradas.
4. **Reversível.** Até a Fase 7, dá pra abortar e voltar ao estado anterior sem perder data.

---

## 4. Decisões de design a tomar ANTES da Fase 1

Estas decisões precisam ser fechadas before de começar a codar. Se mudarem no meio, retrabalho significativo.

### 4.1 Tamanho de page

| Tamanho | Prós | Contras | Quem usa |
|---|---|---|---|
| 4KB | Casa com o setor do SSD; menos amplificação de write | Muitos records pequenos em tree grande | SQL Server |
| **8KB** | Default do PostgreSQL; bom equilíbrio | — | PostgreSQL, Oracle |
| 16KB | Menos overhead de header por record | Mais amplificação em updates pequenos | InnoDB |

**Sugestão:** 8KB. Precedente forte, literatura farta, casa bem com reads de 4KB do SSD (2 pages contíguas por I/O ou read-ahead).

### 4.2 Formato da page (slotted page)

Formato clássico:

```
┌─────────────────────────────────────────────────────┐
│ PageHeader (32 bytes)                                │
│  magic | version | pageType | flags | pageLSN |      │
│  checksum | freeSpacePointer | slotCount | ...       │
├─────────────────────────────────────────────────────┤
│ Slot 0 → offset, length                              │
│ Slot 1 → offset, length                              │
│ Slot 2 → offset, length      (cresce pra baixo)      │
│   ...                                                │
├─────────────────────────────────────────────────────┤
│                                                      │
│        (espaço livre entre slots e data)            │
│                                                      │
├─────────────────────────────────────────────────────┤
│                 Record 2 bytes                       │
│             Record 1 bytes                           │
│         Record 0 bytes    (cresce pra cima)          │
└─────────────────────────────────────────────────────┘
```

Esse layout permite: update in-place sem invalidar ponteiros externos (slot IDs são estáveis), compactação quando fragmentado.

### 4.3 Identificação de page (PageID)

`PageID = (fileID uint32, pageNum uint32) = uint64`

- `fileID` permite múltiplos files (heap, index, wal) sem colisão.
- Mapa `fileID → *os.File` fica no PageFile manager.

### 4.4 Estratégia de criptografia por page

- **AES-256-GCM**, uma DEK por file lógico.
- **Nonce = pageID || pageLSN** (24 bytes, mas GCM usa 12 — truncar com HKDF, ou usar AES-GCM-SIV que aceita nonce reusado).
- **Alternativa mais simples:** AES-XTS (sem expansão de tamanho, mas sem autenticação — confia no CRC do header).
- **AAD = PageID em little-endian**, impede swap de pages.

**Decisão:** AES-GCM-SIV se disponível em Go (biblioteca `crypto/cipher` + package externo), senot AES-GCM com nonce aleatório armazenado no header (tomamos 12 bytes dos 32 do header).

---

## 5. Fases

Cada fase tem:

- **Objetivo** (uma frase)
- **Arquivos tocados**
- **Entrega** (o que deve existir quando terminar)
- **Critério de "pronto"** (teste concreto que deve passar)
- **Riscos**
- **Rollback**

### Fase 0 — Research spike (3-5 dias)

**Objetivo:** validar decisões de §4 implementando um protótipo descartável.

**Escopo:**

- Diretório `experiments/pagestore/` fora de `pkg/` (para not ser compilado pelo resto).
- Implementar um PageFile minimalista: abrir file, ler/escrever page 8KB, com AES-GCM.
- Medir: latência de read/write de uma page, overhead de encryptiongem com AES-NI.
- Escrever **ADR** (Architectural Decision Record) documentando escolhas.

**Entrega:**

- `experiments/pagestore/prototype.go` (pode ser deletado after).
- `docs/adr/001-page-format.md` com tamanho, layout, criptografia.

**Critério:** benchmark rodando, números na mão.

**Rollback:** apagar `experiments/`.

---

### Fase 1 — Primitiva de page + PageFile + PageCipher (1 semana)

**Objetivo:** criar a camada mais baixa — `PageFile` sabe ler/escrever pages de 8KB com checksum e encryptiongem opcional.

**Novo pacote:** `pkg/pagestore/`

```
pkg/pagestore/
├── page.go          # type Page [8192]byte + PageHeader
├── pagefile.go      # PageFile: Allocate, ReadPage, WritePage
├── cipher.go        # PageCipher: encryption/deencryption uma page inteira
└── pagestore_test.go
```

**Arquivos tocados:** apenas novos. **Zero mudanças no engine existente.**

**Entrega:**

```go
type PageFile struct { /* ... */ }
func NewPageFile(path string, cipher crypto.Cipher) (*PageFile, errorr)
func (pf *PageFile) AllocatePage() (PageID, errorr)
func (pf *PageFile) ReadPage(id PageID) (*Page, errorr)
func (pf *PageFile) WritePage(id PageID, p *Page) errorr
func (pf *PageFile) Sync() errorr
```

**Critério de pronto:**

- Tests: round-trip de 1000 pages, validação de checksum, detecção de tamper.
- `go test ./pkg/pagestore -race`.
- Todos os testes existentes continuam passando.

**Riscos:**

- **Zero pra o resto do engine.** Este é o menor risco do plano.

**Rollback:** deletar `pkg/pagestore/`.

---

### Fase 2 — Buffer pool (1 semana)

**Objetivo:** cache de pages em RAM com política LRU e dirty tracking.

**Arquivos novos:** `pkg/pagestore/bufferpool.go`

**Entrega:**

```go
type BufferPool struct { /* ... */ }
func NewBufferPool(pf *PageFile, capacity int) *BufferPool

// Fetch traz a page pro pool. Retorna handle com RLock/RUnlock.
func (bp *BufferPool) Fetch(id PageID) (*PageHandle, errorr)

// MarkDirty marca a page suja. Flush futuro a persiste.
func (bp *BufferPool) MarkDirty(id PageID)

// FlushAll grava todas as pages sujas no disco.
func (bp *BufferPool) FlushAll() errorr
```

**Considerações de concurrency:**

- Latches por page (not mutex global).
- Pinning: pages em uso not podem ser evictadas.

**Critério de pronto:**

- Teste com N pages > capacity, verifica que LRU eviga as menos usadas.
- Teste de concurrency com 100 goroutines lendo/escrevendo.
- `-race` limpo.

**Riscos:**

- Deadlock em escalation de latches.
- Write amplification se flush for muito agressivo.

**Rollback:** deletar o file.

---

### Fase 3 — Heap page-based (2 semanas) ⚠️ maior risco

**Objetivo:** reimplementar `HeapManager` em cima do BufferPool.

**Arquivos tocados:**

- `pkg/heap/heap.go` — reescrever mantendo a mesma API pública
- `pkg/heap/slotted_page.go` (novo) — layout slotted
- **Todos os testes do heap** continuam passando

**Detalhes:**

- Formato slotted dentro da page.
- Record ID = `(PageID, SlotID)` — substitui o offset global atual.
- **Compatibilidade de API:** `Write(doc, lsn, prev)` continua retornando `int64`, mas agora o int64 codifica `(pageID<<16) | slotID`. Callers not sabem disso.
- Iterator itera pages → slots → records.

**Migração de data:**

- Novo campo `HeapFormat` em metadata: `v1_legacy` ou `v2_paged`.
- Tabelas antigas continuam em `v1_legacy`.
- Tabelas novas (`CreateTable` after da Fase 3) usam `v2_paged`.
- **Nenhuma migração automática.** Opcional: `pkg/migration/heap_v1_to_v2.go` pra quem quiser.

**Critério de pronto:**

- **Toda a suíte de `pkg/heap` e `pkg/storage` passa** sem mudanças nos testes existentes.
- Novo teste: `TestHeapPaged_*` exercita v2 explicitamente.
- Benchmark: throughput de insert not pior que 50% do v1 (aceitável pelo cache).

**Riscos:**

- ⚠️ **Alta chance de regressão.** MVCC depende de offsets estáveis pós-update — preciso garantir isso no formato slotted.
- ⚠️ Vacuum precisa entender o novo formato.
- ⚠️ O iterator precisa ser determinístico na mesma ordem.

**Rollback:** `git revert` da fase. Dados em `v1_legacy` not são afetados.

---

### Fase 4 — WAL page-based (1 semana)

**Objetivo:** mudar o WAL de "log de entradas lógicas" para "log de mudanças físicas em pages" (físico vs lógico).

**Alternativa mais barata:** manter o WAL lógico (como está), mas armazená-lo em formato de pages (apenas a mudança de formato, not de semântica). É o caminho recomendado — migrar pra WAL físico é upgrade separado.

**Arquivos tocados:** `pkg/wal/writer.go`, `pkg/wal/reader.go`, `pkg/wal/entry.go`.

**Critério:** toda a suíte de `pkg/wal` e todos os testes de durability passam.

**Riscos:**

- Recovery é sensível a WAL format. Validar em testes de crash (simulados).

**Rollback:** `git revert`.

---

### Fase 5 — B+ tree persistido page-based (2 semanas) ⚠️ maior risco

**Objetivo:** tirar a B+ tree da memória. Cada nó é uma page.

**Arquivos tocados:**

- `pkg/btree/btree.go` (307 LOC)
- `pkg/btree/node.go` (436 LOC)
- `pkg/storage/checkpoint*.go` — **torna-se obsoleto** (not deleta ainda, só marca)

**Mudanças semânticas:**

- `Node` not tem mais ponteiro `*Node` filho; tem `PageID`.
- Operações de insert/delete carregam pages via BufferPool, aplicam mutação, marcam suja.
- Checkpoint vira trivialmente um `bp.FlushAll()`.

**Critério de pronto:**

- Toda suíte de `pkg/btree` e `pkg/storage` passa.
- Benchmark: operação de insert na tree not pior que 70% do in-memory (cache absorve).
- Teste de recovery: kill -9 during insert, restart, data consistentes.

**Riscos:**

- ⚠️⚠️ **Maior risco do plano.** B+ tree concorrente com pages é notoriamente difícil (latch crabbing, splits). O que você tem hoje com mutex RWLock é simples; a versão paginada precisa de latch protocol correto.
- **Alternativa se travar:** manter tree em memória, mas persistir como stream de pages no checkpoint (refactor menor, perde um pouco do ganho).

**Rollback:** `git revert` da fase inteira.

---

### Fase 6 — Recovery ARIES-lite (1-2 semanas)

**Objetivo:** implementar recovery correto para o mundo page-based.

**Componentes:**

- **Analysis:** scan do WAL pra montar Active Transaction Table + Dirty Page Table.
- **Redo:** reaplicar mudanças de WAL nas pages desde o último checkpoint.
- **Undo:** desfazer transações not-commitadas.

**Arquivos tocados:** `pkg/storage/engine.go` (recovery path), testes de durability.

**Critério:**

- Crash simulado em 100 pontos distintos → 100 recoveries corretas.
- Teste existente `durability_test.go` passa + novos testes page-level.

**Riscos:**

- Recovery é um dos tópicos mais sutis de storage engines. Literatura recomendada: papers originais de ARIES (Mohan et al.).

**Rollback:** possível, mas painful — recovery afeta decisões de formato de WAL.

---

### Fase 7 — Deprecação do formato v1 e limpeza (3 dias)

**Objetivo:** remover código legado.

**Arquivos tocados:**

- Deletar caminho v1 do heap.
- Deletar `pkg/storage/checkpoint*.go` (substituído por flush do buffer pool).
- Atualizar examples em `examples/*/main.go`.

**Pré-requisitos:** decisão consciente de abandonar data v1 OU migrador pronto.

---

### Fase 8 (bônus) — Free Space Map, vacuum compactante, checkpoint fuzzy

Melhorias after do core funcionar:

- **FSM:** map de pages com espaço livre pra evitar scan linear em inserts.
- **Vacuum compactante:** reescreve pages fragmentadas em in-place.
- **Fuzzy checkpoint:** checkpoint not bloqueia writes (como PostgreSQL).

---

## 6. Riscos globais

| Risco | Probabilidade | Impacto | Mitigação |
|---|---|---|---|
| Regressão em MVCC | Alta | Alto | Pinar testes de mvcc/vacuum, correr `-race` constantemente |
| Recovery quebrado | Média | Catastrófico | Fase 6 inteira dedicada; crash simulation |
| Perda de performance | Alta | Médio | Buffer pool deve recuperar; medir por fase |
| Deadlock em latches | Média | Alto | Hierarquia fixa de latches; `-race` |
| Refactor encalhar (cansaço) | Alta | Alto | Entregas pequenas, mergeáveis, reversíveis |

---

## 7. Quando parar (plano B)

Este plano é **opcional**. Se em qualquer ponto o custo not estiver se pagando:

- **Parar na Fase 2:** você ganha buffer pool (pra heap existente) e a primitiva de page. Suficiente pra prototipar TDE uniforme.
- **Parar na Fase 3:** heap page-based com cache. Cobre 70% do valor prático.
- **Not fazer a Fase 5:** B+ tree em memória continua funcionando. Perde persistência incremental de index mas mantém vida.

**Critérios de "abortar":**

- Uma fase tomou 2x o tempo estimado → reavaliar se vale.
- Perdendo mais tempo consertando regressões do que avançando.
- Descobrindo que uma decisão de §4 estava errada — recuar pro último ponto estável e rever.

---

## 8. Ordem de read recomendada before de começar

1. PostgreSQL docs: *Storage > Disk Page Layout* e *Database Page Layout*.
2. CMU Database Group (prof. Andy Pavlo), aulas *Database Storage I & II*.
3. Paper **ARIES** (Mohan et al., 1992) — só para Fase 6.
4. Livro *Database Internals* (Alex Petrov) — capítulos 1-4.

---

## 9. Checklist de arranque

Antes de começar a Fase 1:

- [ ] Fase 0 concluída (protótipo + ADR).
- [ ] Decisão de tamanho de page registrada.
- [ ] Decisão de encryptiongem (XTS vs GCM vs GCM-SIV) registrada.
- [ ] CI configurada com `go test -race` em todas as branches.
- [ ] Branch `feature/page-based` criada do `main`.
- [ ] Time ciente de que PRs ficarão ativos por semanas.

---

## 10. Escopo fora deste plano

Coisas que são evoluções naturais mas **not** fazem parte:

- Index secundário com outros tipos de tree (LSM, skip list).
- Replicação / read em réplicas.
- Compressão de pages.
- Direct I/O (`O_DIRECT`) pra contornar page cache do SO.
- TOAST (payloads gigbefore separados da page).

Cada um é um projeto por si só.
