# ADR 001 — Formato de Página e Estratégia de Cifragem

- **Status:** proposto (validado em protótipo, não implementado em produção)
- **Data:** 2026-04-22
- **Contexto:** Fase 0 do [plano de migração page-based](../page_based_migration_plan.md)
- **Protótipo:** [`experiments/pagestore/`](../../experiments/pagestore/)

---

## 1. Contexto

Para migrar o storage engine para arquitetura page-based (plano em `docs/page_based_migration_plan.md`) precisamos fechar três decisões antes de escrever qualquer linha de produção:

1. **Tamanho de página** — afeta throughput, amplificação de write e layout do slot directory.
2. **Formato do header e campos obrigatórios** — uma vez escolhido, alterar é quebra de formato on-disk.
3. **Estratégia de cifragem** — afeta autenticação, espaço usável e overhead de CPU.

Esta ADR documenta as escolhas e os números que as sustentam.

---

## 2. Decisão

| Aspecto | Decisão |
|---|---|
| Tamanho da página | **8192 bytes (8KB)** |
| Header | **32 bytes fixos**, sempre em claro |
| Cifra | **AES-256-GCM** (nonce aleatório em disco) |
| AAD | **PageID em little-endian (8 bytes)** |
| Checksum | **CRC32-Castagnoli** sobre o body em disco (ciphertext quando houver cifra) |
| Body usável | 8160 bytes sem cifra · 8132 bytes com cifra |

### 2.1 Layout on-disk

```
Offset  Bytes  Campo
──────  ─────  ───────────────────────────────────
  0     4      Magic       ("PAGE" = 0x50414745)
  4     2      Version     (1)
  6     1      PageType    (Heap | BTreeLeaf | ...)
  7     1      Flags
  8     8      PageID
 16     8      PageLSN
 24     4      Checksum    (CRC32 do body em disco)
 28     4      Reserved
──────  ─────  ────── fim do header (32 bytes) ───
 32     8160   Body
        └── Sem cifra:   8160 bytes plaintext
        └── Com cifra:   nonce(12) || ciphertext(8132) || tag(16)
```

### 2.2 Criptografia

- Apenas o **body** é cifrado. O header é escrito em claro para permitir que recovery e diagnóstico leiam `PageID`, `PageLSN`, `Type` sem precisar da chave.
- O nonce é gerado aleatoriamente a cada write e **armazenado nos primeiros 12 bytes do body em disco**. 2^96 nonces fornece margem de colisão desprezível para qualquer workload prático.
- AAD = `PageID` em little-endian. Amarra o ciphertext ao seu pageID; um atacante que faça swap de bodies entre páginas falha na autenticação da GCM.
- O checksum é calculado sobre o **conteúdo em disco** (ciphertext no caso cifrado). Permite *fast fail* em corrupção antes de tentar decifrar.

### 2.3 Decisões rejeitadas explicitamente

| Opção rejeitada | Por quê |
|---|---|
| **Página de 4KB** | Overhead de header/slot-dir dobra relativamente; pior fator de preenchimento da B+ tree. |
| **Página de 16KB** | Amplificação de write em updates pequenos (ler+cifrar+escrever 16KB pra mudar 50 bytes). |
| **AES-XTS** | Não oferece autenticação — swap de setores ou tamper silencioso passam despercebidos. Poderíamos compor com HMAC, mas GCM já resolve. |
| **AES-GCM-SIV** | Requer dependência externa; GCM com nonce aleatório é suficiente (2^96 ≫ páginas/segundo × anos). |
| **Cifrar header também** | Quebra leitura de metadados em recovery e dificulta diagnóstico. Padrão em PostgreSQL/Oracle TDE é manter o header em claro. |
| **Checksum sobre plaintext** | Forçaria decifrar antes de validar corrupção — desperdício e vazamento de erro misturado (corrupção vs chave errada). |

---

## 3. Dados do protótipo

Ambiente: **Apple M3 Pro, Go 1.25, darwin/arm64, AES-NI ativo**

Comando reproduzível:

```bash
go test ./experiments/pagestore/ -bench=. -benchmem -run=^$ -benchtime=2s
```

### 3.1 Throughput (páginas de 8KB)

| Operação | Sem cifra | Com AES-GCM | Overhead | Observação |
|---|---:|---:|---:|---|
| Write | 1569 MB/s (5.2µs) | 1000 MB/s (8.2µs) | +57 % | Write → SO buffer; fsync não incluído |
| Read sequencial | 3725 MB/s (2.2µs) | 2045 MB/s (4.0µs) | +82 % | Dominado por page cache do SO quente |
| Cifra pura (encrypt) | — | 4301 MB/s | — | Isola custo CPU da GCM |
| Cifra pura (decrypt) | — | 4933 MB/s | — | Verify + decrypt |

### 3.2 Alocações

| Operação | Allocs/op | Bytes/op |
|---|---:|---:|
| Write sem cifra | 1 | 8192 |
| Write com cifra | 4 | 16408 |
| Read sem cifra | 1 | 8192 |
| Read com cifra | 3 | 16392 |

---

## 4. Interpretação dos números

1. **AES-NI funciona.** 4-5 GB/s em GCM puro é coerente com o esperado para o M3 Pro — cerca de 2µs para cifrar 8KB. Em CPUs sem AES-NI (x86 antigo, ARM sem extensões) o overhead seria 5-10x maior.
2. **Custo dominante no read cifrado = AES + alocação dupla.** A primeira alocação é a `Page` ([8192]byte); a segunda é a saída do `Decrypt` (allocação do GCM-Open). Otimizável depois via buffer pool (Fase 2) que reusa páginas.
3. **Overhead relativo é alto (+57% / +82%) mas o absoluto é pequeno.** Mesmo no pior caso, fazemos ~250k páginas/segundo (2 GB/s). Qualquer workload real fica limitado por latência de disco muito antes.
4. **fsync domina em workloads durable.** Este benchmark NÃO inclui fsync — com SyncEveryWrite o throughput cai 10-100x independente da cifra. A cifra deixa de ser o gargalo.

**Veredicto:** overhead de cifra é aceitável. Prosseguir para Fase 1 com AES-GCM como default quando TDE estiver ligado.

---

## 5. Consequências

### Positivas

- Formato uniforme para heap, índices, checkpoints e WAL paginado.
- Um único ponto de cifragem/checksum — não há como esquecer de cifrar um componente.
- AAD por pageID fecha classe inteira de ataques (swap, rollback).
- Header em claro habilita debug tools sem chave (`xxd` + script de decodificar header).

### Negativas

- **8132 bytes de body usável quando cifrado** — 0.34% a menos que sem cifra. Cálculos de fator de preenchimento da B+ tree devem usar esse número.
- Formato fixo de 8KB por página **não casa com records > 8132 bytes**. Documentos grandes vão precisar de *overflow pages* ou TOAST em fase futura.
- Nonce aleatório = 12 bytes extras por write. Se um dia quisermos economizar espaço, migrar pra AES-GCM-SIV com nonce derivado de (PageID, PageLSN).
- Qualquer mudança futura no layout do header (ex: migrar de CRC32 para CRC64) exige bump de `Version` e caminho de leitura legado.

### Neutras

- PageLSN está no header mas a semântica dele só é definida na Fase 6 (recovery). Até lá, fica em zero.

---

## 6. Itens para revisitar depois

- **Página de 16KB** pra workloads analíticos (ver benchmarks em cargas reais).
- **AES-GCM-SIV** (via `github.com/google/tink/go/aead/subtle`) se o overhead de nonce aleatório começar a doer.
- **Compressão por página** antes da cifra (LZ4/Zstd). Reduz amplificação de write mas aumenta CPU.
- **Direct I/O (`O_DIRECT`)** para evitar double buffering com o page cache do SO.

---

## 7. Referências

- [experiments/pagestore/page.go](../../experiments/pagestore/page.go) — protótipo do layout.
- [experiments/pagestore/pagefile.go](../../experiments/pagestore/pagefile.go) — protótipo do PageFile.
- [experiments/pagestore/benchmark_test.go](../../experiments/pagestore/benchmark_test.go) — benchmarks.
- PostgreSQL docs: *Database Page Layout* (https://www.postgresql.org/docs/current/storage-page-layout.html).
- NIST SP 800-38D: *Recommendation for Block Cipher Modes of Operation: Galois/Counter Mode (GCM) and GMAC*.
