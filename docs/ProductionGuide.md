# Production Guide

Este documento descreve, de forma objetiva, quais recursos o storage engine implementa hoje, quais estao parcialmente implementados e quais ainda nao existem. Ele deve ser lido antes de usar o projeto com dados reais.

## Status Geral

O projeto implementa um storage engine em Go com heap page-based, B+ tree page-based, WAL, recovery em duas camadas (redo fisico por pagina + redo logico idempotente), MVCC basico, snapshots, lock manager transacional para writes, deadlock detection com waits-for graph, TDE opcional e testes de durabilidade/concorrencia. A arquitetura atual e adequada para estudo, prototipos e cargas internas controladas.

Ainda nao e um banco de dados completo de producao geral. Faltam ARIES completo com dirty page table persistida, undo fisico por pagina, locks de range para `Serializable`, anti-starvation formal, metricas internas, compressao e replicacao.

## Como Usar em Modo Mais Seguro

Use o construtor de producao e o WAL com configuracao padrao:

```go
walOpts := wal.DefaultOptions()
ww, err := wal.NewWALWriter("./db/wal.log", walOpts)
if err != nil {
    return err
}

se, err := storage.NewProductionStorageEngine(tableManager, ww)
if err != nil {
    return err
}
defer se.Close()
```

Regras:

- Use `NewProductionStorageEngine`, pois ele exige WAL e executa recovery automatico.
- Use `wal.DefaultOptions()` para durabilidade estrita. Ele usa `SyncEveryWrite`.
- Use `HeapFormatV2` e `BTreeFormatV2`; o runtime atual assume heap e indices page-based.
- Para TDE completo, configure cipher separado para heap, indices e WAL.
- Sempre trate erro de `Put`, `Commit`, `Close`, `Recover` e `NewProductionStorageEngine` como erro critico.

## Resumo de Features

| Area | Feature | Status |
|---|---|---|
| Consistencia e durabilidade | WAL / redo log | Implementado |
| Consistencia e durabilidade | fsync nos momentos corretos | Implementado no caminho duravel |
| Consistencia e durabilidade | checksums em paginas/blocos | Implementado |
| Consistencia e durabilidade | recovery deterministico | Implementado em modo ARIES-lite com CLRs |
| Consistencia e durabilidade | testes de crash/falhas simuladas | Implementado |
| Integridade dos dados | checksums | Implementado |
| Integridade dos dados | magic bytes | Implementado |
| Integridade dos dados | versao do formato | Parcial |
| Integridade dos dados | validacao de headers | Parcial |
| Integridade dos dados | limites de tamanho | Implementado |
| Integridade dos dados | protecao contra partial writes | Implementado com redo fisico por pagina |
| Integridade dos dados | testes com arquivos truncados/corrompidos | Parcial |
| Modelo de armazenamento | paginas/blocos fixos | Implementado |
| Modelo de armazenamento | registros variaveis dentro de paginas | Implementado com limite |
| Modelo de armazenamento | row-store | Implementado |
| Modelo de armazenamento | column-store | Nao implementado |
| Modelo de armazenamento | key-value puro | Parcial |
| Modelo de armazenamento | B+ tree | Implementado |
| Modelo de armazenamento | LSM-tree | Nao implementado |
| Modelo de armazenamento | hash index | Nao implementado |
| Modelo de armazenamento | formato binario versionado | Implementado |
| Modelo de armazenamento | compressao | Nao implementado |
| Concorrencia | locks/latches | Implementado |
| Concorrencia | lock-free real | Nao implementado |
| Concorrencia | MVCC | Implementado |
| Concorrencia | isolamento | Parcial |
| Concorrencia | deadlock handling | Implementado |
| Concorrencia | starvation handling | Nao implementado |
| Concorrencia | controle de snapshots | Implementado |
| Transacoes | atomicidade | Parcial |
| Transacoes | commit protocol | Implementado |
| Transacoes | rollback | Parcial |
| Transacoes | isolamento | Parcial |
| Transacoes | recovery apos crash | Implementado em modo ARIES-lite com undo logico |
| Transacoes | operacoes parcialmente aplicadas | Parcial |
| Recuperacao de espaco | free lists | Parcial |
| Recuperacao de espaco | compaction | Parcial |
| Recuperacao de espaco | garbage collection | Parcial |
| Recuperacao de espaco | vacuum | Implementado |
| Recuperacao de espaco | reaproveitamento de paginas | Parcial |
| Recuperacao de espaco | tratamento de fragmentacao | Parcial |
| Cache e I/O | buffer pool | Implementado |
| Cache e I/O | politica de eviction | Implementado |
| Cache e I/O | read-ahead | Nao implementado |
| Cache e I/O | batch writes | Parcial |
| Cache e I/O | page dirty tracking | Implementado |
| Cache e I/O | alinhamento com page size do sistema | Parcial |
| Cache e I/O | page cache do SO versus cache proprio | Parcial |
| Testes agressivos | fuzzing | Nao implementado |
| Testes agressivos | crash/recovery | Implementado |
| Testes agressivos | concorrencia pesada | Implementado |
| Testes agressivos | fault injection | Parcial |
| Testes agressivos | arquivos corrompidos | Implementado |
| Testes agressivos | benchmark com dados grandes | Parcial |
| Testes agressivos | comparacao contra referencia | Parcial |
| Observabilidade | numero de reads/writes | Nao implementado |
| Observabilidade | cache hit rate | Nao implementado |
| Observabilidade | latencia de fsync | Nao implementado |
| Observabilidade | paginas sujas | Parcial |
| Observabilidade | compaction time | Nao implementado |
| Observabilidade | locks contended | Nao implementado |
| Observabilidade | tamanho dos logs | Parcial |
| Observabilidade | tempo de recovery | Nao implementado |
| Operacional | backup/restore online | Implementado |
| Operacional | metricas internas | Nao implementado |
| Operacional | replicacao | Nao implementado |
| Seguranca | TDE opcional | Implementado |

## Consistencia e Durabilidade

### Implementado

**WAL / redo log**

O engine escreve no WAL antes de aplicar mudancas no heap e na B+ tree. `Put` e `Del` criam entradas WAL com LSN, tipo, payload e CRC. Quando uma pagina suja vai para disco, o runtime tambem grava um after-image fisico dessa pagina no WAL antes do flush efetivo. O recovery primeiro reaplica essas paginas fisicas por `pageLSN` e depois roda o replay logico idempotente de operacoes autocommit ou transacoes commitadas.

**fsync**

`wal.DefaultOptions()` usa `SyncEveryWrite`, fazendo cada `WriteEntry` sincronizar o WAL antes de retornar. `PageFile.Sync` chama fsync no arquivo, e a criacao/rotacao de arquivos tambem sincroniza diretorios quando necessario.

**Checksums**

O `pagestore` usa paginas de 8KB com header em claro e checksum CRC32-Castagnoli sobre o body em disco. O WAL tambem valida CRC32 do payload logico. Corrupcao em heap, B+ tree e WAL e detectada como erro.

**Crash/fault tests**

Existem testes de:

- `kill -9` com recovery de writes confirmados.
- reopen repetido com fuzzy checkpoint.
- corrupcao de WAL, heap e B+ tree.
- ENOSPC em filesystem pequeno.
- falha de fsync por injecao.
- stress e race detector.

### Parcial

**Recovery deterministico**

O recovery atual segue um modelo ARIES-lite com fases claras:

- analysis: varre o WAL, monta tabela de transacoes, identifica winners/losers e calcula o ponto de redo a partir do checkpoint;
- redo fisico: reaplica after-images de paginas quando `record.LSN > page.pageLSN` ou quando a pagina on-disk esta ilegivel/corrompida;
- redo logico: reaplica apenas operacoes winners/autocommit que ainda nao estao refletidas no estado atual;
- undo logico: CLRs sao reaplicadas no redo e losers pendentes sao desfeitas ao fim do recovery com escrita de novos CLRs e `ABORT`.

Cada pagina persiste `pageLSN` no header e esse valor e obedecido no redo fisico. Isso torna o replay idempotente e permite reparar pagina rasgada de heap ou indice quando o WAL contem o after-image correspondente.

Limites atuais:

- Nao ha ARIES completo.
- Nao ha dirty page table persistida no checkpoint; o checkpoint atual calcula o menor `pageLSN` sujo em memoria no momento do flush.
- O undo pos-crash atual e logico e orientado por LSN/chains de versao, nao undo fisico por pagina.
- A atomicidade runtime continua parcial quando a aplicacao em memoria falha depois do `COMMIT`.

### Nao implementado

- Teste fisico real de queda de energia.
- ARIES completo com analysis/redo/undo fisico.
- Replicacao para tolerancia a falha de maquina.
- Verificacao end-to-end tipo Jepsen.

## Integridade dos Dados

### Implementado

**Checksums**

O `pagestore` calcula CRC32-Castagnoli sobre o body on-disk de cada pagina. Quando TDE esta ligado, o checksum cobre o ciphertext, permitindo detectar corrupcao antes de tentar decifrar.

O WAL tambem usa CRC32-Castagnoli por entry logica. `WALReader.ReadEntry` valida o checksum do payload antes de devolver a entrada ao recovery.

Efeito pratico:

- bit flip no heap e detectado como `pagestore.ErrChecksumMismatch`;
- bit flip na B+ tree e detectado ao abrir, recuperar ou ler;
- bit flip no WAL e propagado como `wal.ErrChecksumMismatch`;
- tamper em paginas cifradas pode falhar por checksum ou por `ErrDecryptFailed`.

**Magic bytes**

Ha magic bytes em camadas diferentes:

- `pagestore.PageHeader.Magic`, valor ASCII `PAGE`;
- `wal.WALMagic`, valor `0xDEADBEEF`;
- meta page da B+ tree v2, magic ASCII `BTRK`.

O `PageFile.ReadPage` rejeita pagina com magic invalido. O `WALReader` rejeita entry com magic invalido. A B+ tree valida o magic da meta page ao abrir.

**Limites de tamanho**

O projeto valida varios limites:

- `PageFile` rejeita arquivo cujo tamanho nao e multiplo de `PageSize`;
- `ReadPage` rejeita `InvalidPageID` e pageID fora de `NumPages`;
- heap v2 rejeita registro que nao cabe em uma pagina;
- slotted page rejeita registro cujo tamanho excede `uint16`;
- slotted page retorna `ErrBadRecord` quando um slot tem tamanho menor que o header minimo;
- WAL rejeita `PayloadLen` maior que 1GB;
- WAL valida `bytesUsed` dentro da pagina WAL contra o body util;
- B+ tree calcula capacidade maxima de folhas/internal nodes a partir do body util da pagina.

**Testes de corrupcao**

Existem testes para:

- corrupcao de body de pagina com checksum mismatch;
- magic invalido em page file;
- arquivo de page store com tamanho invalido;
- chave errada/TDE com `ErrDecryptFailed`;
- AAD amarrado ao `PageID`;
- corrupcao de pagina WAL;
- corrupcao de heap recuperavel via WAL quando existe after-image fisico valido;
- corrupcao de B+ tree detectada em open/recovery/read;
- payload WAL corrompido em recovery;
- stress/chaos validando que dados commitados nao voltam corrompidos apos recovery.

### Parcial

**Versao do formato**

O formato tem campos de versao:

- `pagestore.VersionV1`;
- `wal.WALVersion`;
- `treeMetaVersion`;
- versao de manifest de backup.

A validacao, porem, nao e uniforme:

- B+ tree meta page valida `treeMetaVersion`;
- backup valida versao do manifest;
- recovery usa a versao do WAL para distinguir payload transacional novo;
- `PageFile.ReadPage` decodifica `PageHeader.Version`, mas nao rejeita explicitamente versao diferente de `VersionV1`;
- `WALReader.ReadEntry` valida magic, payload length e checksum, mas nao rejeita explicitamente `header.Version` desconhecida antes de entregar a entrada.

Portanto, versionamento existe, mas ainda nao e uma barreira completa de compatibilidade para todos os formatos on-disk.

**Validacao de headers**

Ha validacao importante de headers:

- magic de pagina;
- checksum de pagina;
- `PageID` do header contra offset lido;
- magic do WAL;
- tamanho de payload do WAL;
- `bytesUsed` da pagina WAL;
- magic/versao da meta page da B+ tree.

Ainda faltam validacoes mais rigorosas:

- rejeitar `PageHeader.Version` desconhecida;
- validar `PageHeader.Type` esperado pelo chamador;
- validar `Reserved`/flags quando necessario;
- validar `WALHeader.Version` e `EntryType` contra conjunto suportado no leitor;
- validar invariantes completas de slotted page antes de confiar em `numSlots`, offsets e `freeSpaceStart/freeSpaceEnd`;
- ferramenta offline de verificacao completa de heap, indices e WAL.

**Protecao contra partial writes**

Partial/torn writes sao detectados e, no caso de heap/B+ tree com WAL integro, podem ser reparados:

- se o arquivo fica com tamanho nao multiplo de 8192 bytes, `NewPageFile` falha;
- se uma pagina parcialmente escrita fica com body inconsistente, o checksum falha;
- se header/magic e corrompido, a leitura falha;
- se o WAL termina no meio de uma entry, o recovery trata `io.ErrUnexpectedEOF` no fim como tail esperado de crash mid-write e para no ultimo entry valido.

Mas isto ainda e protecao parcial:

- nao ha double-write buffer;
- nao ha copia espelhada de pagina;
- nao ha setor/page atomic write garantido;
- `WritePage` usa um write de 8KB via `WriteAt`, sem protocolo de commit por pagina;
- o reparo depende de o WAL ainda conter o after-image fisico da pagina;
- nao ha garantias equivalentes a full_page_writes + backup para corrupcao arbitraria fora da janela coberta pelo WAL.

**Testes com arquivos truncados/corrompidos**

Os testes de corrupcao sao bons para page body, magic, WAL, heap e B+ tree. Ha tambem teste para arquivo com tamanho nao multiplo de `PageSize`.

Ainda falta ampliar a matriz:

- truncamento de WAL no meio de header;
- truncamento de WAL no meio de payload;
- truncamento de heap/B+ tree em fronteira exata de pagina;
- corrupcao de `PageHeader.Version`;
- corrupcao de `PageHeader.PageID`;
- corrupcao de slotted header/slot directory;
- fuzz tests de arquivos on-disk.

### Nao implementado

- Validador offline completo tipo `fsck` para o storage.
- Checksums por registro individual dentro da pagina.
- Merkle tree/hash end-to-end por tabela ou arquivo.
- Double-write buffer para protecao forte contra torn pages.
- Validacao rigorosa de versao em todos os leitores de formato.

## Modelo de Armazenamento

### Implementado

**Paginas fixas**

A unidade basica e uma pagina fixa de 8192 bytes:

- header: 32 bytes
- body: 8160 bytes
- magic/version/page type/page ID/page LSN/checksum

O mesmo formato de pagina suporta heap, B+ tree, meta pages e WAL paginado.

**Registros variaveis dentro da pagina**

O heap v2 usa slotted pages. Slots crescem para cima e registros crescem para baixo. Cada registro tem header MVCC com:

- `Valid`
- `CreateLSN`
- `DeleteLSN`
- `PrevRecordID`

Limite: um registro precisa caber em uma pagina. Overflow pages/TOAST nao existem.

**Row-store**

O modelo principal e row/document store:

- documentos sao armazenados como bytes no heap;
- JSON valido e convertido para BSON;
- indices B+ tree apontam para `RecordID`;
- atualizacoes criam nova versao e mantem ponteiro para versao anterior.

**B+ tree**

Indices usam B+ tree v2 page-based. Ha suporte a:

- chaves fixas: int, float, bool, date;
- chaves variaveis: varchar;
- leaf/internal pages;
- split;
- scan;
- delete com rebalance;
- latch crabbing.

**Formato binario versionado**

O projeto tem varios pontos de versionamento/formato:

- `pagestore.PageHeader` com `MagicV1` e `VersionV1`;
- meta page da B+ tree com magic `BTRK` e versao;
- WAL header com versao;
- payload de WAL serializado via Protobuf;
- documentos armazenados como BSON quando possivel.

### Parcial

**Key-value**

A API exposta permite `Put`, `Get`, `Del` e `Scan` por chave de indice, entao ela tem comportamento key-value em cima de uma tabela/index. Internamente, porem, o modelo e tabela + heap + indices, nao um KV store puro.

### Nao implementado

- Column-store.
- LSM-tree.
- Hash index.
- Overflow pages para documentos maiores que uma pagina.
- Compressao de paginas, registros, WAL ou backups.
- Particionamento/sharding.
- Free space map persistente completo; o heap tem FSM em memoria como estrutura auxiliar, mas nao uma estrategia persistida robusta para larga escala.

## Concorrencia

### Implementado

**Locks e latches**

O projeto usa:

- `sync.Mutex` e `sync.RWMutex`;
- lock manager transacional para writes;
- lock por tabela;
- `opMu` para coordenar operacoes globais como backup/checkpoint;
- latches por frame no BufferPool;
- pin count para evitar eviction de pagina em uso;
- lock no WAL writer;
- lock de escrita no heap para coordenar pagina ativa;
- latch crabbing na B+ tree.

**MVCC**

O engine usa MVCC por LSN. Cada transacao de leitura captura um `SnapshotLSN`. A leitura percorre a cadeia de versoes e aplica regras de visibilidade:

- versao criada antes ou no snapshot e candidata;
- versao deletada depois do snapshot continua visivel para aquela transacao;
- versao criada depois do snapshot e ignorada, seguindo `PrevRecordID`.

**Controle de snapshots**

O `TransactionRegistry` rastreia transacoes ativas e calcula o menor `SnapshotLSN`. Isso e usado para vacuum/GC decidir quando tombstones podem ser reclamados.

**Transacoes de escrita**

Existe `WriteTransaction` com:

- buffer de operacoes;
- strict 2PL para writes via lock exclusivo por item logico;
- markers WAL `BEGIN`, `COMMIT` e `ABORT`;
- commit que escreve WAL antes de aplicar mudancas;
- rollback que descarta o write set e grava abort marker quando ha WAL.

**Modelo de lock transacional**

O escopo formal dos locks transacionais hoje e:

- lock exclusivo por item logico `(table, index, key)` para writes;
- autocommit (`Put`, `Del`, `InsertRow`, `UpsertRow`) usa o mesmo lock manager;
- operacoes multi-index autocommit travam todos os itens de indice afetados em ordem canonica;
- `WriteTransaction` segura os locks ate `Commit` ou `Rollback` (strict 2PL).

O que NAO existe nesse modelo:

- lock transacional por pagina;
- lock transacional por tabela inteira para DML comum;
- range locks / predicate locks;
- isolamento `Serializable`.

**Compatibilidade com MVCC**

Leituras continuam MVCC e nao pegam lock transacional. O lock manager cobre os casos em que MVCC sozinho nao basta no runtime atual:

- writes concorrentes no mesmo item;
- manutencao coerente de ponteiros de indice para o item afetado;
- verificacao de duplicidade/unicidade no caminho atual de indice/primary key;
- coordenacao de writes autocommit e `WriteTransaction` sobre o mesmo item logico.

**Deadlocks**

O engine agora mantem um waits-for graph implicito para requests bloqueadas. Quando detecta ciclo:

- escolhe como vitima a transacao mais jovem no ciclo (maior `txID`);
- marca a vitima como abortada;
- libera imediatamente todos os locks dela;
- devolve erro ao chamador via `ErrDeadlockVictim`;
- permite que o sobrevivente prossiga sem ficar bloqueado indefinidamente.

Tambem existe timeout de espera de lock (5s por padrao) como cerca adicional para requests que nao formam ciclo, mas ficam contenciosas por tempo demais.

### Parcial

**Isolamento**

Existem dois niveis:

- `RepeatableRead`: snapshot fixo durante a transacao.
- `ReadCommitted`: atualiza snapshot antes de cada operacao.

Ha validacao formal de conflitos write-write no escopo dos locks exclusivos por item logico. Ainda nao ha isolamento `Serializable`, predicate locking nem protecao formal contra phantoms/write skew.

### Nao implementado

- Lock-free real. Algumas leituras evitam lock global de tabela, mas usam latches/RWMutex internamente.
- Politica formal contra starvation.
- Fairness/priority/aging para writers e readers.
- Serializable isolation.
- Range locking / predicate locking.

## Transacoes

### Implementado

**Transacoes explicitas de escrita**

O projeto tem `WriteTransaction`, criada por `BeginWriteTransaction`. Ela acumula operacoes em memoria antes de qualquer mudanca visivel:

- `Put` adiciona insert/update ao write set;
- `Del` adiciona delete ao write set;
- validacao basica de tabela, indice e tipo de chave acontece antes do commit;
- lock exclusivo por item logico e adquirido no `Put`/`Del` e mantido ate o fim da transacao;
- deadlocks entre writers sao detectados e a vitima e abortada automaticamente;
- dados nao ficam visiveis antes de `Commit`;
- depois de `Commit` ou `Rollback`, novas operacoes na mesma transacao sao rejeitadas.

Existem testes cobrindo commit, rollback, delete, double commit, erro de WAL, tipos de chave e rollback com marker no WAL.

**Commit protocol**

O protocolo de commit atual e WAL-first:

1. Aloca LSNs para `BEGIN`, operacoes e `COMMIT`.
2. Escreve marker `BEGIN` no WAL.
3. Escreve cada operacao com `WALHeader.Version = txAwareWALVersion`.
4. Prefixa o payload com `txID`.
5. Escreve marker `COMMIT` no WAL.
6. Somente depois aplica as mudancas em heap e indices.
7. Marca LSNs aplicados em `appliedLSN`.

Com `wal.DefaultOptions()`, cada `WriteEntry` usa `SyncEveryWrite`, entao o `COMMIT` fica duravel antes das mudancas ficarem visiveis em memoria.

**Rollback antes de aplicar**

`Rollback` descarta o write set e, quando ha WAL, escreve `BEGIN` se necessario e depois `ABORT`. Como o write set normal ainda nao foi aplicado em heap/indices, o rollback em processo vivo continua barato; se o recovery encontrar paginas de loser persistidas, ele executa undo logico e grava CLRs/`ABORT`.

**Recovery de winners e losers**

O recovery faz uma fase de analise do WAL:

- monta tabela de transacoes por `txID`;
- identifica transacoes commitadas;
- identifica losers (`BEGIN` sem `COMMIT`/`ABORT`);
- reaplica operacoes autocommit, operacoes transacionais commitadas e CLRs;
- executa undo logico das losers ao final, gerando CLRs e `ABORT` quando necessario.

Os testes cobrem winners/losers, losers multi-operacao em varias paginas, restauracao de heap+indices e recovery de recovery apos crash no meio do undo.

**Isolamento de leitura**

Leituras usam `Transaction` separado do `WriteTransaction`, com dois niveis:

- `RepeatableRead`: snapshot fixo no LSN de inicio;
- `ReadCommitted`: atualiza snapshot antes de cada operacao.

O MVCC percorre cadeias de versoes e usa `CreateLSN`, `DeleteLSN` e `PrevRecordID` para decidir visibilidade.

### Parcial

**Atomicidade**

A atomicidade e forte tanto no recovery quanto no runtime para `WriteTransaction`.

O protocolo atual de `Commit` explicito e:

1. escreve `BEGIN`, operacoes e `COMMIT` no WAL;
2. so depois do `COMMIT` duravel entra na fase de aplicacao local;
3. segura `opMu` em modo exclusivo durante toda a fase de aplicacao, bloqueando novas leituras/escritas publicas ate a convergencia;
4. para `INSERT`/`UPDATE`, grava primeiro a nova versao no heap e so depois instala o ponteiro do indice;
5. para `DELETE`, marca o registro no heap primeiro e mantem o indice apontando para o mesmo head tombstoned;
6. se qualquer etapa falhar, o engine entra em modo `degraded` e passa a devolver `ErrEngineDegraded` nas APIs publicas ate `Recover` ou reopen.

Isso define a ordem formal heap/index e evita visibilidade parcial em processo vivo: enquanto a fase pos-commit esta em andamento, nenhuma API publica observa o prefixo aplicado; se a aplicacao falha, a janela e fechada com erro explicito em vez de deixar o processo continuar em cima de estado intermediario.

Consequencia:

- crash depois do `COMMIT` continua sendo corrigido pelo recovery;
- erro em processo vivo depois do `COMMIT` nao expoe estado parcial; o engine exige recovery antes de voltar a aceitar trafego;
- reopen/recovery reaplica deterministicamente a transacao winner e converge heap+indices ao mesmo estado final.

**Rollback**

Rollback funciona bem antes da aplicacao, porque as operacoes estao apenas no write set. No recovery, rollback pos-crash de losers persistidas agora grava CLRs e leva heap/indices ao mesmo estado final mesmo se houver crash no meio do undo.

Nao ha:

- rollback de uma transacao ja parcialmente aplicada;
- savepoints;
- nested transactions;
- undo log fisico;
- rollback runtime all-or-nothing generico para caminhos fora de `WriteTransaction`.

**Isolamento**

O isolamento de leitura e baseado em snapshot por LSN. Isso cobre `RepeatableRead` e `ReadCommitted` basicos.

Ainda nao ha:

- isolamento `Serializable`;
- deteccao de write skew;
- range protection/predicate locking;
- leitura dos writes pendentes da propria `WriteTransaction` antes do commit.

**Recovery apos crash**

O recovery transacional e fisico e suficiente para o desenho atual, em que transacoes explicitas escrevem WAL completo e so aplicam mudancas depois do `COMMIT`.

Ainda e parcial como garantia de banco maduro porque:

- nao ha ARIES completo;
- nao ha undo fisico de paginas;
- o undo usa restauracao logica de chains/ponteiros, nao page-oriented undo;
- checkpoint ainda nao persiste uma dirty page table completa.

**Operacoes parcialmente aplicadas**

Para `WriteTransaction`, o comportamento agora e:

- antes do `COMMIT`: operacoes ficam no write set e nao sao visiveis;
- depois do `COMMIT` duravel: operacoes sao aplicadas sob barreira exclusiva de runtime;
- em crash durante a aplicacao: recovery reaplica a transacao commitada;
- em erro retornado durante a aplicacao sem crash: o engine marca `degraded`, bloqueia observacao do estado parcial e exige recovery.

Esse mecanismo foi validado com fault injection no meio da aplicacao pos-commit, incluindo falha entre mutacao de heap e instalacao do indice.

### Nao implementado

- Transacoes serializable.
- Lock manager transacional.
- Two-phase locking.
- Savepoints.
- Nested transactions.
- Undo log fisico.
- Compensation log records.
- Commit atomico multi-recurso generico para caminhos que nao usam `WriteTransaction`.

## Recuperacao de Espaco

### Implementado

**Vacuum manual**

O projeto tem API de vacuum em `StorageEngine.Vacuum(tableName)`. O caminho atual:

- consulta o menor snapshot ativo em `TransactionRegistry.GetMinActiveLSN`;
- chama `HeapV2.Vacuum(minLSN)` para tabelas em heap v2;
- percorre paginas do heap;
- compacta cada pagina com `SlottedPage.Compact(minLSN)`;
- registra espaco livre no `FreeSpaceMap` em memoria para writes futuros.

Isso evita que deletes antigos fiquem para sempre ocupando espaco dentro das paginas, desde que o vacuum seja chamado.

**Garbage collection de tombstones**

Deletes no heap v2 sao lazy deletes. `MarkDeleted` marca o slot como invalido e grava `DeleteLSN`, preservando os bytes e a cadeia MVCC enquanto ainda podem existir transacoes antigas lendo aquela versao.

Durante o vacuum, `SlottedPage.Compact(minLSN)` so remove registros deletados quando `DeleteLSN <= minLSN`. Assim, o GC respeita snapshots ativos e evita remover versoes que ainda podem ser visiveis.

**Compaction dentro da pagina**

`SlottedPage.Compact` reescreve os registros sobreviventes em um buffer temporario, empacota os bytes no final da pagina, atualiza offsets dos slots e recalcula `freeSpaceEnd`. Slots removidos ficam com tamanho zero e leituras futuras retornam `ErrVacuumed`, preservando a estabilidade do `RecordID`.

A B+ tree de chaves variaveis tambem recompata a folha no delete (`LeafDeleteVar`), evitando holes no corpo da pagina de chaves variaveis.

**Reaproveitamento de espaco no heap**

O heap v2 possui `FreeSpaceMap` em memoria. Writes consultam a FSM para encontrar pagina existente com espaco suficiente antes de alocar nova pagina. Depois do vacuum, paginas com espaco recuperado voltam a ser candidatas para inserts.

Existem testes especificos para:

- compactacao de slotted page;
- respeito ao `minLSN`;
- leitura de slot vacuumed como `ErrVacuumed`;
- populacao da FSM pelo vacuum;
- reuse de espaco vacuumed;
- vacuum no nivel de storage engine;
- stress com write/read/delete/scan/checkpoint/vacuum.

### Parcial

**Free lists**

Nao existe uma free list persistente de paginas livres. A estrutura mais proxima e o `FreeSpaceMap`, mas ela e:

- em memoria;
- aproximada;
- usada como hint;
- voltada a espaco livre dentro de paginas existentes, nao a desalocacao persistente de paginas do arquivo.

**Compaction**

A compaction existe em paginas especificas, principalmente heap v2 e folhas B+ tree de chaves variaveis. Ela nao e uma compaction global do arquivo.

Ainda nao ha:

- movimentacao de registros entre paginas para consolidar espaco;
- merge/rewrite global de arquivos;
- shrink/truncate fisico apos vacuum;
- compactacao offline completa.

**Garbage collection**

O GC de tombstones existe via vacuum manual, mas nao ha processo automatico de background/autovacuum. A aplicacao precisa chamar `Vacuum` de forma operacional.

**Reaproveitamento de paginas**

O engine reaproveita espaco livre dentro de paginas ja existentes. Isso reduz crescimento futuro quando deletes antigos sao vacuumed.

Mas o `PageFile` ainda aloca novas paginas de forma monotonicamente crescente e nao oferece uma camada persistente para:

- marcar paginas inteiras como livres;
- reutilizar paginas fisicamente desalocadas;
- truncar o arquivo;
- devolver espaco ao sistema operacional.

**Tratamento de fragmentacao**

Fragmentacao interna da pagina e tratada parcialmente pela compactacao do heap e por deletes em folhas variaveis da B+ tree.

Fragmentacao em nivel de arquivo ainda nao e tratada. Arquivos podem continuar grandes mesmo depois de muitos deletes, porque o vacuum recupera espaco logico dentro das paginas, mas nao reduz fisicamente o tamanho dos arquivos.

### Nao implementado

- Free list persistente de paginas.
- Free page allocator no `PageFile`.
- Truncate/shrink fisico de arquivos apos vacuum.
- Autovacuum/background vacuum.
- Politicas configuraveis de vacuum por threshold de bloat.
- Compactacao global/offline de tabela.
- Reorganizacao de paginas para reduzir fragmentacao entre paginas.
- Persistencia da FSM entre restarts.

## Cache e I/O

### Implementado

**Buffer pool**

O projeto tem `pagestore.BufferPool`, usado por heap v2 e B+ tree v2. Ele mantem paginas quentes em memoria acima de `PageFile`.

Caracteristicas atuais:

- capacidade fixa;
- minimo de 1 frame;
- mapa `PageID -> frame`;
- LRU para ordenacao de uso;
- `Fetch` com latch compartilhado;
- `FetchForWrite` com latch exclusivo;
- `NewPage` para alocar pagina e criar frame sujo;
- pin count para impedir eviction de pagina em uso;
- `Close` com flush final.

Os helpers de storage usam capacidades padrao:

- heap v2: 64 paginas por tabela, cerca de 512KB;
- B+ tree v2: 16 paginas por indice, cerca de 128KB.

As implementacoes internas `NewHeapV2` e `NewBTreeV2` recebem capacidade de buffer pool, mas os helpers de alto nivel deixam esses valores fixos.

**Politica de eviction**

A politica implementada e LRU:

- acesso move frame para a frente da lista;
- eviction varre do final da lista, escolhendo a pagina menos recentemente usada;
- paginas pinadas nunca sao evictadas;
- se todas as paginas estao pinadas, retorna `ErrBufferPoolFull`;
- se a vitima esta suja, o flush e sincrono antes da eviction;
- se o flush falha, a pagina nao e evictada.

Existem testes para ordem LRU, paginas pinadas, flush em eviction, concorrencia e carga com muitas evictions.

**Page dirty tracking**

Cada frame tem flag atomica `dirty`. `PageHandle.MarkDirty()` marca a pagina como modificada. A pagina suja e persistida em:

- eviction;
- `FlushAll`;
- `Sync` de heap/B+ tree;
- checkpoint;
- `Close`.

O WAL tambem rastreia `currentPageDirty` para saber quando a pagina WAL atual precisa ser escrita antes do fsync.

**Flush duravel**

`BufferPool.FlushAll` coleta frames sujos, escreve cada pagina no `PageFile` e chama `PageFile.Sync`. `CreateCheckpoint` e `FuzzyCheckpoint` sincronizam WAL antes de flushar paginas sujas de heaps e indices.

### Parcial

**Batch writes**

O WAL suporta politicas de sync:

- `SyncEveryWrite`: default seguro, fsync por write;
- `SyncInterval`: fsync periodico em background;
- `SyncBatch`: fsync ao atingir volume acumulado de bytes.

Isto e batch de durabilidade no WAL, nao um sistema completo de batch write para paginas de dados.

No BufferPool, `FlushAll` escreve varias paginas sujas em uma chamada, mas ainda faz writes pagina-a-pagina e termina com um fsync. Nao ha:

- agrupamento ordenado por offset;
- write coalescing;
- group commit completo;
- flush assincrono de paginas sujas;
- background writer;
- controle de pressao de dirty pages.

**Alinhamento com o tamanho de pagina do sistema**

O formato usa pagina fixa de 8192 bytes. O `PageFile` valida que o arquivo tem tamanho multiplo de `PageSize`, e a ADR do formato justifica 8KB como escolha pratica para SSDs e page cache.

Isso e parcialmente alinhado com sistemas comuns, porque 8KB e multiplo de 4KB. Porem, o engine nao consulta o tamanho real de pagina/bloco do sistema operacional, nao ajusta `PageSize` por plataforma e nao usa `O_DIRECT`, entao nao ha garantia formal de alinhamento otimo para todos os ambientes.

**Page cache do SO versus cache proprio**

O codigo reconhece explicitamente que `PageFile` faz `ReadAt`/`WriteAt` passando pelo page cache do SO, enquanto o `BufferPool` e o cache proprio do engine.

Na pratica, isso significa que hoje existe double buffering:

- uma copia no BufferPool do engine;
- outra possivel copia no page cache do SO.

Isto e aceitavel para simplicidade e prototipos, mas ainda nao ha uma estrategia operacional completa para controlar essa interacao.

### Nao implementado

**Read-ahead**

Nao ha read-ahead/prefetch explicito no BufferPool, heap scan, B+ tree scan ou PageFile. Leituras sao feitas sob demanda por `Fetch`/`ReadPage`. O projeto depende do page cache e do read-ahead do sistema operacional quando o acesso sequencial permite.

**Otimizacoes de I/O ainda ausentes**

- Direct I/O / `O_DIRECT`.
- `posix_fadvise`/hints para acesso sequencial ou randomico.
- Read-ahead configuravel por scan.
- Background flusher.
- Dirty page thresholds.
- Eviction assincrona.
- Coalescing de writes por proximidade de offset.
- Metricas de buffer pool hit/miss/eviction/dirty pages.
- Configuracao centralizada de tamanho de buffer pool por tabela/indice nos helpers de alto nivel.

## Testes Agressivos

### Implementado

**Crash/recovery**

O projeto tem testes dedicados com build tag `chaos`:

- `TestChaosKill9CommittedWritesRecover` executa um processo filho, grava dados com WAL duravel, registra um oracle em arquivo fsyncado, mata o processo com `kill -9` e reabre o engine para validar todos os commits registrados no oracle.
- `TestChaosRepeatedReopenRecovery` faz 100 ciclos de reopen, writes e checkpoints, depois valida que todos os documentos esperados continuam presentes e intactos.

Tambem existem testes de recovery no pacote `pkg/storage`, incluindo:

- crash simulado depois de WAL fsyncado e antes de flush de heap/tree;
- recovery fisico de heap com pagina rasgada;
- recovery fisico de indice com pagina rasgada;
- recovery repetido sem duplicar versoes;
- recovery durante checkpoint em andamento;
- auto-recovery via `NewProductionStorageEngine`;
- recovery de transacoes winners/losers;
- undo pos-crash com CLRs, incluindo crash no meio do undo;
- rollback de losers com inserts, updates e deletes em varias paginas;
- fuzzy checkpoint e truncamento seguro do WAL.

**Concorrencia pesada**

Ha cobertura de concorrencia em varios niveis:

- `go test ./pkg/... -race` no Makefile e CI;
- `pkg/pagestore` testa leituras/escritas concorrentes e heavy eviction;
- `pkg/btree/v2` tem testes de concorrencia;
- `pkg/storage/concurrency_test.go` cobre writes, reads, deletes e checkpoint concorrentes;
- `tests/stress` roda writes, reads, deletes, scans, fuzzy checkpoint e vacuum em paralelo.

O alvo `make test-stress-race` executa stress com race detector.

**Arquivos corrompidos**

Os testes com build tag `faults` corrompem arquivos fisicos e validam falha controlada ou reparo via WAL, conforme o caso:

- WAL corrompido deve falhar startup/recovery;
- heap corrompido deve ser reparado quando o WAL contem after-image valido;
- B+ tree corrompida deve falhar no open/recovery/read.

Tambem ha testes unitarios de page store para checksum mismatch, magic invalido, arquivo com tamanho invalido, chave TDE errada e AAD amarrado ao `PageID`.

**CI**

O workflow `.github/workflows/ci.yml` executa:

- `go vet ./...`;
- `go build ./...`;
- `go test ./...`;
- `go test ./pkg/... -race`;
- chaos tests;
- stress tests com race detector;
- corruption fault tests;
- ENOSPC em tmpfs pequeno;
- fsync failure test com marcador de falha.

### Parcial

**Fault injection**

Existe fault injection para cenarios importantes:

- corrupcao por bit flip em WAL, heap e B+ tree;
- ENOSPC usando filesystem pequeno configurado por `STORAGE_ENGINE_ENOSPC_DIR`;
- falha de fsync usando build tag `faults` e marcador `.fail_fsync_now`.

Ainda e parcial porque nao ha uma camada generica de injecao de falhas para:

- short reads/writes;
- falha aleatoria de `ReadAt`/`WriteAt`;
- falha no meio de `BufferPool.FlushAll`;
- falha no meio de rename/copy de backup;
- clock/scheduler adversarial.

**Benchmark com dados grandes**

Ha benchmarks em `experiments/pagestore/benchmark_test.go` para:

- write de paginas sem cifra;
- write de paginas com AES-GCM;
- read sequencial;
- custo puro de encrypt/decrypt.

Esses benchmarks alimentam a ADR do formato de pagina, mas ainda nao sao benchmarks de engine completo com dados grandes.

Faltam benchmarks de:

- milhoes de registros;
- B+ tree com chaves variaveis e distribuicao realista;
- scans grandes;
- updates/deletes/vacuum sob carga;
- recovery com WAL grande;
- latencia p50/p95/p99;
- comparacao com SQLite/PostgreSQL/Pebble em workload equivalente.

**Comparacao contra referencia**

Ha oracles locais em alguns testes:

- `tests/chaos` usa `oracle.log` fsyncado para validar commits sobreviventes;
- `tests/stress` mantem maps em memoria para validar inserts/deletes apos recovery.

Isso e uma comparacao contra referencia parcial. Nao existe ainda uma suite diferencial contra uma implementacao externa ou modelo formal.

### Nao implementado

- Fuzzing nativo com `func Fuzz...`.
- Property-based testing sistematico.
- Fuzzing de WAL/page files/heap/B+ tree com corpus persistente.
- Differential testing contra SQLite, PostgreSQL, Pebble ou um modelo KV simples.
- Long-running soak tests de horas/dias em CI separado.
- Benchmarks integrados de engine completo com dados grandes.
- Testes de falha aleatoria em cada ponto de I/O.
- Simulador deterministico de crash em cada passo de commit/recovery.

## Observabilidade

### Implementado

Nao ha um subsistema de observabilidade estruturada implementado. O projeto nao expoe Prometheus, OpenTelemetry, `expvar`, endpoint HTTP de metrics, interface `Stats`, hooks de tracing, logger configuravel ou coletores internos.

O que existe hoje sao sinais pontuais:

- `StorageEngine.Recover` imprime no stdout quantas entries foram aplicadas, quantas foram puladas, checkpoint LSN e LSN atual;
- `StorageEngine.Vacuum` imprime inicio/fim do vacuum, tabela, `minLSN` e quantidade de registros reclamados;
- `BufferPool` expoe `Capacity()` e `Size()`;
- `PageFile` expoe `NumPages()` e `Path()`;
- testes usam `t.Logf` para alguns eventos, como ENOSPC observado e tamanho de WAL em teste de durabilidade.

Esses sinais ajudam em debug manual, mas nao formam observabilidade de producao.

### Parcial

**Paginas sujas**

O BufferPool rastreia paginas sujas internamente com `frame.dirty`. `FlushAll` coleta frames sujos antes de escrever no `PageFile`, e eviction tambem detecta se a vitima esta suja.

Limitacao: nao existe API publica para consultar quantidade de dirty pages, idade de dirty pages, taxa de flush ou thresholds de pressao.

**Tamanho dos logs**

O WAL tem arquivo/segmentos fisicos e politica de rotacao por `MaxSegmentBytes`. O tamanho pode ser obtido externamente via filesystem, e alguns testes fazem `os.Stat` para validar tamanho.

Limitacao: o engine nao expoe metrica interna de:

- tamanho do WAL ativo;
- quantidade de segmentos;
- bytes escritos;
- bytes desde ultimo checkpoint;
- segmentos retidos/arquivados;
- taxa de crescimento do WAL.

**Recovery**

Recovery imprime contadores de entries aplicadas/puladas e checkpoint LSN. Isso e util para diagnostico manual.

Limitacao: nao mede tempo de recovery, throughput de replay, quantidade de bytes lidos, tempo por fase de analysis/redo/undo, nem exporta esses dados como metricas.

### Nao implementado

**Numero de reads/writes**

Nao ha contadores de reads/writes em:

- `PageFile.ReadPage` / `WritePage`;
- `BufferPool.Fetch` / `FetchForWrite`;
- heap reads/writes/deletes;
- B+ tree gets/scans/upserts/deletes;
- WAL writes/syncs;
- APIs publicas `Put`, `Get`, `Del`, `Scan`.

**Cache hit rate**

O BufferPool sabe se um `Fetch` foi hit ou miss no momento da chamada, mas nao incrementa contadores. Nao ha hit rate, miss rate, eviction count, pinned-page failures ou dirty eviction count exportados.

**Latencia de fsync**

`PageFile.Sync`, `WALWriter.Sync`, checkpoints, close e backup chamam fsync, mas nenhum mede duracao. Nao ha histograma p50/p95/p99, contador de erros por tipo, nem separacao entre fsync de arquivo e fsync de diretorio.

**Compaction time**

`SlottedPage.Compact`, `HeapV2.Vacuum` e `StorageEngine.Vacuum` nao medem duracao. O vacuum imprime quantidade reclamada, mas nao tempo total, paginas percorridas, paginas modificadas, bytes recuperados ou tempo por pagina.

**Locks contended**

O projeto usa `sync.Mutex`/`sync.RWMutex`, latches por frame, lock por tabela, lock no WAL, `opMu`, locks de heap e locks de B+ tree. Nao ha medicao de:

- tempo esperando lock;
- numero de contencoes;
- tempo segurando lock;
- filas de espera;
- deadlock/timeout counters.

**Metricas operacionais ausentes**

Tambem nao ha metricas para:

- tempo de checkpoint;
- tempo de backup/restore;
- taxa de recovery;
- corrupcoes detectadas;
- ENOSPC/fsync failures;
- tombstones/vacuum backlog;
- tamanho de heap/B+ tree por tabela/indice;
- numero de transacoes ativas;
- menor snapshot ativo.

## Backup, Restore e Operacao

### Implementado

O projeto tem backup/restore online:

- pausa writes por `opMu`;
- executa checkpoint/flush;
- copia heap, indices e WAL;
- grava `manifest.json`;
- registra tamanho e SHA-256;
- possui verificacao e restore para diretorio vazio.

Tambem ha ciclo de vida do WAL:

- rotacao por tamanho;
- retencao de segmentos;
- archive opcional;
- restore de segmentos arquivados.

### Nao implementado

- Metricas internas nativas.
- Telemetria de BufferPool hit rate.
- Metricas de latencia p50/p95/p99.
- Alertas internos para corrupcao, fsync failure ou recovery failure.
- Replicacao nativa.
- Failover.
- Online schema migration.

## Seguranca e Criptografia

### Implementado

O projeto implementa TDE opcional:

- AES-GCM por body de pagina;
- header de pagina em claro para diagnostico/recovery;
- AAD amarrado ao `PageID`;
- keystore para DEKs;
- suporte a cipher separado para heap, B+ tree e WAL.

### Parcial ou ausente

- Nao ha sistema de autenticacao/autorizacao.
- Nao ha controle de acesso por tabela/usuario.
- Nao ha rotacao automatica operacional de chaves em runtime.
- TDE protege armazenamento local, mas nao substitui controles de sistema operacional, backup encryption e gestao segura da master key.

## Testes Existentes Relevantes

Use estes comandos antes de confiar em uma mudanca:

```bash
go test ./...
go test ./pkg/... -race
go test ./tests/chaos -tags chaos -count=1 -v
go test ./tests/faults -tags faults -count=1 -v
go test ./tests/stress -tags stress -count=1 -v
go test ./tests/stress -tags stress -race -count=1 -v
go test ./tests/faults -tags faults -run 'TestFault(WAL|Heap|BTree)' -count=1 -v
```

O `Makefile` tambem oferece:

```bash
make test
make test-race
make test-chaos
make test-faults
make test-stress
make test-stress-race
make test-safety
```

## Quando Nao Usar

Nao use este engine como armazenamento primario de dados criticos se voce precisa de:

- garantia formal de banco de dados completo sob todos os cenarios de crash;
- ARIES completo;
- isolamento serializable;
- atomicidade runtime forte para transacoes multi-operacao apos `COMMIT`;
- rollback de transacao parcialmente aplicada;
- range locks/predicate locks para serializacao completa;
- anti-starvation formal;
- replicacao/failover;
- compressao;
- rejeicao rigorosa de todas as versoes de formato desconhecidas;
- reparo automatico de paginas rasgadas/partial writes;
- validador offline completo de integridade;
- recuperacao automatica de espaco sem operacao manual de vacuum;
- reducao fisica do tamanho dos arquivos apos muitos deletes;
- free list persistente para reaproveitar paginas inteiras;
- read-ahead/prefetch controlado pelo engine;
- background writer ou controle fino de dirty pages;
- eliminacao de double buffering entre page cache do SO e cache proprio;
- fuzzing/property tests e differential testing contra referencia externa;
- benchmarks de engine completo com dados grandes;
- metricas estruturadas de reads/writes, cache, fsync, recovery, locks e WAL;
- column-store;
- LSM-tree;
- hash indexes;
- documentos maiores que uma pagina;
- workloads muito grandes, por exemplo acima de dezenas ou centenas de GB;
- operacao multi-tenant com dados sensiveis de terceiros sem camada adicional robusta.

## Roadmap Recomendado

Prioridade alta:

1. Evoluir de undo logico com CLRs para ARIES completo com dirty page table e undo fisico por pagina.
2. Adicionar anti-starvation, metricas de lock contention e locks de range/predicate para `Serializable`.
3. Persistir e endurecer free space map.
4. Implementar free list persistente e page allocator com reaproveitamento de paginas inteiras.
5. Persistir dirty page table/checkpoint state de forma mais completa.
6. Validar explicitamente versoes desconhecidas em PageFile e WALReader.
7. Estender a mesma cerca de atomicidade/degraded state para outros caminhos multi-recurso que ainda nao usam `WriteTransaction`.
8. Criar metricas internas para WAL, BufferPool, recovery, vacuum e erros de corrupcao.

Prioridade media:

1. Overflow pages para registros grandes.
2. Compressao configuravel por pagina ou por registro.
3. Autovacuum com thresholds de bloat e limites de impacto em latencia.
4. Compactacao offline ou rewrite de tabela com truncate fisico.
5. Adicionar metricas de BufferPool: hit rate, miss rate, evictions, dirty pages e flush latency.
6. Medir latencia de fsync, checkpoint, vacuum e recovery com histogramas.
7. Adicionar contadores de reads/writes por camada: API, heap, B+ tree, BufferPool, PageFile e WAL.
8. Implementar background writer com thresholds de dirty pages.
9. Implementar read-ahead para scans sequenciais.
10. Criar validador offline de integridade para heap, B+ tree e WAL.
11. Ampliar testes de truncamento e fuzz de arquivos on-disk.
12. Adicionar fuzzing nativo para WAL, page file, slotted page e B+ tree.
13. Criar differential tests contra uma implementacao de referencia.
14. Testes de longa duracao com race/stress/chaos.
15. Benchmarks de engine completo com dados grandes e latencia p95/p99.
16. Hardening do latch crabbing para workloads adversariais.

Prioridade baixa ou futura:

1. Replicacao.
2. Sharding/partitioning.
3. Hash index.
4. LSM-tree.
5. Column-store.

## Veredito

O projeto ja possui uma base tecnica relevante: page store, BufferPool, heap v2, B+ tree v2, WAL, checksums, magic bytes, recovery fisico+logico por `pageLSN`, MVCC, snapshots, lock manager com deadlock handling para writes, latches, TDE, testes de falha, chaos tests e stress tests. Ele e forte para aprendizado, validacao de arquitetura e uso controlado.

Para producao critica, o ponto de corte e claro: ainda faltam mecanismos que bancos maduros usam para suportar falhas, transacoes, concorrencia adversarial e crescimento operacional em larga escala, especialmente ARIES completo, dirty page table persistida, undo fisico por pagina, atomicidade runtime forte pos-commit, starvation handling, range locking para `Serializable`, validacao completa de formato, double-write/full-page strategy mais ampla, free lists persistentes, autovacuum, observabilidade estruturada, read-ahead, background writer, fuzzing, differential testing, benchmarks grandes e compressao.
