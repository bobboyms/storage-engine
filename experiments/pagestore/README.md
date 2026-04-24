# pagestore — spike descartável

> ⚠️ **Isto é um protótipo descartável. NÃO IMPORTE este pacote a partir de `pkg/` ou `cmd/`.**

Este diretório existe apenas para validar decisões de design da [Fase 0](../../docs/page_based_migration_plan.md) do plano de migração page-based.

## O que existe aqui

- `page.go` — protótipo do layout de página (32 bytes de header + 8160 de body = 8KB)
- `pagefile.go` — PageFile minimalista com cifra opcional (AES-GCM)
- `pagefile_test.go` — testes de correção (round-trip, tamper, AAD, chave errada)
- `benchmark_test.go` — medições que alimentam a ADR

## Rodar

```bash
# Testes
go test ./experiments/pagestore/ -v

# Benchmarks
go test ./experiments/pagestore/ -bench=. -benchmem -run=^$ -benchtime=2s
```

## Próximos passos

Os resultados do spike estão em [`docs/adr/001-page-format.md`](../../docs/adr/001-page-format.md).

Quando a Fase 1 do plano for iniciada, o código real vai para `pkg/pagestore/` com:

- Buffer pool
- Latches por página
- Integração com WAL e recovery
- API estável

**Este diretório pode ser deletado com `git rm -r experiments/pagestore/` assim que a Fase 1 estiver pronta.**
