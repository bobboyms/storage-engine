# Tutorial de Go (basico ao intermediario)

Este arquivo e um guia pratico, em ordem, com exemplos pequenos e focados. A ideia e voce conseguir:

- entender os fundamentos (variaveis, tipos, funcoes, interfaces, erros)
- usar concorrencia com seguranca (goroutines, channels, context)
- reconhecer armadilhas comuns de iniciante

> Dica: os trechos de codigo foram escritos para leitura. Em alguns exemplos, voce vai precisar adicionar imports (ex: `fmt`, `errors`, `time`, `context`, `sync`) para rodar.

## Indice

- [Como usar este tutorial](#como-usar-este-tutorial)
- [Variaveis e constantes](#variaveis-e-constantes)
- [Escopo e shadowing](#escopo-e-shadowing)
- [Tipos de dados](#tipos-de-dados)
- [Funcoes e metodos](#funcoes-e-metodos)
- [Interfaces](#interfaces)
- [Erros](#erros)
- [Concorrencia](#concorrencia)
  - [Goroutines](#goroutines)
  - [WaitGroup](#waitgroup)
  - [Channels](#channels)
  - [Context](#context)
  - [Worker pools](#worker-pools)
- [Lacos (for)](#lacos-for)
- [Proximos passos](#proximos-passos)

---

## Como usar este tutorial

1) Tenha o Go instalado e verifique:

```bash
go version
```

2) Para executar um projeto com `main`:

```bash
go run .
```

3) Para formatar codigo:

```bash
go fmt ./...
```

4) Para rodar testes (quando existirem):

```bash
go test ./...
```

[↑ Voltar ao indice](#indice)

---

## Variaveis e constantes

### Declaracao basica com `var`

Forma mais tradicional:

```go
var idade int
idade = 20
```

Tudo em uma linha:

```go
var idade int = 20
```

Onde:

- `var`: palavra-chave para declarar variavel
- `idade`: nome
- `int`: tipo
- `20`: valor inicial

### Inferencia de tipo (Go descobre o tipo)

Voce pode omitir o tipo se ja atribuir um valor:

```go
var nome = "Carlos"
var altura = 1.75
```

Go infere:

- `nome`: `string`
- `altura`: `float64`

### Declaracao curta `:=` (a mais usada)

Forma mais comum no dia a dia:

```go
idade := 20
nome := "Ana"
```

Regras importantes:

- so funciona **dentro de funcoes**
- sempre precisa inicializar

### Declarando varias variaveis

Em uma linha:

```go
var a, b, c int = 1, 2, 3
```

Com inferencia:

```go
var x, y = 10, "Go"
```

Em bloco:

```go
var (
	nome  string = "Lucas"
	idade int    = 25
	ativo bool   = true
)
```

### Valor zero (zero value)

Se voce nao inicializar, Go atribui um valor padrao:

```go
var numero int
var texto string
var ativo bool
```

Valores padrao:

- `int`: `0`
- `float64`: `0.0`
- `string`: `""`
- `bool`: `false`
- ponteiros, maps, slices, funcs, channels, interfaces: `nil`

### Constantes (`const`)

Constantes nao podem ser alteradas:

```go
const PI = 3.14
const linguagem = "Go"
```

Em bloco:

```go
const (
	StatusOK   = 200
	StatusErro = 500
)
```

### `var` vs `:=` (resumo)

| `var`                | `:=`                 |
|----------------------|----------------------|
| pode ser global      | so dentro de funcoes |
| tipo opcional        | tipo sempre inferido |
| pode nao inicializar | sempre inicializa    |

### Exemplo completo

```go
package main

import "fmt"

func main() {
	var nome string = "Maria"
	idade := 30
	var ativo bool

	fmt.Println(nome, idade, ativo)
}
```

Saida:

```text
Maria 30 false
```

[↑ Voltar ao indice](#indice)

---

## Escopo e shadowing

Escopo e onde uma variavel pode ser usada, dependendo de onde foi declarada.

### 1) Escopo local (dentro de funcao)

```go
func main() {
	idade := 20
	fmt.Println(idade) // ok
}
```

Fora da funcao:

```go
fmt.Println(idade) // erro: nao existe aqui
```

### 2) Escopo de bloco (`{}`)

Qualquer bloco cria um escopo novo (`if`, `for`, `switch`):

```go
func main() {
	if true {
		msg := "Ola"
		fmt.Println(msg)
	}

	// fmt.Println(msg) // erro: msg so existe dentro do if
}
```

### 3) Escopo global (no pacote)

Variaveis declaradas fora das funcoes sao globais (no pacote):

```go
package main

import "fmt"

var nome = "Go"

func main() {
	fmt.Println(nome)
}
```

Boa pratica: use globais com moderacao.

### 4) Exportacao (publico vs privado no pacote)

Em Go:

- identificadores com letra maiuscula sao exportados (publicos para outros pacotes)
- com letra minuscula sao privados ao pacote

```go
var Nome string // exportado
var idade int   // nao-exportado
```

### 5) Sombreamento (shadowing)

Quando uma variavel interna tem o mesmo nome de uma externa:

```go
func main() {
	x := 10

	if true {
		x := 20
		fmt.Println(x) // 20
	}

	fmt.Println(x) // 10
}
```

### 6) `:=` e escopo (pegadinha comum)

`:=` cria uma variavel nova; `=` altera uma existente.

```go
func main() {
	x := 10

	if true {
		x := 20 // nova variavel
		fmt.Println(x)
	}

	fmt.Println(x) // continua 10
}
```

Para reutilizar:

```go
func main() {
	x := 10

	if true {
		x = 20 // altera a existente
	}

	fmt.Println(x) // 20
}
```

### Regras rapidas

- `{}` cria um novo escopo
- `:=` cria variavel (quando ainda nao existe naquele escopo)
- `=` altera variavel existente

[↑ Voltar ao indice](#indice)

---

## Tipos de dados

Go e fortemente tipada e estaticamente tipada (tipo definido em tempo de compilacao).

### Tipos numericos

Inteiros:

```go
int    // depende da arquitetura (32 ou 64 bits)
int8   int16   int32   int64
uint   uint8   uint16  uint32  uint64
```

Ponto flutuante:

```go
float32
float64 // padrao
```

### String

```go
var nome string = "Go"
```

Strings sao imutaveis.

### Booleano

```go
var ativo bool = true
```

### Tipos compostos

Array (tamanho fixo):

```go
var numeros [3]int = [3]int{1, 2, 3}
```

Slice (dinamico):

```go
numeros := []int{1, 2, 3}
numeros = append(numeros, 4)
```

Map (dicionario):

```go
idades := map[string]int{
	"Ana":  20,
	"Joao": 30,
}
```

Struct (tipo personalizado):

```go
type Pessoa struct {
	Nome  string
	Idade int
}
```

Ponteiros:

```go
x := 10
p := &x
fmt.Println(*p) // 10
```

[↑ Voltar ao indice](#indice)

---

## Funcoes e metodos

Uma funcao e um bloco de codigo que pode receber parametros e retornar valores.

### Sintaxe basica

```go
func soma(a int, b int) int {
	return a + b
}
```

Mesmo tipo pode ser agrupado:

```go
func soma(a, b int) int {
	return a + b
}
```

### Multiplos retornos (muito usado)

```go
func dividir(a, b int) (int, error) {
	if b == 0 {
		return 0, errors.New("divisao por zero")
	}
	return a / b, nil
}
```

Uso:

```go
res, err := dividir(10, 2)
if err != nil {
	fmt.Println(err)
	return
}
fmt.Println(res)
```

### Retorno nomeado (use com moderacao)

```go
func calcular(a, b int) (resultado int) {
	resultado = a + b
	return
}
```

### Funcoes como valores

```go
operacao := func(a, b int) int {
	return a + b
}
fmt.Println(operacao(2, 3))
```

### Closures (capturam variaveis externas)

```go
func contador() func() int {
	i := 0
	return func() int {
		i++
		return i
	}
}
```

### Metodos (funcao + struct)

```go
type Pessoa struct {
	Nome string
}

func (p Pessoa) Falar() string {
	return "Ola, " + p.Nome
}
```

Receiver por valor vs ponteiro:

```go
func (p Pessoa) MudarNome(nome string) { p.Nome = nome }   // nao altera original
func (p *Pessoa) MudarNome(nome string) { p.Nome = nome }  // altera original
```

### Funcoes variadicas

```go
func soma(nums ...int) int {
	total := 0
	for _, n := range nums {
		total += n
	}
	return total
}
```

[↑ Voltar ao indice](#indice)

---

## Interfaces

Uma interface define um conjunto de comportamentos (metodos), nao dados.

### Definicao basica

```go
type Animal interface {
	Som() string
}
```

Qualquer tipo que tenha `Som() string` implementa automaticamente `Animal` (nao existe `implements`).

### Implementacao implicita

```go
type Cachorro struct{}
func (c Cachorro) Som() string { return "Au au" }

type Gato struct{}
func (g Gato) Som() string { return "Miau" }
```

Uso:

```go
func emitirSom(a Animal) {
	fmt.Println(a.Som())
}
```

### Interfaces pequenas (regra de ouro)

Prefira interfaces pequenas e focadas (ex: `io.Reader`, `io.Writer`).

### `any` e `interface{}`

Em Go moderno, `any` e um alias de `interface{}`:

```go
var x any
x = 10
x = "texto"
```

Type assertion:

```go
v, ok := x.(int)
_ = v
_ = ok
```

Type switch:

```go
switch v := x.(type) {
case int:
	fmt.Println("int:", v)
case string:
	fmt.Println("string:", v)
default:
	fmt.Println("outro:", v)
}
```

### Ponteiros e interfaces (importante)

Se o metodo so existe no ponteiro receiver, apenas o ponteiro implementa:

```go
type Pessoa struct{}
func (p *Pessoa) Falar() string { return "Oi" }
```

### Armadilha: `nil` em interface

Uma interface tem (tipo, valor). Um ponteiro `nil` dentro de uma interface pode fazer `i == nil` ser `false`.

[↑ Voltar ao indice](#indice)

---

## Erros

Go nao usa `try/catch`. Erros sao valores retornados explicitamente.

### Criando erros

Com `errors.New`:

```go
return 0, errors.New("divisao por zero")
```

Com `fmt.Errorf`:

```go
return 0, fmt.Errorf("divisao invalida: %d", b)
```

### Regra de ouro

Se recebeu `error`, verifique imediatamente:

```go
res, err := dividir(10, 0)
if err != nil {
	fmt.Println("erro:", err)
	return
}
fmt.Println(res)
```

### Wrapping (Go 1.13+)

```go
return 0, fmt.Errorf("falha ao dividir: %w", err)
```

E para checar:

```go
if errors.Is(err, ErrDivisaoPorZero) {}
```

### Erros sentinela (padrao comum)

```go
var ErrNaoEncontrado = errors.New("nao encontrado")
```

### Quando usar `panic`

`panic` nao e fluxo normal. Use apenas para bugs irrecuperaveis/estado inconsistente.

`recover` aparece muito em servidores/middlewares:

```go
func seguro() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("recuperado:", r)
		}
	}()
	panic("falha")
}
```

[↑ Voltar ao indice](#indice)

---

## Concorrencia

Concorrencia e lidar com varias tarefas; paralelismo e executar ao mesmo tempo. Go suporta ambos.

### Goroutines

Uma goroutine e uma funcao executada concorrentemente:

```go
go minhaFuncao()
```

Importante: se `main` terminar, o programa encerra (mesmo com goroutines rodando).

### WaitGroup

```go
var wg sync.WaitGroup
wg.Add(1)

go func() {
	defer wg.Done()
	fmt.Println("executando")
}()

wg.Wait()
```

### Closures e goroutines (armadilha classica)

Evite capturar a mesma variavel do loop:

```go
for i := 0; i < 3; i++ {
	i := i
	go func() { fmt.Println(i) }()
}
```

### Corrida de dados (race condition)

Detectar:

```bash
go run -race .
```

### Channels

Channel e um “tubo” tipado para mandar valores entre goroutines.

```go
ch := make(chan int)
```

Enviar e receber:

```go
ch <- 10
x := <-ch
_ = x
```

### Nao-bufferizado vs bufferizado

Nao-bufferizado sincroniza produtor/consumidor:

```go
ch := make(chan string)
```

Bufferizado cria uma fila:

```go
ch := make(chan string, 3)
```

### Fechar canal (`close`)

Regras:

- so o produtor deve fechar o canal
- enviar em canal fechado causa `panic`

Padrao de consumo:

```go
for v := range ch {
	fmt.Println(v)
}
```

### `select`

Multiplexa canais, com timeout e modo nao-bloqueante:

```go
select {
case v := <-ch:
	fmt.Println(v)
case <-time.After(500 * time.Millisecond):
	fmt.Println("timeout")
default:
	fmt.Println("sem nada agora")
}
```

### Direcao de canal (boa pratica)

```go
func produtor(out chan<- int) { out <- 1 }
func consumidor(in <-chan int) { fmt.Println(<-in) }
```

### Armadilhas comuns

- deadlock (todo mundo bloqueado)
- fechar do lado errado (consumidor fechando)
- vazamento de goroutine (presa esperando send/receive para sempre)

### Context

`context.Context` e o padrao para cancelar trabalho, propagar deadlines e carregar metadata (com cuidado).

Criando contexto cancelavel:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
```

Goroutine que respeita cancelamento:

```go
func trabalhar(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// faz trabalho
		}
	}
}
```

Timeout:

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
```

Boas praticas:

- passe `ctx` como primeiro parametro
- sempre chame `cancel()` quando usar `WithCancel/WithTimeout`
- nao guarde `ctx` em struct global

### Worker pools

Worker pool ajuda a limitar concorrencia quando ha muitas tarefas.

Modelo:

- um channel de jobs
- N workers lendo jobs ate fechar
- (opcional) channel de results

Exemplo completo (com `context` + `WaitGroup`):

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Job struct{ ID int }
type Result struct {
	JobID int
	Err   error
}

func worker(ctx context.Context, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			time.Sleep(100 * time.Millisecond)
			results <- Result{JobID: job.ID, Err: nil}
		}
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const numWorkers = 4
	jobs := make(chan Job)
	results := make(chan Result)

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker(ctx, jobs, results, &wg)
	}

	go func() {
		defer close(jobs)
		for i := 1; i <= 20; i++ {
			select {
			case <-ctx.Done():
				return
			case jobs <- Job{ID: i}:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		fmt.Println("job", r.JobID, "err", r.Err)
	}
}
```

[↑ Voltar ao indice](#indice)

---

## Lacos (for)

Em Go, so existe `for` (nao existe `while` nem `do...while`).

### `for` classico

```go
for i := 0; i < 5; i++ {
	fmt.Println(i)
}
```

### `for` como `while`

```go
i := 0
for i < 5 {
	fmt.Println(i)
	i++
}
```

### Loop infinito (use com saida)

```go
for {
	// ...
}
```

### `for range`

Slice/array:

```go
nums := []int{10, 20, 30}
for i, v := range nums {
	fmt.Println(i, v)
}
```

Map (ordem nao garantida):

```go
for nome, idade := range idades {
	fmt.Println(nome, idade)
}
```

Channel (termina quando fecha):

```go
for v := range ch {
	fmt.Println(v)
}
```

### `break` e `continue`

```go
for i := 0; i < 5; i++ {
	if i == 2 {
		continue
	}
	if i == 4 {
		break
	}
	fmt.Println(i)
}
```

[↑ Voltar ao indice](#indice)

---

## Proximos passos

Se voce quiser evoluir a partir daqui, bons proximos temas sao:

- pacotes, modulos e organizacao de projeto
- `go test`, table-driven tests e subtests
- `go vet`, `golangci-lint` (quando fizer sentido)
- `context` em I/O (HTTP, banco, filas)
- generics (quando reduzir duplicacao de forma clara)
