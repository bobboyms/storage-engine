package main

import (
	"fmt"
	"log"

	// Importando o pacote bson da v2

	"go.mongodb.org/mongo-driver/v2/bson"
)

// JsonToBson converts uma string JSON generic para um documento bson.M
func JsonToBson(jsonStr string) (bson.M, error) {
	var doc bson.M

	// true = Canonical (estrito), false = Relaxed
	// Tenta convertsr diretamente de JSON bytes para estrutura BSON interna
	err := bson.UnmarshalExtJSON([]byte(jsonStr), true, &doc)
	if err != nil {
		return nil, fmt.Errorf("error no parser nativo: %w", err)
	}

	return doc, nil
}

func main() {
	// Example de um JSON generic com mixed and nested types
	inputJson := `
	{
		"nome": "Produto X",
		"preco": 99.90,
		"disponivel": true,
		"tags": ["promotion", "summer"],
		"detalhes": {
			"peso": "1kg",
			"dimensoes": [10, 20, 30]
		}
	}`

	// Executa a conversion
	bsonDoc, err := JsonToBson(inputJson)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("--- Documento BSON Criado ---")
	fmt.Println(bsonDoc)

	// Example: Acessando um campo dinamicamente
	if nome, ok := bsonDoc["nome"].(string); ok {
		fmt.Printf("\nNome do produto: %s\n", nome)
	}

	bsonData, err := bson.Marshal(bsonDoc)
	if err != nil {
		log.Fatal(err)
	}

	var doc bson.M
	err = bson.Unmarshal(bsonData, &doc)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(doc["nome"].(string))

	// opts := &pebble.Options{}
	// db, err := pebble.Open("/caminho/para/data", opts)
}
