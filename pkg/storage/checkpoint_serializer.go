package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/bobboyms/storage-engine/pkg/btree"
	"github.com/bobboyms/storage-engine/pkg/types"
)

// Constantes para Serialização de Checkpoint
const (
	CheckpointMagic   = 0x43484B50 // "CHKP"
	CheckpointVersion = 1
	NodeTypeInternal  = 0
	NodeTypeLeaf      = 1
)

// CheckpointHeader cabeçalho do arquivo de checkpoint
type CheckpointHeader struct {
	Magic      uint32
	Version    uint8
	LastLSN    uint64
	TreeGrade  int32 // T da B+ Tree
	UniqueKey  bool
	CRC32      uint32 // Checksum do conteúdo (opcional, pode ser zero por enquanto)
	NumEntries uint64 // Total de chaves na árvore (estatística)
}

// SerializeBPlusTree serializa a árvore inteira para bytes
func SerializeBPlusTree(tree *btree.BPlusTree, lastLSN uint64) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Calcula estatísticas básicas (opcional, mas bom ter no header)
	// Para simplificar, não vamos percorrer a árvore inteira só para contar agora,
	// apenas se a árvore já tivesse esse contador. Vamos salvar 0.

	header := CheckpointHeader{
		Magic:     CheckpointMagic,
		Version:   CheckpointVersion,
		LastLSN:   lastLSN,
		TreeGrade: int32(tree.T),
		UniqueKey: tree.UniqueKey,
	}

	// Escreve Header temporário (CRC será calculado depois ou ignorado por performance agora)
	if err := binary.Write(buf, binary.LittleEndian, header); err != nil {
		return nil, err
	}

	// Serializa Root Recursivamente
	if tree.Root == nil {
		// Árvore vazia? B+ Tree geralmente tem pelo menos um nó raiz vazio.
		// Se for nil, tratamos como flag especial ou erro.
		return nil, fmt.Errorf("tree root is nil")
	}

	if err := SerializeNode(buf, tree.Root); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// SerializeNode serializa um nó e seus filhos recursivamente
func SerializeNode(w io.Writer, node *btree.Node) error {
	node.RLock()
	defer node.RUnlock()

	// Node Header:
	// [Type (1 byte)] [N (4 bytes)]
	var nodeType uint8 = NodeTypeInternal
	if node.Leaf {
		nodeType = NodeTypeLeaf
	}
	if err := binary.Write(w, binary.LittleEndian, nodeType); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int32(node.N)); err != nil {
		return err
	}

	// Keys
	// Precisamos saber o tipo da chave para deserializar.
	// O Engine sabe, mas aqui no node genérico é types.Comparable.
	// O ideal seria salvar o tipo da chave no CHECKPOINT HEADER ou assumir
	// que o restore já sabe o schema.
	// Solução Robusta: Cada chave tem prefixo de tipo (similar ao WAL).
	for i := 0; i < node.N; i++ {
		keyBytes, err := serializeKey(node.Keys[i])
		if err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint16(len(keyBytes))); err != nil {
			return err
		}
		if _, err := w.Write(keyBytes); err != nil {
			return err
		}
	}

	// Pointers
	if node.Leaf {
		// DataPtrs (int64 offsets)
		for i := 0; i < node.N; i++ {
			if err := binary.Write(w, binary.LittleEndian, node.DataPtrs[i]); err != nil {
				return err
			}
		}
	} else {
		// Children (Recursão)
		// Em B+ Tree Internal Node, temos N+1 filhos.
		for i := 0; i <= node.N; i++ {
			if err := SerializeNode(w, node.Children[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

// DeserializeBPlusTree reconstrói a árvore
func DeserializeBPlusTree(data []byte) (*btree.BPlusTree, uint64, error) {
	r := bytes.NewReader(data)

	var header CheckpointHeader
	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		return nil, 0, err
	}

	if header.Magic != CheckpointMagic {
		return nil, 0, fmt.Errorf("invalid checkpoint magic")
	}

	tree := btree.NewTree(int(header.TreeGrade)) // Inicializa padrão
	tree.UniqueKey = header.UniqueKey
	// O NewTree cria um Root vazio, vamos substituí-lo.

	root, err := DeserializeNode(r, int(header.TreeGrade))
	if err != nil {
		return nil, 0, err
	}
	tree.Root = root

	return tree, header.LastLSN, nil
}

func DeserializeNode(r io.Reader, t int) (*btree.Node, error) {
	var nodeType uint8
	if err := binary.Read(r, binary.LittleEndian, &nodeType); err != nil {
		return nil, err
	}

	var nVal int32
	if err := binary.Read(r, binary.LittleEndian, &nVal); err != nil {
		return nil, err
	}

	node := btree.NewNode(t, nodeType == NodeTypeLeaf)
	node.N = int(nVal)
	// Ajustar slices para o tamanho N
	// O NewNode já aloca com capacidade, mas len é 0. Vamos append.

	// Keys
	for i := 0; i < node.N; i++ {
		var kLen uint16
		if err := binary.Read(r, binary.LittleEndian, &kLen); err != nil {
			return nil, err
		}
		kBytes := make([]byte, kLen)
		if _, err := io.ReadFull(r, kBytes); err != nil {
			return nil, err
		}
		key, err := deserializeKey(kBytes)
		if err != nil {
			return nil, err
		}
		node.Keys = append(node.Keys, key)
	}

	if node.Leaf {
		// DataPtrs
		for i := 0; i < node.N; i++ {
			var offset int64
			if err := binary.Read(r, binary.LittleEndian, &offset); err != nil {
				return nil, err
			}
			node.DataPtrs = append(node.DataPtrs, offset)
		}
	} else {
		// Children
		for i := 0; i <= node.N; i++ {
			child, err := DeserializeNode(r, t)
			if err != nil {
				return nil, err
			}
			node.Children = append(node.Children, child)
		}
	}

	return node, nil
}

// Helpers reutilizados ou adaptados do serializer.go para chaves
// Idealmente refatorar serializer.go para exportar esses helpers,
// mas para evitar mexer em muito código agora, vou duplicar/adaptar a lógica simples
// de serialização de tipos primitivos para []byte com tag.

func serializeKey(key types.Comparable) ([]byte, error) {
	buf := new(bytes.Buffer)
	switch k := key.(type) {
	case types.IntKey:
		buf.WriteByte(1) // TypeInt
		binary.Write(buf, binary.LittleEndian, int64(k))
	case types.VarcharKey:
		buf.WriteByte(2) // TypeVarchar
		str := string(k)
		binary.Write(buf, binary.LittleEndian, uint16(len(str)))
		buf.WriteString(str)
	case types.BoolKey:
		buf.WriteByte(3) // TypeBool
		var b uint8
		if k {
			b = 1
		}
		buf.WriteByte(b)
	case types.FloatKey:
		buf.WriteByte(4) // TypeFloat
		binary.Write(buf, binary.LittleEndian, float64(k))
	case types.DateKey:
		buf.WriteByte(5) // TypeDate
		ts := time.Time(k).UnixNano()
		binary.Write(buf, binary.LittleEndian, ts)
	default:
		return nil, fmt.Errorf("unsupported key type in checkpoint: %T", k)
	}
	return buf.Bytes(), nil
}

func deserializeKey(data []byte) (types.Comparable, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty key data")
	}
	kType := data[0]
	r := bytes.NewReader(data[1:])

	switch kType {
	case 1: // Int
		var i int64
		if err := binary.Read(r, binary.LittleEndian, &i); err != nil {
			return nil, err
		}
		return types.IntKey(i), nil
	case 2: // Varchar
		var l uint16
		if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
			return nil, err
		}
		b := make([]byte, l)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		return types.VarcharKey(string(b)), nil
	case 3: // Bool
		var b uint8
		if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
			return nil, err
		}
		return types.BoolKey(b == 1), nil
	case 4: // Float
		var f float64
		if err := binary.Read(r, binary.LittleEndian, &f); err != nil {
			return nil, err
		}
		return types.FloatKey(f), nil
	case 5: // Date
		var ts int64
		if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
			return nil, err
		}
		return types.DateKey(time.Unix(0, ts)), nil
	default:
		return nil, fmt.Errorf("unknown key type tag: %d", kType)
	}
}
