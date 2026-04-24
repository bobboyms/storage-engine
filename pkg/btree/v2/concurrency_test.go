package v2

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bobboyms/storage-engine/pkg/types"
)

func TestBTreeV2_Concurrent_Readers(t *testing.T) {
	tr := newTree(t, nil)

	const N = 2000
	for i := int64(0); i < N; i++ {
		if err := tr.Insert(k(i), i*7); err != nil {
			t.Fatal(err)
		}
	}

	const readers = 16
	const opsPerG = 200

	var wg sync.WaitGroup
	var errCount atomic.Int64

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				key := int64((g*opsPerG + i) % N)
				v, found, err := tr.Get(k(key))
				if err != nil || !found || v != key*7 {
					errCount.Add(1)
					return
				}
			}
		}(r)
	}
	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("%d erros em readers concorrentes", errCount.Load())
	}
}

func TestBTreeV2_Concurrent_ScanAndGet(t *testing.T) {
	tr := newTree(t, nil)
	const N = 2000
	for i := int64(0); i < N; i++ {
		tr.Insert(k(i), i)
	}

	var wg sync.WaitGroup
	var scanCount atomic.Int64
	var getCount atomic.Int64
	var errCount atomic.Int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tr.ScanAll(func(key types.Comparable, v int64) error {
			kk := int64(key.(types.IntKey))
			if v != kk {
				errCount.Add(1)
			}
			scanCount.Add(1)
			return nil
		})
		if err != nil {
			errCount.Add(1)
		}
	}()

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := int64((g*100 + i) % N)
				v, found, err := tr.Get(k(key))
				if err != nil || !found || v != key {
					errCount.Add(1)
					return
				}
				getCount.Add(1)
			}
		}(r)
	}

	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("%d erros em scan+get concorrentes", errCount.Load())
	}
	if scanCount.Load() != N {
		t.Fatalf("scan esperava %d, viu %d", N, scanCount.Load())
	}
	if getCount.Load() != 800 {
		t.Fatalf("gets esperados 800, recebi %d", getCount.Load())
	}
}

func TestBTreeV2_Concurrent_WritersSerializeWithReaders(t *testing.T) {
	tr := newTree(t, nil)
	const N = 500

	for i := int64(0); i < N; i++ {
		tr.Insert(k(i), 0)
	}

	var wg sync.WaitGroup
	var errCount atomic.Int64
	done := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(0); i < N; i++ {
			if err := tr.Insert(k(i), 1); err != nil {
				errCount.Add(1)
				return
			}
		}
		close(done)
	}()

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				for i := int64(0); i < N; i++ {
					v, found, err := tr.Get(k(i))
					if err != nil || !found {
						errCount.Add(1)
						return
					}
					if v != 0 && v != 1 {
						t.Errorf("valor torn para key %d: %d", i, v)
						errCount.Add(1)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	if errCount.Load() != 0 {
		t.Fatalf("%d erros de concorrência", errCount.Load())
	}

	for i := int64(0); i < N; i++ {
		v, _, _ := tr.Get(k(i))
		if v != 1 {
			t.Fatalf("key %d: esperado 1 após writer, recebi %d", i, v)
		}
	}
}

func TestBTreeV2_Concurrent_Stress(t *testing.T) {
	tr := newTree(t, nil)
	const initial = 500
	for i := int64(0); i < initial; i++ {
		tr.Insert(k(i), i*2)
	}

	var wg sync.WaitGroup
	var errCount atomic.Int64
	const rounds = 50

	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				key := int64((g*rounds + i) % initial)
				if _, _, err := tr.Get(k(key)); err != nil {
					errCount.Add(1)
					return
				}
			}
		}(r)
	}

	for s := 0; s < 2; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				count := 0
				err := tr.ScanAll(func(key types.Comparable, v int64) error {
					count++
					return nil
				})
				if err != nil {
					errCount.Add(1)
					return
				}
				if count < initial {
					errCount.Add(1)
					return
				}
			}
		}()
	}

	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				key := int64(initial + g*1000 + i)
				if err := tr.Insert(k(key), key); err != nil {
					errCount.Add(1)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	if errCount.Load() != 0 {
		t.Fatalf("%d erros no stress test", errCount.Load())
	}
}

func TestBTreeV2_Concurrent_Writers_WithSplits(t *testing.T) {
	tr := newTree(t, nil)

	const writers = 6
	const perWriter = 400

	var wg sync.WaitGroup
	var errCount atomic.Int64

	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			base := int64(worker * perWriter)
			for i := 0; i < perWriter; i++ {
				key := base + int64(i)
				if err := tr.Insert(k(key), key*11); err != nil {
					errCount.Add(1)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	if errCount.Load() != 0 {
		t.Fatalf("%d erros em writers concorrentes com split", errCount.Load())
	}

	total := int64(writers * perWriter)
	for i := int64(0); i < total; i++ {
		v, found, err := tr.Get(k(i))
		if err != nil || !found || v != i*11 {
			t.Fatalf("key %d: found=%v v=%d err=%v", i, found, v, err)
		}
	}
}

func TestBTreeV2_Varchar_Concurrent_Writers_WithSplits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "varchar-concurrent.btree.v2")
	tr, err := NewBTreeV2Varchar(path, 16, nil, VarcharKeyCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	const writers = 5
	const perWriter = 250

	var wg sync.WaitGroup
	var errCount atomic.Int64

	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := types.VarcharKey(
					fmt.Sprintf("worker-%02d-key-%04d-payload", worker, i),
				)
				if err := tr.Insert(key, int64(worker*100000+i)); err != nil {
					errCount.Add(1)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	if errCount.Load() != 0 {
		t.Fatalf("%d erros em varchar writers concorrentes com split", errCount.Load())
	}

	for g := 0; g < writers; g++ {
		for i := 0; i < perWriter; i++ {
			key := types.VarcharKey(
				fmt.Sprintf("worker-%02d-key-%04d-payload", g, i),
			)
			want := int64(g*100000 + i)
			v, found, err := tr.Get(key)
			if err != nil || !found || v != want {
				t.Fatalf("key %q: found=%v v=%d err=%v", key, found, v, err)
			}
		}
	}
}
