package backup

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

type ts struct {
	name string
	db   DatabaseBackend
}

func TestLoadDatabaseFQueueBackend(t *testing.T) {
	backends := make([]ts, 1)

	err, fq := LoadDatabaseFQueueBackend("./TestLoadDatabaseFQueueBackend")
	if err != nil {
		t.Fatal(err)
	}
	backends[0] = ts{
		"fq",
		fq,
	}

	for _, tts := range backends {
		backend := tts.db
		t.Run(tts.name, func(t *testing.T) {
			defer os.Remove("./TestLoadDatabaseFQueueBackend")
			if backend == nil {
				t.Fatal("backend is nil")
			}

			Close(backend)
		})
	}
}

func Test_databaseFQueue_append_pop(t *testing.T) {
	backends := make([]ts, 1)

	err, fq := LoadDatabaseFQueueBackend("./databaseFQueue_append_pop")
	if err != nil {
		t.Fatal(err)
	}
	backends[0] = ts{
		"fq",
		fq,
	}

	for _, tts := range backends {
		backend := tts.db
		t.Run(tts.name, func(t *testing.T) {
			defer os.Remove("./databaseFQueue_append_pop")

			if backend == nil {
				t.Fatal("backend is nil")
			}

			d1 := []byte("Hello World 1")
			err = backend.append(d1)
			if err != nil {
				t.Fatal(err)
			}
			d2 := []byte("Hello World 2")
			err = backend.append(d2)
			if err != nil {
				t.Fatal(err)
			}
			d3 := []byte("Hello World 3")
			err = backend.append(d3)
			if err != nil {
				t.Fatal(err)
			}

			err, dx := backend.pop()
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(dx, d1) {
				t.Fatalf("fail pop %s", dx)
			}

			err, dx = backend.pop()
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(dx, d2) {
				t.Fatalf("fail pop %s", dx)
			}

			err, dx = backend.pop()
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(dx, d3) {
				t.Fatalf("fail pop %s", dx)
			}

			Close(backend)
		})
	}
}

func Test_databaseFQueue_count_extract(t *testing.T) {
	backends := make([]ts, 1)

	err, fq := LoadDatabaseFQueueBackend("./databaseFQueue_count_extract_fq")
	if err != nil {
		t.Fatal(err)
	}
	backends[0] = ts{
		"fq",
		fq,
	}

	for _, tts := range backends {
		backend := tts.db
		t.Run(tts.name, func(t *testing.T) {
			defer os.Remove("./databaseFQueue_count_extract_fq")

			if backend == nil {
				t.Fatal("backend is nil")
			}

			d1 := []byte("Hello World 1")
			err = backend.append(d1)
			if err != nil {
				t.Fatal(err)
			}
			d2 := []byte("Hello World 2")
			err = backend.append(d2)
			if err != nil {
				t.Fatal(err)
			}
			d3 := []byte("Hello World 3")
			err = backend.append(d3)
			if err != nil {
				t.Fatal(err)
			}

			err, count := backend.count()
			if err == ErrMethodNotImplemented {
				t.Log("count not implement")
			} else if err != nil {
				t.Fatal(err)
			} else {
				if count != 3 {
					t.Fatal("fail count", count)
				}
			}

			err, count, arr := backend.extract()
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(arr, [][]byte{d1, d2, d3}) {
				t.Fatal("fail extract", arr)
			}

			if count != 3 {
				t.Fatal("fail count extract", count)
			}

			Close(backend)
		})
	}
}
