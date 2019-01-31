package backup

import (
	"bytes"
	"os"
	"testing"
)

func TestLoadDatabaseFQueueBackend(t *testing.T) {
	err, backend := LoadDatabaseFQueueBackend("./test_dir")

	if err != nil {
		t.Fatal(err)
	}

	if backend == nil {
		t.Fatal("backend is nil")
	}

	Close(backend)

	err = os.Remove("./test_dir")
	if err != nil {
		t.Fatal(err)
	}

	err, backend = LoadDatabaseFQueueBackend("./test_dir")

	if err != nil {
		t.Fatal(err)
	}

	if backend == nil {
		t.Fatal("backend is nil")
	}

	Close(backend)

	err = os.Remove("./test_dir")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_databaseFQueue_append_pop(t *testing.T) {
	err, backend := LoadDatabaseFQueueBackend("./test_dir")

	if err != nil {
		t.Fatal(err)
	}

	if backend == nil {
		t.Fatal("backend is nil")
	}

	d1 := []byte("Hello World")
	err = backend.append(d1)
	if err != nil {
		t.Fatal(err)
	}

	err, d2 := backend.pop()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(d2, d1) {
		t.Fatal("fail pop")
	}

	Close(backend)
	err = os.Remove("./test_dir")
	if err != nil {
		t.Fatal(err)
	}
}
