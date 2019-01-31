package backup

import "github.com/Joinhack/fqueue"

func LoadDatabaseFQueueBackend(dir string) (error, DatabaseBackend) {

	db, err := fqueue.NewFQueue(dir)
	if err != nil {
		return err, nil
	}

	return nil, &databaseFQueue{
		dir,
		db,
	}
}

type databaseFQueue struct {
	dir    string
	fqueue *fqueue.FQueue
}

func (db *databaseFQueue) close() {
	db.fqueue.Close()
}

func (db *databaseFQueue) pop() (err error, data []byte) {
	data, err = db.fqueue.Pop()
	return
}

func (db *databaseFQueue) count() (error, int) {
	return ErrMethodNotImplemented, 0
}

func (db *databaseFQueue) extract() (err error, count int, arr [][]byte) {
	arr = make([][]byte, 0)

	for {
		var d []byte
		d, err = db.fqueue.Pop()
		if err == fqueue.QueueEmpty {
			break
		}
		if err != nil {
			return err, 0, nil
		}
		arr = append(arr, d)
	}

	return nil, len(arr), arr
}

func (db *databaseFQueue) append(data []byte) error {
	return db.fqueue.Push(data)
}
