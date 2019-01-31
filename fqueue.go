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
	panic("implement me")
}

func (db *databaseFQueue) extract() (error, int, [][]byte) {
	panic("implement me")
}

func (db *databaseFQueue) append(data []byte) error {
	return db.fqueue.Push(data)
}
