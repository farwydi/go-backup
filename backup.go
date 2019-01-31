package backup

import (
	"errors"
)

var (
	ErrDatabaseIsNotOpen = errors.New("go-backup: database is not open")
)

type Backup struct {
	open bool
	db   DatabaseBackend
}

func NewBackup(backend DatabaseBackend) (error, *Backup) {
	return nil, &Backup{
		true,
		backend,
	}
}

func (b *Backup) Pop() (err error, data []byte) {
	if !b.open {
		return ErrDatabaseIsNotOpen, nil
	}

	return b.db.pop()
}

func (b *Backup) Append(data []byte) error {
	if !b.open {
		return ErrDatabaseIsNotOpen
	}

	return b.db.append(data)
}

func (b *Backup) GetCount() (err error, count int) {
	if !b.open {
		return ErrDatabaseIsNotOpen, 0
	}

	return b.db.count()
}

func (b *Backup) Extract() (err error, count int, arr [][]byte) {
	if !b.open {
		return ErrDatabaseIsNotOpen, 0, nil
	}

	return b.db.extract()
}