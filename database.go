package backup

type DatabaseBackend interface {
	append(data []byte) error
	pop() (error, []byte)
	count() (error, int)
	extract() (error, int, [][]byte)
	close()
}

func Close(backend DatabaseBackend) {
	if backend != nil {
		backend.close()
	}
}
