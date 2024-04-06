package datastore

type Persister interface {
	Reload() *KVStore
	PersistCmd(cmd [][]byte)
	Close()
}
