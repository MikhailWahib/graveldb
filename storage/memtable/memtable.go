package memtable

type Memtable interface {
	Put(key, value string)
	Get(key string) (string, bool)
	Delete(key string)
	Size() int
}

type memtable struct {
	sl *SkipList
}

func NewMemtable() Memtable {
	return &memtable{
		sl: NewSkipList(),
	}
}

func (m *memtable) Put(key, value string) {
	m.sl.Put(key, value)
}

func (m *memtable) Get(key string) (string, bool) {
	return m.sl.Get(key)
}

func (m *memtable) Delete(key string) {
	m.sl.Delete(key)
}

func (m *memtable) Size() int {
	return m.sl.Size()
}
