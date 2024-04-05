package datastore

import "github.com/xiaoxuxiansheng/goredis/handler"

func (k *KVStore) getAsSet(key string) (Set, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	set, ok := v.(Set)
	if !ok {
		return nil, handler.NewWrongTypeErrReply()
	}

	return set, nil
}

func (k *KVStore) putAsSet(key string, set Set) {
	k.data[key] = set
}

type Set interface {
	Add(value string) int64
	Exist(value string) int64
	Rem(value string) int64
}

type setEntity struct {
	container map[string]struct{}
}

func newSetEntity() Set {
	return &setEntity{
		container: make(map[string]struct{}),
	}
}

func (s *setEntity) Add(value string) int64 {
	if _, ok := s.container[value]; ok {
		return 0
	}
	s.container[value] = struct{}{}
	return 1
}

func (s *setEntity) Exist(value string) int64 {
	if _, ok := s.container[value]; ok {
		return 1
	}
	return 0
}

func (s *setEntity) Rem(value string) int64 {
	if _, ok := s.container[value]; ok {
		delete(s.container, value)
		return 1
	}
	return 0
}
