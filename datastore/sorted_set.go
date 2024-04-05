package datastore

import (
	"math"
	"math/rand"

	"github.com/xiaoxuxiansheng/goredis/handler"
	"github.com/xiaoxuxiansheng/goredis/lib"
)

func (k *KVStore) getAsSortedSet(key string) (SortedSet, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	zset, ok := v.(SortedSet)
	if !ok {
		return nil, handler.NewWrongTypeErrReply()
	}

	return zset, nil
}

func (k *KVStore) putAsSortedSet(key string, zset SortedSet) {
	k.data[key] = zset
}

type SortedSet interface {
	Add(score int64, member string)
	Rem(member string) int64
	Range(score1, score2 int64) []string
}

type skiplist struct {
	scoreToNode   map[int64]*skipnode
	memberToScore map[string]int64
	head          *skipnode
	rander        *rand.Rand
}

func newSkiplist() SortedSet {
	return &skiplist{
		memberToScore: make(map[string]int64),
		scoreToNode:   make(map[int64]*skipnode),
		head:          newSkipnode(0, 0),
		rander:        rand.New((rand.NewSource(lib.TimeNow().UnixNano()))),
	}
}

func (s *skiplist) Add(score int64, member string) {
	// 之前存在，需要删除
	oldScore, ok := s.memberToScore[member]
	if ok {
		if oldScore == score {
			return
		}
		s.rem(oldScore, member)
	}

	s.memberToScore[member] = score
	node, ok := s.scoreToNode[score]
	if ok {
		node.members[member] = struct{}{}
		return
	}

	// 新插入，roll 出高度
	height := s.roll()
	for int64(len(s.head.nexts)) < height+1 {
		s.head.nexts = append(s.head.nexts, nil)
	}

	inserted := newSkipnode(score, height+1)
	inserted.members[member] = struct{}{}
	s.scoreToNode[score] = inserted

	move := s.head
	for i := height; i >= 0; i-- {
		for move.nexts[i] != nil && move.nexts[i].score < score {
			move = move.nexts[i]
			continue
		}

		inserted.nexts[i] = move.nexts[i]
		move.nexts[i] = inserted
	}
}

func (s *skiplist) Rem(member string) int64 {
	// 之前存在，需要删除
	score, ok := s.memberToScore[member]
	if !ok {
		return 0
	}
	s.rem(score, member)
	return 1
}

// [score1,score2]
func (s *skiplist) Range(score1, score2 int64) []string {
	if score2 == -1 {
		score2 = math.MaxInt64
	}

	if score1 > score2 {
		return nil
	}

	move := s.head
	for i := len(s.head.nexts) - 1; i >= 0; i-- {
		for move.nexts[i] != nil && move.nexts[i].score < score1 {
			move = move.nexts[i]
		}
	}

	// 来到了 level0 层，move.nexts[i] 如果存在，就是首个 >= score1 的元素
	if move.nexts[0] == nil {
		return nil
	}

	var res []string
	for move.nexts[0] != nil && move.nexts[0].score >= score1 && move.nexts[0].score <= score2 {
		for member := range move.nexts[0].members {
			res = append(res, member)
		}
		move = move.nexts[0]
	}
	return res
}

func (s *skiplist) roll() int64 {
	var level int64
	for s.rander.Intn(2) > 0 {
		level++
	}
	return level
}

func (s *skiplist) rem(score int64, member string) {
	delete(s.memberToScore, member)
	skipnode := s.scoreToNode[score]

	delete(skipnode.members, member)
	if len(skipnode.members) > 0 {
		return
	}

	move := s.head
	for i := len(s.head.nexts) - 1; i >= 0; i-- {
		for move.nexts[i] != nil && move.nexts[i].score < score {
			move = move.nexts[i]
		}

		if move.nexts[i] == nil || move.nexts[i].score > score {
			continue
		}

		remed := move.nexts[i]
		move.nexts[i] = move.nexts[i].nexts[i]
		remed.nexts[i] = nil
	}
}

type skipnode struct {
	score   int64
	members map[string]struct{}
	nexts   []*skipnode
}

func newSkipnode(score, height int64) *skipnode {
	return &skipnode{
		score:   score,
		members: make(map[string]struct{}),
		nexts:   make([]*skipnode, height),
	}
}
