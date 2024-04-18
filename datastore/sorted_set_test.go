package datastore

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/xiaoxuxiansheng/goredis/database"
	"github.com/xiaoxuxiansheng/goredis/lib"
)

func Test_skiplist_add_rem_range(t *testing.T) {
	skiplist := newSkiplist("")
	// 添加 1000 条指令
	for i := 0; i < 1000; i++ {
		skiplist.Add(int64(i), fmt.Sprintf("%d_0", i))
		skiplist.Add(int64(i), fmt.Sprintf("%d_1", i))
	}

	// 随机移除 1000 个 member
	rander := rand.New(rand.NewSource(lib.TimeNow().UnixNano()))
	remSet := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		score := rander.Intn(1000)
		index := rander.Intn(2)
		member := fmt.Sprintf("%d_%d", score, index)
		remSet[member] = struct{}{}
		skiplist.Rem(member)
	}

	t.Run("single_score", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			score := int64(rander.Intn(1000))
			member := skiplist.Range(score, score)
			sort.Slice(member, func(i, j int) bool {
				return member[i] < member[j]
			})
			expected := make([]string, 0, 2)
			member1 := fmt.Sprintf("%d_0", score)
			member2 := fmt.Sprintf("%d_1", score)
			if _, ok := remSet[member1]; !ok {
				expected = append(expected, member1)
			}
			if _, ok := remSet[member2]; !ok {
				expected = append(expected, member2)
			}

			assert.Equal(t, expected, member)
		}
	})

	t.Run("normal_score_range", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			leftScore := int64(rander.Intn(501))
			rightScore := leftScore + int64(rander.Intn(500))
			member := skiplist.Range(leftScore, rightScore)
			sort.Slice(member, func(i, j int) bool {
				splitted1 := strings.Split(member[i], "_")
				splitted2 := strings.Split(member[j], "_")
				if splitted1[0] == splitted2[0] {
					return cast.ToInt(splitted1[1]) < cast.ToInt(splitted2[1])
				}
				return cast.ToInt(splitted1[0]) < cast.ToInt(splitted2[0])
			})

			expected := make([]string, 0, 2*(rightScore-leftScore+1))
			for j := leftScore; j <= rightScore; j++ {
				member1 := fmt.Sprintf("%d_0", j)
				member2 := fmt.Sprintf("%d_1", j)
				if _, ok := remSet[member1]; !ok {
					expected = append(expected, member1)
				}
				if _, ok := remSet[member2]; !ok {
					expected = append(expected, member2)
				}
			}
			assert.Equal(t, expected, member)
		}
	})

	t.Run("with_maximum_right_range", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			leftScore := int64(rander.Intn(1000))
			rightScore := int64(-1)
			member := skiplist.Range(leftScore, rightScore)
			sort.Slice(member, func(i, j int) bool {
				splitted1 := strings.Split(member[i], "_")
				splitted2 := strings.Split(member[j], "_")
				if splitted1[0] == splitted2[0] {
					return cast.ToInt(splitted1[1]) < cast.ToInt(splitted2[1])
				}
				return cast.ToInt(splitted1[0]) < cast.ToInt(splitted2[0])
			})

			expected := make([]string, 0, 2*(1000-leftScore))
			for j := leftScore; j < 1000; j++ {
				member1 := fmt.Sprintf("%d_0", j)
				member2 := fmt.Sprintf("%d_1", j)
				if _, ok := remSet[member1]; !ok {
					expected = append(expected, member1)
				}
				if _, ok := remSet[member2]; !ok {
					expected = append(expected, member2)
				}
			}
			assert.Equal(t, expected, member)
		}
	})
}

func Test_skiplist_upsert_member_with_dif_score(t *testing.T) {
	skiplist := newSkiplist("")
	rander := rand.New(rand.NewSource(lib.TimeNow().UnixNano()))
	scoreToMembers := make(map[int64][]string)
	memberSet := make(map[string]struct{})
	for i := 0; i < 1000; i++ {
		score1 := int64(rander.Intn(1000))
		member := cast.ToString(score1)
		if _, ok := memberSet[member]; ok {
			continue
		}
		memberSet[member] = struct{}{}
		skiplist.Add(score1, member)
		score2 := int64(rander.Intn(1000))
		skiplist.Add(score2, member)
		scoreToMembers[score2] = append(scoreToMembers[score2], member)
	}

	t.Run("score_to_members", func(t *testing.T) {
		for score, members := range scoreToMembers {
			sort.Slice(members, func(i, j int) bool {
				return cast.ToInt(members[i]) < cast.ToInt(members[j])
			})

			actualMembers := skiplist.Range(score, score)
			sort.Slice(actualMembers, func(i, j int) bool {
				return cast.ToInt(actualMembers[i]) < cast.ToInt(actualMembers[j])
			})

			assert.Equal(t, members, actualMembers)

			// member 对应的前一个 score 不能查询得到 member
			for _, member := range members {
				oldScore := cast.ToInt64(member)
				if oldScore == score {
					continue
				}
				for _, gotMember := range skiplist.Range(oldScore, oldScore) {
					if gotMember == member {
						t.Errorf("old score: %d, members: %s", oldScore, gotMember)
					}
				}
			}
		}
	})
}

func Test_skiplist_to_cmd(t *testing.T) {
	skiplist := newSkiplist("")

	rander := rand.New(rand.NewSource(lib.TimeNow().UnixNano()))
	memberToScore := make(map[int]int, 1000)
	// 插入1000条数据
	for i := 0; i < 1000; i++ {
		score := rander.Intn(1000)
		member := rander.Intn(1000)
		skiplist.Add(int64(score), cast.ToString(member))
		memberToScore[member] = score
	}

	cmd := skiplist.ToCmd()
	t.Run("length", func(t *testing.T) {
		assert.Equal(t, 2*len(memberToScore)+2, len(cmd))
	})
	t.Run("command", func(t *testing.T) {
		assert.Equal(t, database.CmdTypeZAdd, database.CmdType(cmd[0]))
	})
	t.Run("key", func(t *testing.T) {
		assert.Equal(t, "", string(cmd[1]))
	})

	type scoreToMember struct {
		score, member int
	}
	actual := make([]scoreToMember, 0, 1000)
	for i := 2; i < len(cmd); i += 2 {
		actual = append(actual, scoreToMember{
			score:  cast.ToInt(string(cmd[i])),
			member: cast.ToInt(string(cmd[i+1])),
		})
	}

	sort.Slice(actual, func(i, j int) bool {
		if actual[i].score == actual[j].score {
			return actual[i].member < actual[j].member
		}
		return actual[i].score < actual[j].score
	})

	expect := make([]scoreToMember, 0, 2*len(memberToScore))
	for member, score := range memberToScore {
		expect = append(expect, scoreToMember{
			score:  score,
			member: member,
		})
	}
	sort.Slice(expect, func(i, j int) bool {
		if expect[i].score == expect[j].score {
			return expect[i].member < expect[j].member
		}
		return expect[i].score < expect[j].score
	})

	t.Run("member", func(t *testing.T) {
		assert.Equal(t, expect, actual)
	})
}
