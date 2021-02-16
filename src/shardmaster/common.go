package shardmaster

import "sort"
//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

const (
	kJoin  = 1
	kLeave = 2
	kMove  = 3
	kQuery = 4
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func FillReply(result interface{}, ty int, i interface{}) {
	switch ty {
	case kJoin:
		var reply *JoinReply
		reply = result.(*JoinReply)
		*reply = i.(JoinReply)
	case kLeave:
		var reply *LeaveReply
		reply = result.(*LeaveReply)
		*reply = i.(LeaveReply)
	case kMove:
		var reply *MoveReply
		reply = result.(*MoveReply)
		*reply = i.(MoveReply)
	case kQuery:
		var reply *JoinReply
		reply = result.(*JoinReply)
		*reply = i.(JoinReply)
	}
}

func NackReply(result interface{}, ty int) {
	switch ty {
	case kJoin:
		var reply *JoinReply
		reply = result.(*JoinReply)
		reply.WrongLeader = true
	case kLeave:
		var reply *LeaveReply
		reply = result.(*LeaveReply)
		reply.WrongLeader = true
	case kMove:
		var reply *MoveReply
		reply = result.(*MoveReply)
		reply.WrongLeader = true
	case kQuery:
		var reply *JoinReply
		reply = result.(*JoinReply)
		reply.WrongLeader = true
	}
}

type GroupLoad struct {
	Gid int
	Load int
}

type ByLoad []GroupLoad

func (a ByLoad) Len() int { return len(a) }
func (a ByLoad) Swap(l, r int) { a[r], a[l] = a[l], a[r] }
func (a ByLoad) Less(l, r int) bool {
	cmp := a[l].Load - a[r].Load
	if cmp != 0 {
		return cmp < 0
	}
	return a[l].Gid < a[r].Gid
}

// sort GIDs in load ascending order
func (config *Config) SortedGIDs() []int {
	m := make(map[int]int)
	for _, gid := range config.Shards {
		m[gid]++
	}
	vec := make([]GroupLoad)
	for gid, count := range m {
		vec := append(vec, GroupLoad{gid, count}
	}
	sort.Sort(ByLoad(vec))
	ret := make([]int)
	for _, gl := range vec {
		ret = append(ret, gl.Gid)
	}
	return ret
}
