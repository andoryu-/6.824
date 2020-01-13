package shardmaster

import "raft"
import "labrpc"
import "sync"
import "labgob"
import "fmt"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	pendings map[uint64]bool
	staged   map[uint64]interface{} // map cid to instance of JoinReply/LeaveReply/MoveReply/QueryReply

	cv      *sync.Cond
	leeding bool
}

type Op struct {
	// Your data here.
	Cid            uint64
	Type           int
	JoiningServers map[int][]string
	LeavingGroups  []int
	MovingShard    int
	MovingGroup    int
	QueryConfig    int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	log.Printf("[%d] ShardMaster.Query(%v)", sm.me, args)
	defer func() {
		log.Printf("[%d] ShardMaster.Query(%v)={%v %v}", sm.me, args, reply.WrongLeader, reply.Err)
	}()
	cid := args.Cid
	op := Op{Cid: cid, Type: kQuery, QueryConfig: args.Num}
	sm.mu.Lock()

	if sm.leading {
		if _, ok := sm.staged[cid]; ok {
			*reply = sm.staged[cid].(QueryReply)
			sm.mu.Unlock()
			return
		}
	}
	if _, ok := sm.pendings[cid]; !ok {
		sm.mu.Unlock()
		if _, _, leading := sm.rf.Start(op); !leading {
			reply.WrongLeader = true
			return
		}
		sm.mu.Lock()
		sm.pendings[cid] = true
	}

	log.Printf("[%d] ShardMaster.Get(%v) before wait", sm.me, args)

	for {
		if !sm.pendings[cid] {
			break
		}
		if _, ok := sm.staged[cid]; ok {
			break
		}
		sm.cv.Wait()
	}

	if !sm.pendings[cid] {
		delete(sm.pendings, cid)
		reply.WrongLeader = true
	} else if i, ok := sm.staged[cid]; !ok {
		log.Printf("[%d] WARN Cid %d absent in staged, maybe lost leadership", sm.me, cid)
		reply.WrongLeader, reply.Err, reply.Value = true, "", ""
	} else {
		*reply = i.(QueryReply)
	}
	sm.mu.Unlock()
	return
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	// register types
	{
		a := make(map[uint64]interface{})
		a[0] = JoinReply{}
		labgob.Register(a)
		labgob.Register(a[0])
	}
	{
		a := make(map[uint64]interface{})
		a[0] = LeaveReply{}
		labgob.Register(a)
		labgob.Register(a[0])
	}
	{
		a := make(map[uint64]interface{})
		a[0] = MoveReply{}
		labgob.Register(a)
		labgob.Register(a[0])
	}
	{
		a := make(map[uint64]interface{})
		a[0] = QueryReply{}
		labgob.Register(a)
		labgob.Register(a[0])
	}
	labgob.Register(map[uint64]interface{}{})
	labgob.Register(make([]Config, 1))

	// make dedup table
	latest_cids := make(map[uint64]uint64)

	if ss := sm.rf.GetSnapshot(); len(ss) > 0 {
		data_ := make([]Config, 0, 1)
		buf := bytes.NewBuffer(ss)
		dec := labgob.NewDecoder(buf)
		if err := dec.Decode(&data_); err != nil {
			log.Fatal(fmt.Sprintf("[%d] StartKVServer() decode snapshot error! %s", sm.me, err.Error()))
		} else if err := dec.Decode(&latest_cids); err != nil {
			log.Fatal(fmt.Sprintf("[%d] ShardMaster.Run() decode dedup from snapshot error! %s", sm.me, err.Error()))
		}
		sm.configs = data_
	}

	logs, commit := sm.rf.GetLogs()
	for i, entry := range logs {
		if i > commit {
			break
		}
		op := entry.Command.(Op)
		is_duplicate := false
		if cid, ok := latest_cids[op.Cid>>32]; ok && op.Cid <= cid {
			log.Printf("[%d] cid %d present in dedup", sm.me, op.Cid)
			is_duplicate = true
		} else {
			log.Printf("[%d] cid %d op %v to apply", sm.me, op.Cid, op)
		}
		switch op.Type {
		case kJoin:
		case kLeave:
		case kMove:
		case kQuery:
		}
		if cid, ok := latest_cids[op.Cid>>32]; !ok {
			latest_cids[op.Cid>>32] = op.Cid
		} else if cid < op.Cid {
			delete(sm.staged, cid)
			latest_cids[op.Cid>>32] = op.Cid
		}
	}
	go sm.Run(latest_cids)

	return sm
}

func (sm *ShardMaster) Run(latest_cids map[uint64]uint64) {
	for msg := range sm.applyCh {
		if msg.CommandValid { // Join/Leave/Move/Query
			op, ok := msg.Command.(Op)
			if !ok {
				log.Fatal(fmt.Sprintf("[%d] ShardMaster.Run() invalid command msg from applyCh %v", sm.me, msg))
			}
			is_duplicate := false
			if cid, ok := latest_cids[op.Cid>>32]; ok && op.Cid <= cid {
				log.Printf("[%d] Cid %d present in dedup", sm.me, op.Cid)
				is_duplicate = true
			} else {
				log.Printf("[%d] Cid %d op %v to apply", sm.me, op.Cid, op)
			}
			sm.mu.Lock()
			switch op.Type {
			case kQuery:
				if op.QueryConfig >= 0 && op.QueryConfig < len(sm.configs) {
					sm.staged[op.Cid] = QueryReply{WrongLeader: false, Err: OK, Config: sm.configs[op.QueryConfig]}
				} else {
					sm.staged[op.Cid] = QueryReply{WrongLeader: false, Err: OK, Config: sm.configs[len(sm.configs)-1]}
				}
			}
			if sm.pendings[op.Cid] {
				delete(sm.pendings, op.Cid)
				sm.cv.Broadcast()
			} else if sm.leading {
				log.Printf("[%d] WARN Cid %d absent in pendings", sm.me, op.Cid)
			}
			// assuming every client is linearizable, garbage collect dedup-table
			if cid, ok := latest_cids[op.Cid>>32]; !ok {
				latest_cids[op.Cid>>32] = op.Cid
			} else if cid < op.Cid {
				delete(sm.staged, cid)
				latest_cids[op.Cid>>32] = op.Cid
			}
			sm.mu.Unlock()
		} else if msg.CommandIndex == -2 { // to leader/follower
			if sm.leading = msg.Command.(bool); !sm.leading {
				log.Printf("[%d] clear pendings", sm.me)
				sm.mu.Lock()
				for cid, _ := range sm.pendings {
					sm.pendings[cid] = false
				}
				if len(sm.pendings) > 0 {
					sm.cv.Broadcast()
				}
				sm.mu.Unlock()
			}
		} else { // install snapshot
			log.Printf("[%d] incoming snapshot %d", sm.me, msg.CommandIndex-1)
			ss, ok := msg.Command.([]byte)
			if !ok {
				log.Fatal(fmt.Sprintf("[%d] ShardMaster.Run() invalid install snapshot msg %v", sm.me, msg))
			}
			data_ := make([]Config, 0, 1)
			latest_cids_ := make(map[uint64]uint64)
			buf := bytes.NewBuffer(ss)
			dec := labgob.NewDecoder(buf)
			if err := dec.Decode(&data_); err != nil {
				log.Fatal(fmt.Sprintf("[%d] ShardMaster.Run() decode configs from snapshot error! %s", sm.me, err.Error()))
			} else if err := dec.Decode(&latest_cids_); err != nil {
				log.Fatal(fmt.Sprintf("[%d] ShardMaster.Run() decode dedup-table from snapshot error! %s", sm.me, err.Error()))
			}
			latest_cids = latest_cids_
			nkeys_new := len(data_)

			sm.mu.Lock()
			nkeys_old := len(sm.configs)
			sm.configs = data_
			sm.mu.Unlock()

			log.Printf("[%d] install snapshot complete, #keys %d => %d", sm.me, nkeys_old, nkeys_new)
		}
	}
}
