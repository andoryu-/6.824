package shardmaster

import (
	"labgob"
	"labrpc"
	"raft"
	"sync"
)

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

func (sm *ShardMaster) startOp(op *Op, result interface{}) {
	cid := op.Cid
	sm.mu.Lock()

	if sm.leading {
		if i, ok := sm.staged[cid]; ok {
			FillReply(result, op.Type, i)
			sm.mu.Unlock()
			return
		}
	}
	if _, ok := sm.pendings[cid]; !ok {
		sm.mu.Unlock()
		if _, _, leading := sm.rf.Start(op); !leading {
			NackReply(result, op.Type)
			return
		}
		sm.mu.Lock()
		sm.pendings[cid] = true
	}

	for {
		if !sm.pendings[cid] { // ack cancellation
			break
		}
		if _, ok := sm.staged[cid]; ok { // ack apply
			break
		}
		sm.cv.Wait()
	}

	if i, ok := sm.staged[cid]; ok {
		FillReply(result, op.Type, i)
	} else {
		NackReply(result, op.Type)
	}
	if _, ok := sm.pendings[cid]; ok {
		delete(sm.pendings, cid)
	}

	sm.mu.Unlock()
	return
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	log.Printf("[%d] ShardMaster.Join(%v)", sm.me, args)
	defer func() {
		log.Printf("[%d] ShardMaster.Join(%v)=%v", sm.me, args, reply)
	}()
	cid := args.Cid
	op := Op{Cid: cid, Type: kJoin, JoiningServers: args.Servers}
	startOp(&op, reply)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	log.Printf("[%d] ShardMaster.Leave(%v)", sm.me, args)
	defer func() {
		log.Printf("[%d] ShardMaster.Leave(%v)=%v", sm.me, args, reply)
	}()
	cid := args.Cid
	op := Op{Cid: cid, Type: kLeave, LeavingGroups: args.GIDs}
	startOp(&op, reply)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	log.Printf("[%d] ShardMaster.Move(%v)", sm.me, args)
	defer func() {
		log.Printf("[%d] ShardMaster.Move(%v)=%v", sm.me, args, reply)
	}()
	cid := args.Cid
	op := Op{Cid: cid, Type: kMove, MovingShard: args.Shard, MovingGroup: args.GID}
	startOp(&op, reply)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	log.Printf("[%d] ShardMaster.Query(%v)", sm.me, args)
	defer func() {
		log.Printf("[%d] ShardMaster.Query(%v)={%v %v}", sm.me, args, reply.WrongLeader, reply.Err)
	}()
	cid := args.Cid
	op := Op{Cid: cid, Type: kQuery, QueryConfig: args.Num}

	startOp(&op, reply)
	//sm.mu.Lock()

	//if sm.leading {
	//	if _, ok := sm.staged[cid]; ok {
	//		*reply = sm.staged[cid].(QueryReply)
	//		sm.mu.Unlock()
	//		return
	//	}
	//}
	//if _, ok := sm.pendings[cid]; !ok {
	//	sm.mu.Unlock()
	//	if _, _, leading := sm.rf.Start(op); !leading {
	//		reply.WrongLeader = true
	//		return
	//	}
	//	sm.mu.Lock()
	//	sm.pendings[cid] = true
	//}

	//log.Printf("[%d] ShardMaster.Get(%v) before wait", sm.me, args)

	//for {
	//	if !sm.pendings[cid] {
	//		break
	//	}
	//	if _, ok := sm.staged[cid]; ok {
	//		break
	//	}
	//	sm.cv.Wait()
	//}

	//if _, ok := sm.pendings[cid]; ok {
	//	delete(sm.pendings, cid)
	//	reply.WrongLeader = true
	//} else if i, ok := sm.staged[cid]; !ok {
	//	log.Printf("[%d] WARN Cid %d absent in staged, maybe lost leadership", sm.me, cid)
	//	reply.WrongLeader, reply.Err, reply.Value = true, "", ""
	//} else {
	//	*reply = i.(QueryReply)
	//}
	//sm.mu.Unlock()
	//return
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
	log.SetFlags(log.Ltime | log.Ldate | log.Lshortfile | Lmsgprefix)
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Num = 0
	sm.configs[0].Groups = make(map[int][]string) // initialize to empty map
	for shard := 0; shard < NShards; shard++ {
		sm.configs[0].Shards[shard] = 0 // intialize to invalid GID zero
	}

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
	var nkeys_new, nlogs_new int

	// load Raft Snapshot
	if ss := sm.rf.GetSnapshot(); len(ss) > 0 {
		data_ := make([]Config, 0, 1)
		buf := bytes.NewBuffer(ss)
		dec := labgob.NewDecoder(buf)
		if err := dec.Decode(&data_); err != nil {
			log.Fatalf("[%d] decode state from snapshot error! %s", sm.me, err.Error())
		} else if err := dec.Decode(&latest_cids); err != nil {
			log.Fatalf("[%d] decode dedup-table from snapshot error! %s", sm.me, err.Error())
		}
		sm.configs = data_
		nkeys_new = len(data_)
	}

	// apply Raft Logs into state machine
	logs, lastApplied := sm.rf.GetLogs()
	for i, entry := range logs {
		if i > lastApplied {
			break
		}
		nlogs_new++
		op := entry.Command.(Op)
		clerkID := op.Cid >> 32
		if cid, ok := latest_cids[clerkID]; ok && op.Cid <= cid {
			log.Printf("[%d] cid %d present in dedup", sm.me, op.Cid)
		} else {
			log.Printf("[%d] cid %d op %v to apply", sm.me, op.Cid, op)
			switch op.Type {
			case kJoin:
				applyJoining(op.JoiningServers)
			case kLeave:
				applyLeaving(op.LeavingGroups)
			case kMove:
				applyMoving(op.MovingShard, op.MovingGroup)
			}
			latest_cids[clerkID] = op.Cid
			if ok {
				delete(sm.staged, cid)
			}
		}
	}
	log.Printf("[%d] load complete, #configs %d #logs %d", sm.me, nkeys_new, nlogs_new)
	go sm.Run(latest_cids)

	return sm
}

func (sm *ShardMaster) Run(latest_cids map[uint64]uint64) {
	for msg := range sm.applyCh {
		if msg.CommandValid { // Join/Leave/Move/Query
			op, ok := msg.Command.(Op)
			if !ok {
				log.Fatalf("[%d] ShardMaster.Run() invalid command msg from applyCh %v", sm.me, msg)
			}
			clerkID := op.Cid >> 32
			if cid, ok := latest_cids[clerkID]; ok && op.Cid <= cid {
				log.Printf("[%d] Cid %d present in dedup", sm.me, op.Cid)
			} else {
				log.Printf("[%d] Cid %d op %v to apply", sm.me, op.Cid, op)
				sm.mu.Lock()
				switch op.Type {
				case kQuery:
					if op.QueryConfig >= 0 && op.QueryConfig < len(sm.configs) {
						sm.staged[op.Cid] = QueryReply{WrongLeader: false, Err: OK, Config: sm.configs[op.QueryConfig]}
					} else {
						sm.staged[op.Cid] = QueryReply{WrongLeader: false, Err: OK, Config: sm.configs[len(sm.configs)-1]}
					}
				case kJoin:
					applyJoining(op.JoiningServers)
					sm.staged[op.Cid] = JoinReply{WrongLeader: false, Err: OK}
				case kLeave:
					applyLeaving(op.LeavingGroups)
					sm.staged[op.Cid] = LeaveReply{WrongLeader: false, Err: OK}
				case kMove:
					applyMoving(op.MovingShard, op.MovingGroup)
					sm.staged[op.Cid] = MoveReply{WrongLeader: false, Err: OK}
				}
				latest_cids[clerkID] = op.Cid // update clerkID with latest cid
				if ok {
					delete(sm.staged, cid) // garbage collect stale cid
				}
				sm.cv.Broadcast()
				//if _, ok := sm.pendings[op.Cid]; ok {
				//	delete(sm.pendings, op.Cid)
				//} else if sm.leading {
				//	log.Printf("[%d] WARN Cid %d absent in pendings", sm.me, op.Cid)
				//}
				sm.mu.Unlock()
			}
		} else if msg.CommandIndex == -2 { // to leader/follower
			if sm.leading = msg.Command.(bool); !sm.leading {
				log.Printf("[%d] clear pendings", sm.me)
				sm.mu.Lock()
				for cid := range sm.pendings {
					sm.pendings[cid] = false // mark false to notify cancellation
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
				log.Fatalf("[%d] ShardMaster.Run() invalid install snapshot msg %v", sm.me, msg)
			}
			data_ := make([]Config, 0, 1)
			latest_cids_ := make(map[uint64]uint64)
			buf := bytes.NewBuffer(ss)
			dec := labgob.NewDecoder(buf)
			if err := dec.Decode(&data_); err != nil {
				log.Fatalf("[%d] Run() decode state from snapshot error! %s", sm.me, err.Error())
			} else if err := dec.Decode(&latest_cids_); err != nil {
				log.Fatalf("[%d] Run() decode dedup-table from snapshot error! %s", sm.me, err.Error())
			}
			latest_cids = latest_cids_
			nkeys_new := len(data_)

			sm.mu.Lock()
			nkeys_old := len(sm.configs)
			sm.configs = data_
			sm.mu.Unlock()

			log.Printf("[%d] install snapshot complete, #configs %d => %d", sm.me, nkeys_old, nkeys_new)
		}
	}
}

func (sm *ShardMaster) applyJoining(joining map[int][]string) {
	// TODO deal with initial GID zero
	for range joining {
	}
}
func (sm *ShardMaster) applyLeaving(groups []int) {
	config := &sm.configs[len(sm.configs)-1]
	new_conf := *config
	new_conf.Num = config.Num + 1
	todo_shards := make([]int)
	for _, i := range groups {
		if _, ok := new_conf.Groups[i]; ok {
			delete(new_conf.Groups, i)
			for shard, gid := range new_conf.Shards {
				if gid == i {
					todo_shards = append(todo_shards, shard)
					break
				}
			}
		}
	}
	vec := new_conf.SortedGIDs()
	var idx int
	for _, shard := range todo_shards {
		gid := vec[idx]
		new_conf.Shards[shard] = gid
		idx = (idx + 1) % len(vec)
	}
	sm.confings = append(sm.configs, new_conf)
}

func (sm *ShardMaster) applyMoving(shard int, group int) {
	config := &sm.configs[len(sm.configs)-1]
	new_conf := *config
	new_conf.Num = config.Num + 1
	new_conf.Shards[shard] = group
	sm.confings = append(sm.configs, new_conf)
}
