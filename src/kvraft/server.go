package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  int
	Key   string
	Value string
	Cid   uint64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	pendings map[uint64]interface{} // map cid to pending requests
	staged   map[uint64]interface{} // map cid to reply
	cv       *sync.Cond
	data     map[string]string
	leading  bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	log.Printf("[%d] KVServer.Get(%v)", kv.me, args)
	defer func() {
		log.Printf("[%d] KVServer.Get(%v)={%v %v}", kv.me, args, reply.WrongLeader, reply.Err)
	}()
	cid := args.Cid
	kv.mu.Lock()

	//kv.purgeStaged(cid)
	//log.Printf("[%d] KVServer.Get(%v) after purge staged", kv.me, args)
	if _, ok := kv.staged[cid]; ok {
		*reply = kv.staged[cid].(GetReply)
		kv.mu.Unlock()
		return
	}
	op := Op{Type: kGet, Key: args.Key, Cid: cid}
	if _, ok := kv.pendings[cid]; !ok {
		kv.mu.Unlock()
		_, _, leading := kv.rf.Start(op)
		if leading {
			kv.mu.Lock()
			// currently we only maintain pending request to detect implementation errors, such as lost notification
			// if there're too many spurious wakeups, we might use pendings[cid] to store separate condvar
			kv.pendings[cid] = op
		} else {
			reply.WrongLeader = true
			return
		}
	}

	log.Printf("[%d] KVServer.Get(%v) before wait", kv.me, args)
	for _, ok := kv.pendings[cid]; ok; _, ok = kv.pendings[cid] {
		kv.cv.Wait()
	}

	if i, ok := kv.staged[cid]; !ok {
		log.Printf("[%d] Cid %d absent in staged, maybe lost leadership", kv.me, cid)
		reply.WrongLeader, reply.Err, reply.Value = true, "", ""
	} else {
		*reply = i.(GetReply)
	}
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	log.Printf("[%d] KVServer.PutAppend({%v %v %v})", kv.me, args.Key, args.Op, args.Cid)
	defer func() {
		log.Printf("[%d] KVServer.PutAppend({%v %v %v})=%v", kv.me, args.Key, args.Op, args.Cid, reply)
	}()
	cid := args.Cid
	kv.mu.Lock()

	//kv.purgeStaged(cid)
	//log.Printf("[%d] KVServer.PutAppend({%v %v %v}) after purge staged", kv.me, args.Key, args.Op, args.Cid)
	if _, ok := kv.staged[cid]; ok {
		*reply = kv.staged[cid].(PutAppendReply)
		kv.mu.Unlock()
		return
	}
	t := kPut
	if args.Op == "Append" {
		t = kAppend
	}
	op := Op{Type: t, Key: args.Key, Value: args.Value, Cid: cid}
	if _, ok := kv.pendings[cid]; !ok {
		kv.mu.Unlock()
		_, _, leading := kv.rf.Start(op)
		if leading {
			kv.mu.Lock()
			// currently we only maintain pending request to detect implementation errors, such as lost notification
			// if there're too many spurious wakeups, we might use pendings[cid] to store separate condvar
			kv.pendings[cid] = op
		} else {
			reply.WrongLeader = true
			return
		}
	}

	log.Printf("[%d] KVServer.PutAppend({%v %v %v}) before wait", kv.me, args.Key, args.Op, args.Cid)
	for _, ok := kv.pendings[cid]; ok; _, ok = kv.pendings[cid] {
		kv.cv.Wait()
	}

	if i, ok := kv.staged[cid]; !ok {
		log.Printf("[%d] Cid %d absent in staged, maybe lost leadership", kv.me, cid)
		reply.WrongLeader = true
	} else {
		*reply = i.(PutAppendReply)
	}
	kv.mu.Unlock()
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.pendings = make(map[uint64]interface{})
	kv.staged = make(map[uint64]interface{})
	kv.cv = sync.NewCond(&(kv.mu))
	kv.data = make(map[string]string)
	kv.leading = false
	logs, commit := kv.rf.GetLogs()
	latest_cids := make(map[uint64]uint64)
	for i, entry := range logs {
		if i > commit {
			break
		}
		op := entry.Command.(Op)
		log.Printf("[%d] cid %d op %v to apply", kv.me, op.Cid, op)
		if _, ok := kv.staged[op.Cid]; ok {
			log.Printf("[%d] cid %d present in staged", kv.me, op.Cid)
			continue
		}
		if cid, ok := latest_cids[op.Cid>>32]; ok && cid < op.Cid {
			delete(kv.staged, cid)
			latest_cids[op.Cid>>32] = op.Cid
		}
		switch op.Type {
		case kPut:
			kv.data[op.Key] = op.Value
			kv.staged[op.Cid] = PutAppendReply{WrongLeader: false, Err: OK}
		case kAppend:
			if _, ok := kv.data[op.Key]; ok {
				kv.data[op.Key] += op.Value
			} else {
				kv.data[op.Key] = op.Value
			}
			kv.staged[op.Cid] = PutAppendReply{WrongLeader: false, Err: OK}
		}
	}
	//for _, cid := range latest_cids {
	//	kv.purgeStaged(cid)
	//}
	go kv.Run()
	return kv
}

func (kv *KVServer) Run() {
	latest_cids := make(map[uint64]uint64)
	for msg := range kv.applyCh {
		if msg.CommandValid {
			// Get/Put/Append
			op := msg.Command.(Op)
			kv.mu.Lock()
			// fix a bug in TestManyPartitionsOneClient3A, detect duplicate by staged.
			if _, ok := kv.staged[op.Cid]; ok {
				kv.mu.Unlock()
				log.Printf("[%d] Cid %d present in staged", kv.me, op.Cid)
				continue
			}
			log.Printf("[%d] Cid %d op %v to apply", kv.me, op.Cid, op)
			switch op.Type {
			case kGet:
				if v, ok := kv.data[op.Key]; ok {
					kv.staged[op.Cid] = GetReply{WrongLeader: false, Err: OK, Value: v}
				} else {
					kv.staged[op.Cid] = GetReply{WrongLeader: false, Err: ErrNoKey, Value: ""}
				}
			case kPut:
				kv.data[op.Key] = op.Value
				kv.staged[op.Cid] = PutAppendReply{WrongLeader: false, Err: OK}
			case kAppend:
				if _, ok := kv.data[op.Key]; ok {
					kv.data[op.Key] += op.Value
				} else {
					kv.data[op.Key] = op.Value
				}
				kv.staged[op.Cid] = PutAppendReply{WrongLeader: false, Err: OK}
			}
			if _, ok := kv.pendings[op.Cid]; ok {
				delete(kv.pendings, op.Cid)
				kv.cv.Broadcast()
			} else if kv.leading {
				log.Printf("[%d] Cid %d absent in pendings", kv.me, op.Cid)
			}
			if cid, ok := latest_cids[op.Cid>>32]; ok && cid < op.Cid {
				delete(kv.staged, cid)
				latest_cids[op.Cid>>32] = op.Cid
			}
			kv.mu.Unlock()
		} else if msg.CommandIndex == 0 {
			if kv.leading = msg.Command.(bool); !kv.leading {
				// clear pendings
				log.Printf("[%d] clear pendings", kv.me)
				kv.mu.Lock()
				n := len(kv.pendings)
				for cid, _ := range kv.pendings {
					//if o.Type == kGet {
					//    kv.staged[cid] = GetReply{WrongLeader: true}
					//    delete(kv.pendings, cid)
					//} else if o.Type == kPut || o.Type == kAppend {
					//    kv.staged[cid] = PutAppendReply{WrongLeader: true}
					//    delete(kv.pendings, cid)
					//}
					delete(kv.pendings, cid)
					//delete(kv.staged, cid)
				}
				if n > 0 {
					kv.cv.Broadcast()
				}
				kv.mu.Unlock()
			}
		} else {
			// TODO install snapshot
		}
	}
}

// manage kv.staged, it is okay to assume that each client make only one outstanding request
func (kv *KVServer) purgeStaged(cid uint64) {
	var cidbase uint64 = cid >> 32
	var mask uint64 = cidbase << 32
	for i, _ := range kv.staged {
		if ((i & mask) == mask) && i < cid {
			delete(kv.staged, i)
		}
	}
}
