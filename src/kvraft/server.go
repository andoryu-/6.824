package raftkv

import (
	"bytes"
	"fmt"
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
	// currently we only maintain pending request to detect implementation errors, such as lost notification
	// if there're too many spurious wakeups, we might use pendings[cid] to store separate condvar
	pendings map[uint64]interface{} // map cid to pending requests

	staged    map[uint64]interface{} // map cid to reply
	cancelled map[uint64]bool        // map cid to reply
	cv        *sync.Cond
	data      map[string]string
	leading   bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	log.Printf("[%d] KVServer.Get(%v)", kv.me, args)
	defer func() {
		log.Printf("[%d] KVServer.Get(%v)={%v %v}", kv.me, args, reply.WrongLeader, reply.Err)
	}()
	cid := args.Cid
	op := Op{Type: kGet, Key: args.Key, Cid: cid}
	kv.mu.Lock()

	if kv.leading {
		if _, ok := kv.staged[cid]; ok {
			*reply = kv.staged[cid].(GetReply)
			kv.mu.Unlock()
			return
		}
	}
	if _, ok := kv.pendings[cid]; !ok {
		kv.mu.Unlock()
		if _, _, leading := kv.rf.Start(op); !leading {
			reply.WrongLeader = true
			return
		}
		kv.mu.Lock()
		kv.pendings[cid] = op
	}

	log.Printf("[%d] KVServer.Get(%v) before wait", kv.me, args)

	for {
		if _, ok := kv.pendings[cid]; !ok {
			break
		}
		if _, ok := kv.staged[cid]; ok {
			break
		}
		if _, ok := kv.cancelled[cid]; ok {
			break
		}
		kv.cv.Wait()
	}

	if _, ok := kv.cancelled[cid]; ok {
		delete(kv.cancelled, cid)
		reply.WrongLeader = true
	} else if i, ok := kv.staged[cid]; !ok {
		log.Printf("[%d] WARN Cid %d absent in staged, maybe lost leadership", kv.me, cid)
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
	t := kPut
	if args.Op == "Append" {
		t = kAppend
	}
	op := Op{Type: t, Key: args.Key, Value: args.Value, Cid: cid}
	kv.mu.Lock()

	if kv.leading {
		if _, ok := kv.staged[cid]; ok {
			*reply = kv.staged[cid].(PutAppendReply)
			kv.mu.Unlock()
			return
		}
	}
	if _, ok := kv.pendings[cid]; !ok {
		kv.mu.Unlock()
		if _, _, leading := kv.rf.Start(op); !leading {
			reply.WrongLeader = true
			return
		}
		kv.mu.Lock()
		kv.pendings[cid] = op
	}

	log.Printf("[%d] KVServer.PutAppend({%v %v %v}) before wait", kv.me, args.Key, args.Op, args.Cid)

	for {
		if _, ok := kv.pendings[cid]; !ok {
			break
		}
		if _, ok := kv.staged[cid]; ok {
			break
		}
		if _, ok := kv.cancelled[cid]; ok {
			break
		}
		kv.cv.Wait()
	}

	if _, ok := kv.cancelled[cid]; ok {
		delete(kv.cancelled, cid)
		reply.WrongLeader = true
	} else if i, ok := kv.staged[cid]; !ok {
		log.Printf("[%d] WARN Cid %d absent in staged, maybe lost leadership", kv.me, cid)
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

	a := make(map[uint64]interface{})
	a[0] = PutAppendReply{}
	b := make(map[uint64]interface{})
	b[0] = GetReply{}
	labgob.Register(map[uint64]interface{}{})
	labgob.Register(a)
	labgob.Register(b)
	labgob.Register(PutAppendReply{})
	labgob.Register(GetReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.maxraftstate = 100 // FIXME only for testing

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.pendings = make(map[uint64]interface{})
	kv.staged = make(map[uint64]interface{})
	kv.cancelled = make(map[uint64]bool)
	kv.cv = sync.NewCond(&(kv.mu))
	kv.data = make(map[string]string)
	kv.leading = false
	latest_cids := make(map[uint64]uint64)
	//staged_ := make(map[uint64]interface{})
	if ss := kv.rf.GetSnapshot(); len(ss) > 0 {
		data_ := make(map[string]string)
		buf := bytes.NewBuffer(ss)
		dec := labgob.NewDecoder(buf)
		if err := dec.Decode(&data_); err != nil {
			log.Fatal(fmt.Sprintf("[%d] StartKVServer() decode snapshot error! %s", kv.me, err.Error()))
		} else if err := dec.Decode(&latest_cids); err != nil {
			log.Fatal(fmt.Sprintf("[%d] KVServer.Run() decode dedup from snapshot error! %s", kv.me, err.Error()))
		}
		kv.data = data_
	}

	logs, commit := kv.rf.GetLogs()
	for i, entry := range logs {
		if i > commit {
			break
		}
		op := entry.Command.(Op)
		//log.Printf("[%d] cid %d op %v to apply", kv.me, op.Cid, op)
		//if _, ok := kv.staged[op.Cid]; ok {
		//	log.Printf("[%d] cid %d present in staged", kv.me, op.Cid)
		//	continue
		//}
		is_duplicate := false
		if cid, ok := latest_cids[op.Cid>>32]; ok && op.Cid <= cid {
			log.Printf("[%d] cid %d present in dedup", kv.me, op.Cid)
			is_duplicate = true
		} else {
			log.Printf("[%d] cid %d op %v to apply", kv.me, op.Cid, op)
		}
		switch op.Type {
		case kPut:
			if !is_duplicate {
				kv.data[op.Key] = op.Value
			}
			kv.staged[op.Cid] = PutAppendReply{WrongLeader: false, Err: OK}
		case kAppend:
			//if _, ok := kv.data[op.Key]; ok {
			//	kv.data[op.Key] += op.Value
			//} else {
			//	kv.data[op.Key] = op.Value
			//}
			if !is_duplicate {
				kv.data[op.Key] += op.Value
			}
			kv.staged[op.Cid] = PutAppendReply{WrongLeader: false, Err: OK}
		}
		if cid, ok := latest_cids[op.Cid>>32]; ok && cid < op.Cid {
			delete(kv.staged, cid)
			latest_cids[op.Cid>>32] = op.Cid
		}
	}
	go kv.Run(latest_cids)
	return kv
}

func (kv *KVServer) Run(latest_cids map[uint64]uint64) {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			// Get/Put/Append
			op, ok := msg.Command.(Op)
			if !ok {
				log.Fatal(fmt.Sprintf("[%d] KVServer.Run() invalid command msg from applyCh %v", kv.me, msg))
			}
			kv.mu.Lock()
			// fix a bug in TestManyPartitionsOneClient3A, detect duplicate by staged.
			//if _, ok := kv.staged[op.Cid]; ok {
			//	kv.mu.Unlock()
			//	log.Printf("[%d] Cid %d present in staged", kv.me, op.Cid)
			//	continue
			//}
			is_duplicate := false
			if cid, ok := latest_cids[op.Cid>>32]; ok && op.Cid <= cid {
				log.Printf("[%d] Cid %d present in dedup", kv.me, op.Cid)
				is_duplicate = true
			} else {
				log.Printf("[%d] Cid %d op %v to apply", kv.me, op.Cid, op)
			}
			switch op.Type {
			case kGet:
				if v, ok := kv.data[op.Key]; ok {
					kv.staged[op.Cid] = GetReply{WrongLeader: false, Err: OK, Value: v}
				} else {
					kv.staged[op.Cid] = GetReply{WrongLeader: false, Err: ErrNoKey}
				}
			case kPut:
				if !is_duplicate {
					kv.data[op.Key] = op.Value
				}
				kv.staged[op.Cid] = PutAppendReply{WrongLeader: false, Err: OK}
			case kAppend:
				if !is_duplicate {
					kv.data[op.Key] += op.Value
				}
				//if _, ok := kv.data[op.Key]; ok {
				//	kv.data[op.Key] += op.Value
				//} else {
				//	kv.data[op.Key] = op.Value
				//}
				kv.staged[op.Cid] = PutAppendReply{WrongLeader: false, Err: OK}
			}
			if _, ok := kv.pendings[op.Cid]; ok {
				delete(kv.pendings, op.Cid)
				kv.cv.Broadcast()
			} else if kv.leading {
				log.Printf("[%d] WARN Cid %d absent in pendings", kv.me, op.Cid)
			}
			// assuming every client is linearizable, garbage collect dedup-table
			if cid, ok := latest_cids[op.Cid>>32]; !ok {
				latest_cids[op.Cid>>32] = op.Cid
			} else if cid < op.Cid {
				delete(kv.staged, cid)
				latest_cids[op.Cid>>32] = op.Cid
			}
			kv.mu.Unlock()
			if kv.maxraftstate > 0 && kv.rf.RaftStateSize() >= kv.maxraftstate { // start snapshot
				w := new(bytes.Buffer)
				enc := labgob.NewEncoder(w)
				if err := enc.Encode(kv.data); err != nil {
					log.Fatal(fmt.Sprintf("[%d] KVServer.Run() encode KVMap into snapshot error! %s", kv.me, err.Error()))
				} else if err := enc.Encode(latest_cids); err != nil {
					log.Fatal(fmt.Sprintf("[%d] KVServer.Run() encode dedup into snapshot error! %s", kv.me, err.Error()))
				}
				// NOTE: here it hold applyCh and wait for raft lock. Raft must not hold raft lock and wait for applyCh
				kv.rf.SaveSnapshot(w.Bytes(), msg.CommandIndex-1)
			}
		} else if msg.CommandIndex == -2 { // to leader/follower
			if kv.leading = msg.Command.(bool); !kv.leading {
				// clear pendings
				log.Printf("[%d] clear pendings", kv.me)
				kv.mu.Lock()
				n := len(kv.pendings)
				for cid, _ := range kv.pendings {
					kv.cancelled[cid] = true
					delete(kv.pendings, cid)
				}
				if n > 0 {
					kv.cv.Broadcast()
				}
				kv.mu.Unlock()
			}
		} else { // install snapshot
			log.Printf("[%d] incoming snapshot", kv.me)
			ss, ok := msg.Command.([]byte)
			if !ok {
				log.Fatal(fmt.Sprintf("[%d] KVServer.Run() invalid install snapshot msg %v", kv.me, msg))
			}
			data_ := make(map[string]string)
			//staged_ := make(map[uint64]interface{})
			latest_cids_ := make(map[uint64]uint64)
			buf := bytes.NewBuffer(ss)
			dec := labgob.NewDecoder(buf)
			if err := dec.Decode(&data_); err != nil {
				log.Fatal(fmt.Sprintf("[%d] KVServer.Run() decode KVMap from snapshot error! %s", kv.me, err.Error()))
			} else if err := dec.Decode(&latest_cids_); err != nil {
				log.Fatal(fmt.Sprintf("[%d] KVServer.Run() decode dedup from snapshot error! %s", kv.me, err.Error()))
			}
			//for k, _ := range staged_ {
			//	if cid, ok := latest_cids[k>>32]; !ok || cid < k {
			//		latest_cids[k>>32] = k
			//	}
			//}
			for _, k := range latest_cids_ {
				if cid, ok := latest_cids[k>>32]; !ok || cid < k {
					latest_cids[k>>32] = k
				}
			}
			nkeys_new := len(data_)
			kv.mu.Lock()
			nkeys_old := len(kv.data)
			kv.data = data_
			//kv.staged = staged_
			kv.mu.Unlock()
			log.Printf("[%d] install snapshot complete, #keys %d => %d", kv.me, nkeys_old, nkeys_new)
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
	for i, _ := range kv.cancelled {
		if ((i & mask) == mask) && i < cid {
			delete(kv.cancelled, i)
		}
	}
}
