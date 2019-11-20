package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
    "fmt"
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
    Type int
    Key string
    Value string
    Cid uint64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    pendings map[uint64]interface{} // map cid to pending requests
    staged  map[uint64]interface{} // map cid to reply
    cv      *sync.Cond
    data    map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    cid := args.Cid
    kv.mu.Lock()
    defer kv.mu.Unlock()

    kv.purgeStaged(cid)
    if _, ok := kv.staged[cid]; ok {
        *reply = kv.staged[cid].(GetReply)
        return
    }
    op := Op{Type: kGet, Key: args.Key, Cid: cid}
    if _, ok := kv.pendings[cid]; !ok {
        if _, _, leading := kv.rf.Start(op); leading {
            // currently we only maintain pending request to detect implementation errors, such as lost notification
            // if there're too many spurious wakeups, we might use pendings[cid] to store separate condvar
            kv.pendings[cid] = op
        } else {
            reply.WrongLeader = true
            return
        }
    }

    for _, ok := kv.pendings[cid]; ok {
        kv.cv.Wait()
    }

    if i, ok := kv.staged[cid]; !ok {
        reply.WrongLeader = true
    } else {
        *reply, ok = i.(GetReply)
        if !ok {
            panic(fmt.Sprintf("Cid %d supposed to be a GET, yet cast failed", cid))
        }
    }
    return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    cid := args.Cid
    kv.mu.Lock()
    defer kv.mu.Unlock()

    kv.purgeStaged(cid)
    if _, ok := kv.staged[cid]; ok {
        *reply = kv.staged[cid].(PutAppendReply)
        return
    }
    t := kPut
    if args.Op == "Append" {
        t = kAppend
    }
    op := Op{Type: t, Key: args.Key, Value: args.Value, Cid: cid}
    if _, ok := kv.pendings[cid]; !ok {
        if _, _, leading := kv.rf.Start(op); leading {
            // currently we only maintain pending request to detect implementation errors, such as lost notification
            // if there're too many spurious wakeups, we might use pendings[cid] to store separate condvar
            kv.pendings[cid] = op
        } else {
            reply.WrongLeader = true
            return
        }
    }

    for _, ok := kv.pendings[cid]; ok {
        kv.cv.Wait()
    }

    if i, ok := kv.staged[cid]; !ok {
        reply.WrongLeader = true
    } else {
        *reply, ok = i.(PutAppendReply)
        if !ok {
            panic(fmt.Sprintf("Cid %d supposed to be Put/Append, yet cast failed", cid))
        }
    }
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
    go kv.Run()
	return kv
}

func (kv *KVServer) Run() {
    for msg := range kv.applyCh {
        if msg.CommandValid {
            // Get/Put/Append
            op := msg.Command.(Op)
            kv.data[op.Key] = op.Value
            kv.mu.Lock()
            if _, ok := kv.pendings[op.Cid]; ok {
                if op.Type == kGet {
                    if v, ok := kv.data[op.Key]; ok {
                        kv.staged[op.Cid] = GetReply{WrongLeader: false, Err: OK, Value: v}
                    } else {
                        kv.staged[op.Cid] = GetReply{WrongLeader: false, Err: ErrNoKey}
                    }
                } else if op.Type == kPut {
                    kv.data[op.Key] = op.Value
                    kv.staged[op.Cid] = PutAppendReply{WrongLeader: false, Err: OK}
                } else if op.Type == kAppend {
                    if _, ok = kv.data[op.Key]; ok {
                        kv.data[op.Key] += op.Value
                    } else {
                        kv.data[op.Key] = op.Value
                    }
                    kv.staged[op.Cid] = PutAppendReply{WrongLeader: false, Err: OK}
                }
                delete(kv.pendings, op.Cid)
                kv.cv.Broadcast()
            }
            kv.mu.Unlock()
        } else if msg.Command == nil {
            // clear pendings
            kv.mu.Lock()
            n := len(kv.pendings)
            for cid, o := range kv.pendings {
                //if o.Type == kGet {
                //    kv.staged[cid] = GetReply{WrongLeader: True}
                //    delete(kv.pendings, cid)
                //} else if o.Type == kPut || o.Type == kAppend {
                //    kv.staged[cid] = PutAppendReply{WrongLeader: True}
                //    delete(kv.pendings, cid)
                //}
                delete(kv.pendings, cid)
                delete(kv.staged, cid)
            }
            if n > 0 {
                kv.cv.Broadcast()
            }
            kv.mu.Unlock()
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

