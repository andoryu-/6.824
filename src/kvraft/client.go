package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"
import "log"
import "time"

var (
	_clerkId int32
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cidbase   uint64
	rqseq     uint32
	leaderIdx int
	me        int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) newid() uint64 {
	var n uint64 = uint64(atomic.AddUint32(&ck.rqseq, 1))
	log.Printf("[%d] newid() cidbase %v cid %v", ck.me, ck.cidbase, ck.cidbase|n)
	return (ck.cidbase | n)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = int(atomic.AddInt32(&_clerkId, 1))
	var j uint64 = uint64(nrand())
	var i uint64 = uint64(ck.me)
	ck.cidbase = (i + j<<16) << 32
	atomic.StoreUint32(&ck.rqseq, 0)
	ck.leaderIdx = 0
	log.Printf("[%d] MakeClerk(): cidbase %v clerk %v", ck.me, ck.cidbase, ck)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	cid := ck.newid()
	args := GetArgs{Key: key, Cid: cid}
	j := 0
	for i := ck.leaderIdx; i < len(ck.servers); i++ {
		var reply GetReply
		log.Printf("[%d] Clerk Call Get %v retry %d", ck.me, key, j)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader { // FIXME if !ok should we retry the same peer or retry another peer?
			ck.leaderIdx = i
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err != ErrNoKey {
				log.Printf("[%d] Get() err: %v", ck.me, reply.Err)
			}
			return ""
		}
		if i == len(ck.servers)-1 {
			i = -1
		}
		j++
		if j > 100 { // 1s ~= election timeout
			break
		}
		time.Sleep(time.Millisecond * time.Duration(100))
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	cid := ck.newid()
	args := PutAppendArgs{Key: key, Value: value, Op: op, Cid: cid}
	j := 0
	for i := ck.leaderIdx; i < len(ck.servers); i++ {
		var reply PutAppendReply
		log.Printf("[%d] Clerk Call PutAppend %v retry %d", ck.me, key, j)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader { // FIXME if !ok should we retry the same peer or retry another peer?
			ck.leaderIdx = i
			if reply.Err == OK {
				return
			}
			log.Printf("[%d] PutAppend() err: %v", ck.me, reply.Err)
			return
		}
		if i == len(ck.servers)-1 {
			i = -1
		}
		j++
		if j > 100 { // 1s ~= election timeout
			break
		}
		time.Sleep(time.Millisecond * time.Duration(100))
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
