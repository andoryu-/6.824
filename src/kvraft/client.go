package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"

var (
    _clerkId int32
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    cidbase uint64
    rqseq   uint32
    leaderIdx int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (c *Clerk) newid() uint64 {
    var n uint64 = atomic.AddUint32(&c.rqseq, 1)
    return (c.cidbase | n)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
    var j uint64 = nrand()
    var i uint64 = atomic.AddInt32(&_clerkId, 1)
    ck.cidbase = (i + j << 16) << 32
    atomic.StoreUint32(&ck.rqseq, 0)
    ck.leaderIdx = -1
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
    var reply GetReply
    for peer := range ck.servers {
        ok := peer.Call("KVServer.Get", &args, &reply)
        if ok && !reply.WrongLeader {
            if reply.Err == OK {
                return reply.Value
            }
            break
        }
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
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
