package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"

import "fmt"
import "log"
import "runtime"
import "sync/atomic"

import "bytes"
import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int // according to raft paper, the first index is 1 not 0
}

const (
	kElectionTimeout        int = 0 // election timeout
	kHeartbeatTimeout       int = 1 // heartbeat timeout
	kRequestVoteDone        int = 2 // RequestVote() finished
	kAppendEntriesDone      int = 3 // AppendEntries() finished
	kAppendEntriesDoneFirst int = 4 // appended entries
	kOnVote                 int = 5 // voted for candidate
	kOnCommand              int = 6 // started client action
	kOnLeader               int = 7
	kOnFollower             int = 8
	kOnApply                int = 9
	kOnInstallSnapshot      int = 10
)

type Event struct {
	Type  int
	Args  interface{}
	Reply interface{}
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type LogEntryDescriptor struct {
	Term  int
	Index int
}

type Replicator struct {
	NextIndex  int
	MatchIndex int
	timer_     *TimedClosure
	ack_time_  time.Time // replicators_[me] holds the time when become leader
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	is_leader_    bool
	current_term_ int
	voted_for_    int
	wal_          []LogEntry // WAL
	commit_index_ int        // last commited log index
	apply_index_  int        // last applied log index
	events_       chan Event
	apply_chan_   chan ApplyMsg
	replicators_  []Replicator  // bookkeeping for replication
	timer_        *TimedClosure // timer for election
	events_flag   int32         // to sync `events_`
	events_open   bool          // whether `events_` is open
	ballot        map[int]bool  // ballot for RequestVote!
	ss_index      int           // last included index of snapshot
	ss_term       int           // last included term of snapshot
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term, isleader = rf.getState()
	rf.mu.Unlock()
	return term, isleader
}
func (rf *Raft) getState() (term int, isleader bool) {
	term = rf.current_term_
	isleader = rf.is_leader_
	npeers := len(rf.peers)
	now := time.Now()
	if !isleader {
		return
	}
	et := getElectionTimeout()
	if now.Sub(rf.replicators_[rf.me].ack_time_) >= et {
		connected := 0
		for i := 0; i < npeers; i++ {
			if i == rf.me {
				connected++
			} else if now.Sub(rf.replicators_[i].ack_time_) < getExpireTimeout() {
				connected++
			}
		}
		if connected <= (npeers - connected) {
			isleader = false
		}
		log.Printf("[%d] Raft.getState() election time %v connects %d", rf.me, rf.replicators_[rf.me].ack_time_, connected)
	} else {
		log.Printf("[%d] Raft.getState() election time %v too short", rf.me, rf.replicators_[rf.me].ack_time_)
	}
	return
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	log.Printf("[%d] Raft.persist() term %d votedFor %d lastIncluded %d #logs %d committed %d", rf.me, rf.current_term_, rf.voted_for_, rf.ss_index, len(rf.wal_), rf.commit_index_)
	rf.persister.SaveRaftState(rf.serialize())
}
func (rf *Raft) serialize() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.current_term_)
	e.Encode(rf.voted_for_)
	e.Encode(rf.wal_)
	e.Encode(rf.commit_index_)
	e.Encode(rf.apply_index_)
	e.Encode(rf.ss_index)
	e.Encode(rf.ss_term)
	return w.Bytes()
}
func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}
func (rf *Raft) SaveSnapshot(ss []byte, last_included int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.saveSnapshot(ss, last_included, -1)
}
func (rf *Raft) saveSnapshot(ss []byte, last_included int, last_included_term int) bool {
	log.Printf("[%d] Raft.SaveSnapshot() term %d votedFor %d committed %d ssindex %d lastIncluded %d #logs %d", rf.me, rf.current_term_, rf.voted_for_, rf.commit_index_, last_included, rf.ss_index, len(rf.wal_))
	if last_included < rf.ss_index {
		log.Printf("[%d] WARN invalid ssindex %d lastIncluded %d #logs %d", rf.me, last_included, rf.ss_index, len(rf.wal_))
		return false
	}
	if last_included_term > -1 {
		rf.ss_term = last_included_term
	} else if last_included < rf.dataEnd() {
		rf.ss_term = rf.wal(last_included).Term
	}
	logs := make([]LogEntry, 0, 1)
	for i := last_included + 1; i < rf.dataEnd(); i++ {
		logs = append(logs, rf.wal(i))
	}
	rf.wal_ = logs
	rf.ss_index = last_included
	rf.apply_index_ = last_included
	//rf.commit_index_ = rf.ss_index
	rfsize := rf.persister.RaftStateSize()
	rf.persister.SaveStateAndSnapshot(rf.serialize(), ss)
	log.Printf("[%d] after snapshot, RaftStateSize %d => %d", rf.me, rfsize, rf.persister.RaftStateSize())
	return true
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voted_for int
	var logs []LogEntry
	var committed int
	var applied int
	var ss_index int
	var ss_term int
	if d.Decode(&term) != nil {
		log.Fatal("decode term error")
	} else if d.Decode(&voted_for) != nil {
		log.Fatal("decode voted_for error")
	} else if d.Decode(&logs) != nil {
		log.Fatal("decode logs error")
	} else if d.Decode(&committed) != nil {
		log.Fatal("decode committed error")
	} else if d.Decode(&applied) != nil {
		log.Fatal("decode applied error")
	} else if d.Decode(&ss_index) != nil {
		log.Fatal("decode ss_index error")
	} else if d.Decode(&ss_term) != nil {
		log.Fatal("decode ss_term error")
	} else {
		rf.is_leader_ = false
		rf.current_term_ = term
		rf.voted_for_ = voted_for
		rf.wal_ = logs
		rf.commit_index_ = committed
		rf.apply_index_ = applied
		rf.ss_index = ss_index
		rf.ss_term = ss_term
		for i := 0; i < len(rf.peers); i++ {
			rf.ballot[i] = false
		}
	}
	log.Printf("[%d] Raft.readPersist() term %d votedFor %d #logs %d committed %d ssIdx %d ssTerm %d", rf.me, rf.current_term_, rf.voted_for_, len(rf.wal_), rf.commit_index_, rf.ss_index, rf.ss_term)
	return
}

func (rf *Raft) logsDescriptor() LogEntryDescriptor {
	var ld LogEntryDescriptor
	if rf.dataEnd() > 0 {
		if len(rf.wal_) > 0 {
			ld.Index = rf.dataEnd() - 1
			ld.Term = rf.wal_[len(rf.wal_)-1].Term
		} else {
			ld.Index = rf.ss_index
			ld.Term = rf.ss_term
		}
	} else {
		ld.Index = -1
		ld.Term = 0
	}
	return ld
}

func (rf *Raft) spinLock() {
	for {
		old := atomic.LoadInt32(&rf.events_flag)
		if old == 1 {
			runtime.Gosched()
			continue
		}
		if atomic.CompareAndSwapInt32(&rf.events_flag, old, 1) {
			break
		}
	}
}
func (rf *Raft) spinUnlock() {
	atomic.StoreInt32(&rf.events_flag, 0)
}

func (rf *Raft) notify(e *Event) {
	rf.spinLock()
	if rf.events_open {
		rf.events_ <- (*e)
	}
	rf.spinUnlock()
}

func (rf *Raft) stopElectionTimeout() {
	rf.timer_.Stop()
}

func (rf *Raft) stopHeartbeatTimeout() {
	for i := 0; i < len(rf.replicators_); i++ {
		rf.replicators_[i].timer_.Stop()
	}
}

func (rf *Raft) refreshElectionTimeout() {
	timeout := getElectionTimeout()
	DPrintf("[%d] refresh ElectionTimeout term=%d timeout=%v timer=%p", rf.me, rf.current_term_, timeout, rf.timer_)
	rf.timer_.DelayFor(timeout, func(args ...interface{}) {
		arg := args2Slice(args...)
		term := arg[0].(int)
		t := arg[1].(time.Time)
		e := &Event{kElectionTimeout, term, t}
		rf.notify(e)
	}, rf.current_term_, time.Now())
}

func (rf *Raft) refreshHeartbeatTimeout(server int, ts int) {
	if !rf.is_leader_ {
		return
	}
	var timeout time.Duration
	if ts <= 0 {
		timeout = getHeartbeatTimeout()
	} else {
		timeout = time.Duration(ts) * time.Millisecond
	}
	r := &rf.replicators_[server]
	DPrintf("[%d] refresh HeartbeatTimeout term=%d timeout=%v server=%d timer=%p", rf.me, rf.current_term_, timeout, server, r.timer_)
	r.timer_.DelayFor(timeout, func(args ...interface{}) {
		arg := args2Slice(args...)
		term := arg[0].(int)
		s := arg[1].(int)
		e := &Event{kHeartbeatTimeout, term, s}
		rf.notify(e)
	}, rf.current_term_, server)
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term    int
	GrpIdx  int
	LastLog LogEntryDescriptor
}
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	GrpIdx  int
	Approve bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf("[%d] RequestVote() args{Term:%d,GrpIdx:%d,LastLog:{Term:%d,Index:%d}}", rf.me, args.Term, args.GrpIdx, args.LastLog.Term, args.LastLog.Index)
	voted_for := rf.voted_for_
	defer func() {
		log.Printf("[%d] RequestVote() reply{Term:%d,Approve:%v} VotedFor:%d", rf.me, reply.Term, reply.Approve, voted_for)
	}()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.GrpIdx = rf.me
	if args.Term > rf.current_term_ {
		if rf.is_leader_ {
			log.Printf("[%d] RequestVote term %d > current term %d step down", rf.me, args.Term, rf.current_term_)
		}
		rf.current_term_ = args.Term // persist when term change?
		rf.voted_for_ = -1
		rf.stepDown()
	}
	reply.Term = rf.current_term_

	if args.Term < rf.current_term_ {
		reply.Approve = false
	} else if rf.voted_for_ < 0 || rf.voted_for_ == args.GrpIdx {
		ld := rf.logsDescriptor()
		reply.Approve = (args.LastLog.Compare(&ld) >= 0)
	} else {
		reply.Approve = false
	}

	if reply.Approve {
		rf.voted_for_ = args.GrpIdx
		defer rf.persist()
		rf.refreshElectionTimeout()
	}
	voted_for = rf.voted_for_
	return
}

type AppendEntriesArgs struct {
	Term         int
	GrpIdx       int
	PrevLog      LogEntryDescriptor
	Entries      []LogEntry
	LeaderCommit int
	Snapshot     []byte
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	GrpIdx        int
	ConflictTerm  int // if ConflictTerm != 0, leader find the first index with the same term in its log to update NextIndex
	ConflictIndex int // if ConflictTerm == 0, leader use ConflictIndex to update NextIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	has_snapshot := len(args.Snapshot) > 0
	log.Printf("[%d] AppendEntries() args{Term:%d,GrpIdx:%d,PrevLog:%v,Entries:%v,LeaderCommit:%d,Snapshot:%v}", rf.me, args.Term, args.GrpIdx, args.PrevLog, args.Entries, args.LeaderCommit, has_snapshot)
	voted_for := rf.voted_for_
	requestEnd := args.PrevLog.Index + 1 + len(args.Entries)
	defer func() {
		log.Printf("[%d] AppendEntries() reply{Term:%d,Success:%v,conflictIndex:%d,conflictTerm:%d} VotedFor %d", rf.me, reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm, voted_for)
	}()

	// lock against concurrency
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term, reply.Success, reply.GrpIdx = rf.current_term_, true, rf.me
	if args.Term < rf.current_term_ {
		reply.Success, reply.ConflictTerm, reply.ConflictIndex = false, -1, -1
		return
	}

	rf.voted_for_ = args.GrpIdx
	voted_for = rf.voted_for_
	if args.Term > rf.current_term_ {
		if rf.is_leader_ {
			log.Printf("[%d] AppendEntries term %d > current term %d step down", rf.me, args.Term, rf.current_term_)
		}
		rf.current_term_ = args.Term // persist when term changed?
		reply.Term = args.Term
		if !rf.stepDown() {
			rf.refreshElectionTimeout()
		}
	} else {
		rf.refreshElectionTimeout()
	}

	// check for gap
	if args.PrevLog.Index >= rf.dataEnd() && !has_snapshot {
		reply.Success, reply.ConflictTerm, reply.ConflictIndex = false, 0, rf.dataEnd()
		return
	}

	// check for conflict at PrevLog
	if args.PrevLog.Index < 0 {
		// pure heartbeat nothing to be done
	} else if args.PrevLog.Index < rf.ss_index {
		// log discared by snapshot, nothing to check
	} else if args.PrevLog.Index == rf.ss_index {
		if args.PrevLog.Term != rf.ss_term && !has_snapshot {
			log.Printf("[%d] WARN AppendEntriesArgs.PrevLog conflict with snapshot lastIncluded, discard snapshot", rf.me)
			reply.Success, reply.ConflictTerm, reply.ConflictIndex = false, rf.ss_term, rf.ss_index
			rf.truncateLogs(args.PrevLog.Index)
			rf.persist()
			return
		}
	} else if args.PrevLog.Index < rf.dataEnd() {
		if rf.wal(args.PrevLog.Index).Term != args.PrevLog.Term && !has_snapshot {
			conflict_term := rf.wal(args.PrevLog.Index).Term
			conflict_start_index := args.PrevLog.Index
			for i := args.PrevLog.Index - 1; i >= 0 && i > rf.ss_index; i-- {
				if rf.wal(i).Term == conflict_term {
					conflict_start_index = i
				} else {
					break
				}
			}
			reply.Success, reply.ConflictTerm, reply.ConflictIndex = false, conflict_term, conflict_start_index
			rf.truncateLogs(args.PrevLog.Index)
			rf.persist()
			return
		}
	}
	// maybe install snapshot
	if has_snapshot && rf.saveSnapshot(args.Snapshot, args.PrevLog.Index, args.PrevLog.Term) {
		rf.notify(&Event{kOnInstallSnapshot, ApplyMsg{false, args.Snapshot, args.PrevLog.Index + 1}, nil})
	}
	// check for gap again
	if rf.dataEnd() < args.PrevLog.Index+1 {
		reply.Success, reply.ConflictTerm, reply.ConflictIndex = false, 0, rf.dataEnd()
		return
	}
	need_persist := false
	// check for conflict between logs and AppendEntriesArgs.Entries
	for i := args.PrevLog.Index + 1; i < rf.dataEnd() && i < requestEnd; i++ {
		if i < rf.ss_index {
			continue
		}
		if rf.wal(i).Term != args.Entries[i-args.PrevLog.Index-1].Term {
			rf.truncateLogs(i)
			need_persist = true
			break
		}
	}
	// append any new entries
	if requestEnd > rf.dataEnd() {
		start := rf.dataEnd() - (args.PrevLog.Index + 1)
		rf.wal_ = append(rf.wal_, args.Entries[start:]...)
		need_persist = true
	}
	reply.Success, reply.ConflictTerm, reply.ConflictIndex = true, 0, rf.dataEnd()
	if rf.dataEnd() > requestEnd {
		log.Printf("[%d] dataEnd %d > requestEnd %d", rf.me, rf.dataEnd(), requestEnd)
		reply.ConflictIndex = requestEnd
	}
	new_commit := args.LeaderCommit
	if new_commit > rf.dataEnd()-1 {
		new_commit = rf.dataEnd() - 1
	}
	if rf.commit_index_ < new_commit {
		rf.commit_index_ = new_commit
		need_persist = true
	}
	if rf.apply_index_ < rf.ss_index {
		log.Fatalf("[%d] FATAL last applied %d < ssindex %d", rf.me, rf.apply_index_, rf.ss_index)
	}
	// commit entries
	if rf.apply_index_ < rf.commit_index_ {
		log.Printf("[%d] Apply index (%d, %d] according to leader %d", rf.me, rf.apply_index_, rf.commit_index_, args.GrpIdx)
		m := make([]ApplyMsg, 0, 1)
		for i := rf.apply_index_ + 1; i <= rf.commit_index_; i++ {
			m = append(m, ApplyMsg{true, rf.wal(i).Command, i + 1})
		}
		rf.apply_index_ = rf.commit_index_
		rf.notify(&Event{kOnApply, m, nil})
		need_persist = true
	}

	if need_persist {
		rf.persist()
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) bool {
	const kMethod string = "Raft.RequestVote"
	var reply RequestVoteReply
	ok := rf.peers[server].Call(kMethod, args, &reply)
	if !ok {
		log.Printf("[%d] rpc %s() to peer %d %v error", rf.me, kMethod, server, rf.peers[server])
	} else {
		rf.notify(&Event{kRequestVoteDone, *args, reply})
	}
	return ok
}

func (rf *Raft) IssueRequestVote(args *RequestVoteArgs) {
	DPrintf("[%d] IssueRequestVote(%v)", rf.me, *args)
	npeers := len(rf.peers)
	rf.ballot[rf.me] = true
	for server := 0; server < npeers; server++ {
		if server != rf.me {
			rf.ballot[server] = false
			go rf.sendRequestVote(server, args)
		}
	}
	rf.refreshElectionTimeout()
}

func (rf *Raft) IssueAppendEntries(server int, args *AppendEntriesArgs) bool {
	const kMethod string = "Raft.AppendEntries"
	log.Printf("[%d] %p IssueAppendEntries(%d) replicator%v dataEnd %d hb %v", rf.me, rf, server, rf.replicators_[server], rf.dataEnd(), len(args.Entries) == 0 && len(args.Snapshot) == 0)
	rf.refreshHeartbeatTimeout(server, -1)
	var reply AppendEntriesReply
	ok := rf.peers[server].Call(kMethod, args, &reply)

	if ok {
		rf.mu.Lock()
		if rf.replicators_[server].ack_time_.Before(rf.replicators_[rf.me].ack_time_) {
			rf.notify(&Event{kAppendEntriesDoneFirst, *args, reply})
		} else {
			rf.notify(&Event{kAppendEntriesDone, *args, reply})
		}
		rf.replicators_[server].ack_time_ = time.Now()
		rf.mu.Unlock()
	} else {
		log.Printf("[%d] %p rpc %s() to peer %d %v error", rf.me, rf, kMethod, server, rf.peers[server])
	}

	DPrintf("[%d] %p IssueAppendEntries(%d)={Term:%d,Success:%v,conflictIndex:%d,conflictTerm:%d} ok %v", rf.me, rf, server, reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm, ok)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := 0
	isleader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.is_leader_
	if !isleader {
		return index + 1, term, isleader
	}
	term = rf.current_term_
	rf.wal_ = append(rf.wal_, LogEntry{term, command})
	index = rf.dataEnd() - 1
	log.Printf("[%d] Raft.Start(%v) index %d", rf.me, command, index)
	rf.replicators_[rf.me].MatchIndex = index
	rf.replicators_[rf.me].NextIndex = index + 1
	for i := 0; i < len(rf.replicators_); i++ {
		if i != rf.me {
			rf.refreshHeartbeatTimeout(i, 10)
		}
	}
	return index + 1, term, isleader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	log.Printf("[%d] Raft.Kill(%p)", rf.me, rf)
	rf.stopElectionTimeout()
	rf.stopHeartbeatTimeout()
	time.Sleep(time.Millisecond * time.Duration(10))
	rf.spinLock()
	rf.events_open = false
	close(rf.events_)
	rf.spinUnlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.apply_chan_ = applyCh
	rf.voted_for_ = -1
	rf.current_term_ = 0
	rf.is_leader_ = false
	rf.wal_ = make([]LogEntry, 0, 1)
	rf.commit_index_ = -1
	rf.apply_index_ = -1
	rf.events_ = make(chan Event, 4096)
	rf.events_flag = 0
	rf.events_open = true
	rf.replicators_ = make([]Replicator, len(peers))
	rf.timer_ = &TimedClosure{}
	rf.refreshElectionTimeout()
	rf.ballot = make(map[int]bool)
	rf.ss_index = -1
	rf.ss_term = 0

	rf.readPersist(persister.ReadRaftState())
	for i := 0; i < len(peers); i++ {
		rf.replicators_ = append(rf.replicators_, Replicator{rf.dataEnd(), -1, &TimedClosure{}})
	}

	go rf.Run()
	log.Printf("[%d] Raft.Make(peers=%v,me=%d) done, about to run", rf.me, peers, rf.me)
	return rf
}

func (rf *Raft) GetLogs() ([]LogEntry, int) {
	return rf.wal_, rf.commit_index_
}

func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.snapshot
}

func (rf *Raft) stepUp() bool {
	if rf.is_leader_ {
		DPrintf("[%d] ignore stale RequestVote approve as leader", rf.me)
		return false
	} else {
		log.Printf("[%d] StepUp as leader", rf.me)
		rf.is_leader_ = true
		rf.replicators_[rf.me].ack_time_ = time.Now()
		for i := 0; i < len(rf.replicators_); i++ {
			rf.replicators_[i].NextIndex = rf.dataEnd()
			//rf.replicators_[i].MatchIndex = -1
			rf.refreshHeartbeatTimeout(i, 10)
		}
		rf.mu.Unlock()
		rf.notify(&Event{kOnLeader, nil, nil})
		rf.mu.Lock()
		return true
	}
}

func (rf *Raft) stepDown() bool {
	if rf.is_leader_ {
		log.Printf("[%d] StepDown to follower", rf.me)
		rf.is_leader_ = false
		rf.refreshElectionTimeout()
		for i := 0; i < len(rf.replicators_); i++ {
			rf.replicators_[i].timer_.Stop()
		}
		rf.mu.Unlock()
		rf.notify(&Event{kOnFollower, nil, nil})
		rf.mu.Lock()
		return true
	} else {
		return false
	}
}

//
// Main event loop
//
func (rf *Raft) Run() {
	for e := range rf.events_ {
		switch e.Type {
		case kOnLeader:
			rf.apply_chan_ <- ApplyMsg{false, true, -2}
		case kOnFollower:
			rf.apply_chan_ <- ApplyMsg{false, false, -2}
		case kOnInstallSnapshot:
			msg := e.Args.(ApplyMsg)
			rf.apply_chan_ <- msg
		case kOnApply:
			m := e.Args.([]ApplyMsg)
			for _, msg := range m {
				rf.apply_chan_ <- msg
			}
		case kElectionTimeout:
			term := e.Args.(int)
			rf.mu.Lock()
			DPrintf("[%d] kElectionTimeout term=%d %v", rf.me, rf.current_term_, e)
			if rf.is_leader_ {
				DPrintf("[%d] ignore stale election timeout as leader", rf.me)
			} else {
				log.Printf("[%d] vote for self term=%d current term=%d", rf.me, term, rf.current_term_)
				rf.current_term_ += 1
				rf.voted_for_ = rf.me
				rf.persist()
				args := &RequestVoteArgs{rf.current_term_, rf.voted_for_, rf.logsDescriptor()}
				rf.IssueRequestVote(args)
			}
			rf.mu.Unlock()
		case kRequestVoteDone:
			args := e.Args.(RequestVoteArgs)
			reply := e.Reply.(RequestVoteReply)
			server := reply.GrpIdx
			rf.mu.Lock()
			log.Printf("[%d] kRequestVoteDone term=%d %v", rf.me, rf.current_term_, e)
			if args.Term != rf.current_term_ {
				DPrintf("[%d] ignore stale RequestVote reply: term=%d currentTerm=%d", rf.me, args.Term, rf.current_term_)
			} else {
				votes := 0
				rf.ballot[server] = reply.Approve
				if reply.Term > rf.current_term_ {
					log.Printf("[%d] RequestVote reply term %d > current term %d need to step down", rf.me, reply.Term, rf.current_term_)
					rf.current_term_ = reply.Term
					rf.voted_for_ = -1
					rf.stepDown()
					rf.persist()
				} else {
					votes = rf.countVotes()
				}
				if votes > len(rf.peers)-votes {
					rf.stepUp()
				}
			}
			rf.mu.Unlock()
		case kHeartbeatTimeout:
			term := e.Args.(int)
			server := e.Reply.(int)
			DPrintf("[%d] kHeartbeatTimeout %v", rf.me, e)
			rf.mu.Lock()
			if term != rf.current_term_ {
				DPrintf("[%d] ignore stale heartbeat timeout: term=%d currentTerm=%d", rf.me, term, rf.current_term_)
			} else if !rf.is_leader_ {
				DPrintf("[%d] ignore stale heartbeat timeout: not leader", rf.me)
			} else if server == rf.me { // check active step down
				if _, isleader := rf.getState(); !isleader {
					log.Printf("[%d] unable to connect to quorum, step down. term=%d", rf.me, term)
					rf.stepDown()
				} else {
					rf.refreshHeartbeatTimeout(server, -1)
				}
			} else {
				next := rf.replicators_[server].NextIndex
				args := AppendEntriesArgs{Term: rf.current_term_, GrpIdx: rf.me, PrevLog: LogEntryDescriptor{0, -1}, LeaderCommit: rf.commit_index_}
				if next > rf.ss_index+1 {
					args.PrevLog.Index = next - 1
					args.PrevLog.Term = rf.wal(args.PrevLog.Index).Term
				} else {
					args.PrevLog.Index = rf.ss_index
					args.PrevLog.Term = rf.ss_term
				}
				if next < rf.ss_index+1 { // attach snapshot
					args.Snapshot = make([]byte, len(rf.persister.snapshot))
					copy(args.Snapshot, rf.persister.snapshot)
					args.Entries = make([]LogEntry, len(rf.wal_))
					copy(args.Entries, rf.wal_)
				} else if next < rf.dataEnd() {
					args.Entries = make([]LogEntry, rf.dataEnd()-next)
					copy(args.Entries, rf.subLogs(next))
				} else if next > rf.dataEnd() {
					panic(fmt.Sprintf("error prepare AppendEntries, NextIndex %d > dataEnd  %d", next, rf.dataEnd()))
				}
				go rf.IssueAppendEntries(server, &args)
			}
			rf.mu.Unlock()
		case kAppendEntriesDoneFirst:
			fallthrough
		case kAppendEntriesDone:
			args := e.Args.(AppendEntriesArgs)
			reply := e.Reply.(AppendEntriesReply)
			server := reply.GrpIdx
			log.Printf("[%d] kAppendEntriesDone {%d %v %d %d} %v", rf.me, args.Term, args.PrevLog, len(args.Entries), args.LeaderCommit, reply)
			rf.mu.Lock()
			if !rf.is_leader_ {
				DPrintf("[%d] ignore stale AppendEntries callback as follower", rf.me)
				rf.mu.Unlock()
				break
			}
			if args.Term < rf.current_term_ {
				DPrintf("[%d] ignore stale AppendEntries callback: term=%d currentTerm=%d", rf.me, args.Term, rf.current_term_)
				rf.mu.Unlock()
				break
			}
			if reply.Term > rf.current_term_ {
				log.Printf("[%d] AppendEntires reply term %d > current term %d need to step down", rf.me, reply.Term, rf.current_term_)
				rf.current_term_ = reply.Term
				rf.voted_for_ = -1
				rf.stepDown()
				rf.persist()
			}
			if !rf.is_leader_ {
				rf.mu.Unlock()
				break
			}
			r := &rf.replicators_[server]
			vote := 0
			new_commit := -1
			need_persist := false
			if reply.Success {
				if reply.ConflictIndex > r.NextIndex {
					r.NextIndex, r.MatchIndex = reply.ConflictIndex, reply.ConflictIndex-1
				}
				min, max := rf.matchRange()
				for i := max; i >= min && i > rf.ss_index; i-- {
					vote = rf.countEntryAck(i)
					if vote > len(rf.replicators_)-vote && rf.wal(i).Term == rf.current_term_ {
						// ACK'ed by majority and in current term
						new_commit = i
						break
					}
				}
				if r.NextIndex < rf.dataEnd() {
					rf.refreshHeartbeatTimeout(server, 10)
				}
			} else if reply.ConflictTerm == -1 && reply.ConflictIndex == -1 {
				log.Printf("[%d] WARN AppendEntries error, args term %d reply term %d", rf.me, args.Term, reply.Term)
				r.NextIndex, r.MatchIndex = rf.dataEnd(), -1
			} else {
				if r.NextIndex > reply.ConflictIndex && e.Type != kAppendEntriesDoneFirst {
					log.Printf("[%d] WARN NextIndex %d > reply %d", rf.me, r.NextIndex, reply.ConflictIndex)
				}
				r.NextIndex = reply.ConflictIndex
				if reply.ConflictTerm != 0 {
					for i := rf.dataEnd() - 1; i > rf.ss_index; i-- {
						if rf.wal(i).Term == reply.ConflictTerm {
							r.NextIndex = i
							break
						}
					}
				}
				rf.refreshHeartbeatTimeout(server, 10)
			}
			if rf.commit_index_ < new_commit {
				rf.commit_index_ = new_commit
				need_persist = true
			}
			m := make([]ApplyMsg, 0, 1)
			if rf.apply_index_ < rf.commit_index_ {
				log.Printf("[%d] Apply index (%d, %d] as leader pro %d con %d", rf.me, rf.apply_index_, rf.commit_index_, vote, len(rf.replicators_)-vote)
				for i := rf.apply_index_ + 1; i <= rf.commit_index_; i++ {
					m = append(m, ApplyMsg{true, rf.wal(i).Command, i + 1})
				}
				rf.apply_index_ = rf.commit_index_
				need_persist = true
			}
			if need_persist {
				rf.persist()
			}
			rf.mu.Unlock()

			for _, msg := range m {
				rf.apply_chan_ <- msg
			}
		}
	}
	return
}

func (rf *Raft) wal(index int) LogEntry {
	if index < rf.ss_index {
		log.Printf("[%d] WARN wal()=nil index %d < lastIncluded %d #logs %d", rf.me, index, rf.ss_index, len(rf.wal_))
		return LogEntry{0, nil}
	} else if index == rf.ss_index {
		log.Printf("[%d] WARN wal()=nil index %d = lastIncluded %d #logs %d", rf.me, index, rf.ss_index, len(rf.wal_))
		return LogEntry{rf.ss_term, nil}
	} else {
		return rf.wal_[index-rf.ss_index-1]
	}
}

func (rf *Raft) truncateLogs(index int) {
	if index < rf.ss_index+1 {
		rf.wal_ = make([]LogEntry, 0, 1)
	} else {
		rf.wal_ = rf.wal_[:index-rf.ss_index-1]
	}
	return
}

func (rf *Raft) subLogs(index int) []LogEntry {
	return rf.wal_[index-rf.ss_index-1:]
}

func (rf *Raft) dataEnd() int {
	return rf.ss_index + 1 + len(rf.wal_)
}

func (rf *Raft) countVotes() (votes int) {
	for _, pro := range rf.ballot {
		if pro {
			votes++
		}
	}
	return
}

func (rf *Raft) countEntryAck(i int) (ack int) {
	// NOTE replicators_[me] also taken into account
	for j := 0; j < len(rf.replicators_); j++ {
		if rf.replicators_[j].MatchIndex >= i {
			ack++
		}
	}
	return
}
func (rf *Raft) matchRange() (least_match, max_match int) {
	npeers := len(rf.replicators_)
	if npeers < 1 {
		least_match, max_match = 0, -1
		return
	}
	least_match = rf.replicators_[0].MatchIndex
	max_match = rf.replicators_[0].MatchIndex
	for i := 1; i < npeers; i++ {
		if rf.replicators_[i].MatchIndex < least_match {
			least_match = rf.replicators_[i].MatchIndex
		}
		if rf.replicators_[i].MatchIndex > max_match {
			max_match = rf.replicators_[i].MatchIndex
		}
	}
	end := least_match
	if end < rf.commit_index_ {
		end = rf.commit_index_
	}
	if end < 0 {
		end = 0
	}
	least_match = end
	return
}

func (ld *LogEntryDescriptor) Compare(other *LogEntryDescriptor) int {
	cmp := ld.Term - other.Term
	if cmp != 0 {
		return cmp
	}
	return ld.Index - other.Index
}

func getExpireTimeout() time.Duration {
	timeout := 600
	return time.Millisecond * time.Duration(timeout)
}

// get random timeout in [200, 300] ms
func getElectionTimeout() time.Duration {
	timeout := 200 + rand.Intn(100)
	return time.Millisecond * time.Duration(timeout)
}

func getHeartbeatTimeout() time.Duration {
	return time.Millisecond * time.Duration(100)
}

func args2Slice(args ...interface{}) []interface{} {
	var arg []interface{}
	return append(arg, args...)
}
