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
	CommandIndex int
}

const (
	kElectionTimeout   int = 0 // election timeout
	kHeartbeatTimeout  int = 1 // heartbeat timeout
	kRequestVoteDone   int = 2 // RequestVote() finished
	kAppendEntriesDone int = 3 // AppendEntries() finished
	kOnVote            int = 4 // voted for candidate
	kOnEntries         int = 5 // appended entries
	kOnCommand         int = 6 // started client action
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
	ack_time_  time.Time
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

	is_leader_    bool       // whether this believes it is the leader
	current_term_ int        // persisted current term
	voted_for_    int        // persisted candidate for which voted during *current* term
	data_         []LogEntry // [0, last_applied_] in persister, (last_applied_, len(data_) - 1] in WAL.
	//last_applied_ int        // the index of the last applied log entry, *must* persist.
	commit_index_ int // the index of the last commited log entry, could be infered from scratch(starting from 0).
	events_       chan Event
	apply_chan_   chan ApplyMsg
	replicators_  []Replicator  // replicators
	timer_        *TimedClosure // Timer for election/heartbeat
	events_flag   int32
	events_open   bool
	ballot        map[int]bool
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
	if isleader && now.Sub(rf.replicators_[rf.me].ack_time_) >= getElectionTimeout() {
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
	log.Printf("[%d] Raft.persist() term %d votedFor %d len(logs) %d committed %d", rf.me, rf.current_term_, rf.voted_for_, len(rf.data_), rf.commit_index_)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.current_term_)
	e.Encode(rf.voted_for_)
	e.Encode(rf.data_)
	e.Encode(rf.commit_index_)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	if d.Decode(&term) != nil {
		log.Fatal("decode term error")
	} else if d.Decode(&voted_for) != nil {
		log.Fatal("decode voted_for error")
	} else if d.Decode(&logs) != nil {
		log.Fatal("decode logs error")
	} else if d.Decode(&committed) != nil {
		log.Fatal("decode committed error")
	} else {
		rf.is_leader_ = false
		rf.current_term_ = term
		rf.voted_for_ = voted_for
		rf.data_ = logs
		rf.commit_index_ = committed
		rf.refreshElectionTimeout()
		for i := 0; i < len(rf.replicators_); i++ {
			rf.replicators_[i].timer_.Stop()
			rf.ballot[i] = false
		}
	}
	log.Printf("[%d] Raft.readPersist() term %d votedFor %d len(logs) %d committed %d", rf.me, rf.current_term_, rf.voted_for_, len(rf.data_), rf.commit_index_)
	return
}

func (rf *Raft) logsDescriptor() LogEntryDescriptor {
	var ld LogEntryDescriptor
	if rf.data_ != nil && len(rf.data_) > 0 {
		ld.Index = len(rf.data_) - 1
		ld.Term = rf.data_[len(rf.data_)-1].Term
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
	log.Printf("[%d] refresh ElectionTimeout term=%d timeout=%v timer=%p", rf.me, rf.current_term_, timeout, rf.timer_)
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
	log.Printf("[%d] refresh HeartbeatTimeout term=%d timeout=%v server=%d timer=%p", rf.me, rf.current_term_, timeout, server, r.timer_)
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
	log.Printf("[%d] AppendEntries() args{Term:%d,GrpIdx:%d,PrevLog:%v,Entries:%v,LeaderCommit:%d}", rf.me, args.Term, args.GrpIdx, args.PrevLog, args.Entries, args.LeaderCommit)
	voted_for := rf.voted_for_
	defer func() {
		log.Printf("[%d] AppendEntries() reply{Term:%d,Success:%v,conflictIndex:%d,conflictTerm:%d} VotedFor %d", rf.me, reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm, voted_for)
	}()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term, reply.Success, reply.GrpIdx = rf.current_term_, true, rf.me
	if args.Term < rf.current_term_ {
		reply.Success, reply.ConflictTerm, reply.ConflictIndex = false, -1, -1
		return
	}

	if args.Term > rf.current_term_ {
		if rf.is_leader_ {
			log.Printf("[%d] AppendEntries term %d > current term %d step down", rf.me, args.Term, rf.current_term_)
		}
		rf.current_term_ = args.Term // persist when term changed?
		rf.voted_for_ = args.GrpIdx
		reply.Term = args.Term
		if !rf.stepDown() {
			rf.refreshElectionTimeout()
		}
	} else {
		rf.refreshElectionTimeout()
	}

	// check for gap
	if args.PrevLog.Index >= len(rf.data_) {
		reply.Success, reply.ConflictTerm, reply.ConflictIndex = false, 0, len(rf.data_)
		return
	}
	voted_for = rf.voted_for_
	// check for conflict at PrevLog
	if args.PrevLog.Index >= 0 && rf.data_[args.PrevLog.Index].Term != args.PrevLog.Term {
		conflict_term := rf.data_[args.PrevLog.Index].Term
		conflict_start_index := args.PrevLog.Index
		for i := args.PrevLog.Index - 1; i >= 0; i-- {
			if rf.data_[i].Term == conflict_term {
				conflict_start_index = i
			} else {
				break
			}
		}
		rf.data_ = rf.data_[:args.PrevLog.Index]
		reply.Success, reply.ConflictTerm, reply.ConflictIndex = false, conflict_term, conflict_start_index
		return
	}
	if len(rf.data_) < args.PrevLog.Index+1 {
		reply.Success, reply.ConflictTerm, reply.ConflictIndex = false, 0, len(rf.data_)
		return
	}
	// check for conflict between logs and AppendEntriesArgs.Entries
	for i := args.PrevLog.Index + 1; i < len(rf.data_) && i < args.PrevLog.Index+1+len(args.Entries); i++ {
		if rf.data_[i].Term != args.Entries[i-args.PrevLog.Index-1].Term {
			rf.data_ = rf.data_[:i]
			break
		}
		//rf.data_[i] = args.Entries[i-args.PrevLog.Index-1] // overwrite logs with Entries
	}
	// append any new entries
	if args.PrevLog.Index+len(args.Entries) > len(rf.data_)-1 {
		start := len(rf.data_) - args.PrevLog.Index - 1
		rf.data_ = append(rf.data_, args.Entries[start:]...)
	}
	//if rf.commit_index_ > args.PrevLog.Index+1+len(args.Entries) {
	//	panic(fmt.Sprintf("commit %d > requestEnd %d", rf.commit_index_, args.PrevLog.Index+1+len(args.Entries)))
	//}
	//if len(rf.data_) > args.PrevLog.Index+1+len(args.Entries) {
	//	rf.data_ = rf.data_[:args.PrevLog.Index+1+len(args.Entries)]
	//}
	reply.Success, reply.ConflictTerm, reply.ConflictIndex = true, 0, len(rf.data_)
	if len(rf.data_) > args.PrevLog.Index+1+len(args.Entries) {
		log.Printf("[%d] len(logs) %d > requestEnd %d", rf.me, len(rf.data_), args.PrevLog.Index+1+len(args.Entries))
		reply.ConflictIndex = args.PrevLog.Index + 1 + len(args.Entries)
	}
	end := args.LeaderCommit
	if end > len(rf.data_)-1 {
		end = len(rf.data_) - 1
	}
	// commit entries
	for i := rf.commit_index_ + 1; i <= end; i++ {
		rf.apply_chan_ <- ApplyMsg{true, rf.data_[i].Command, i + 1} // according to raft paper, the first index is 1 not 0
	}
	if rf.commit_index_ < end {
		log.Printf("[%d] Apply index (%d, %d] according to leader %d", rf.me, rf.commit_index_, end, args.GrpIdx)
		rf.commit_index_ = end
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
	log.Printf("[%d] IssueRequestVote(%v)", rf.me, *args)
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
	var reply AppendEntriesReply
	ok := rf.peers[server].Call(kMethod, args, &reply)

	rf.mu.Lock()
	if ok {
		rf.replicators_[server].ack_time_ = time.Now()
	} else {
		log.Printf("[%d] %p rpc %s() to peer %d %v error", rf.me, rf, kMethod, server, rf.peers[server])
	}
	rf.mu.Unlock()

	if ok {
		rf.notify(&Event{kAppendEntriesDone, *args, reply})
	}
	log.Printf("[%d] %p IssueAppendEntries(%d)={Term:%d,Success:%v,conflictIndex:%d,conflictTerm:%d} ok %v", rf.me, rf, server, reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm, ok)
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
		return index, term, isleader
	}
	term = rf.current_term_
	rf.data_ = append(rf.data_, LogEntry{term, command})
	index = len(rf.data_) - 1
	log.Printf("[%d] Raft.Start(%v) index %d", rf.me, command, index)
	// also update replicators[me] here
	rf.replicators_[rf.me].MatchIndex = index
	rf.replicators_[rf.me].NextIndex = index + 1
	for i := 0; i < len(rf.replicators_); i++ {
		if i != rf.me {
			rf.refreshHeartbeatTimeout(i, 1)
		}
	}
	return index + 1, term, isleader // according to raft paper, the first index is 1 not 0
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
	rf.data_ = make([]LogEntry, 0, 1)
	//rf.last_applied_ = -1
	rf.commit_index_ = -1
	rf.events_ = make(chan Event)
	rf.events_flag = 0
	rf.events_open = true
	// initialize replicators
	rf.replicators_ = make([]Replicator, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.replicators_[i].timer_ = &TimedClosure{}
		rf.replicators_[i].NextIndex = 0
		rf.replicators_[i].MatchIndex = -1
	}
	// start election timer
	rf.timer_ = &TimedClosure{}
	rf.refreshElectionTimeout()
	rf.ballot = make(map[int]bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	log.Printf("[%d] Raft.Make(peers=%v,me=%d) done, about to run", rf.me, peers, rf.me)

	// start event loop
	go rf.Run()

	return rf
}

func (rf *Raft) stepUp() bool {
	if rf.is_leader_ {
		log.Printf("[%d] ignore stale RequestVote approve as leader", rf.me)
		return false
	} else {
		log.Printf("[%d] StepUp as leader", rf.me)
		rf.is_leader_ = true
		rf.replicators_[rf.me].ack_time_ = time.Now()
		// update replicators & timer
		for i := 0; i < len(rf.replicators_); i++ {
			//rf.replicators_[i].NextIndex = len(rf.data_)
			//rf.replicators_[i].MatchIndex = -1
			rf.refreshHeartbeatTimeout(i, 1)
		}
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
		case kElectionTimeout:
			term := e.Args.(int)
			rf.mu.Lock()
			log.Printf("[%d] kElectionTimeout term=%d %v", rf.me, rf.current_term_, e)
			if rf.is_leader_ {
				log.Printf("[%d] ignore stale election timeout as leader", rf.me)
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
				log.Printf("[%d] ignore stale RequestVote reply: term=%d currentTerm=%d", rf.me, args.Term, rf.current_term_)
			} else {
				votes := 0
				rf.ballot[server] = reply.Approve
				if reply.Term > rf.current_term_ {
					log.Printf("[%d] vote response bigger term=%d currentTerm=%d need to step down", rf.me, reply.Term, rf.current_term_)
					rf.current_term_ = reply.Term
					rf.voted_for_ = -1
					rf.stepDown()
					rf.persist()
				} else {
					for _, pro := range rf.ballot {
						if pro {
							votes++
						}
					}
				}
				if votes > len(rf.peers)-votes {
					rf.stepUp()
				}
			}
			rf.mu.Unlock()
		case kHeartbeatTimeout:
			log.Printf("[%d] kHeartbeatTimeout %v", rf.me, e)
			term := e.Args.(int)
			server := e.Reply.(int)
			rf.mu.Lock()
			if term != rf.current_term_ {
				log.Printf("[%d] ignore stale heartbeat timeout: term=%d currentTerm=%d", rf.me, term, rf.current_term_)
			} else if !rf.is_leader_ {
				log.Printf("[%d] ignore stale heartbeat timeout: not leader", rf.me)
			} else if server == rf.me {
				// we use replicator[me] timer to check whether we should actively step down due to network partition
				if _, isleader := rf.getState(); !isleader {
					log.Printf("[%d] unable to connect to quorum, step down. term=%d", rf.me, term)
					rf.stepDown()
				} else {
					rf.refreshHeartbeatTimeout(server, -1)
				}
			} else {
				args := AppendEntriesArgs{Term: rf.current_term_, GrpIdx: rf.me, PrevLog: LogEntryDescriptor{0, -1}, LeaderCommit: rf.commit_index_}
				if rf.replicators_[server].NextIndex > 0 {
					args.PrevLog.Index = rf.replicators_[server].NextIndex - 1
					args.PrevLog.Term = rf.data_[args.PrevLog.Index].Term
				}
				args.Entries = rf.data_[rf.replicators_[server].NextIndex:]
				log.Printf("[%d] %p IssueAppendEntries(%d) replicator%v len(logs) %d is_hb %v", rf.me, rf, server, rf.replicators_[server], len(rf.data_), len(args.Entries) == 0)
				rf.refreshHeartbeatTimeout(server, -1)
				rf.mu.Unlock()
				go rf.IssueAppendEntries(server, &args)
				break
			}
			rf.mu.Unlock()
		case kAppendEntriesDone:
			rf.mu.Lock()
			log.Printf("[%d] kAppendEntriesDone %v", rf.me, e)
			args := e.Args.(AppendEntriesArgs)
			term := args.Term
			reply := e.Reply.(AppendEntriesReply)
			server := reply.GrpIdx
			if len(rf.replicators_) <= server {
				panic(fmt.Sprintf("[%d] len(replicators) %d <= server %d", rf.me, len(rf.replicators_), server))
			}
			if !rf.is_leader_ {
				log.Printf("[%d] ignore stale AppendEntries callback as follower", rf.me)
				rf.mu.Unlock()
				break
			}
			if term < rf.current_term_ {
				log.Printf("[%d] ignore stale AppendEntries callback: term=%d currentTerm=%d", rf.me, term, rf.current_term_)
				rf.mu.Unlock()
				break
			}
			if reply.Term > rf.current_term_ {
				log.Printf("[%d] step down receiving AppendEntries reply: term=%d currentTerm=%d", rf.me, reply.Term, rf.current_term_)
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
			if reply.Success {
				r.NextIndex, r.MatchIndex = reply.ConflictIndex, reply.ConflictIndex-1
				min, max := rf.matchRange()
				// NOTE replicators_[me] also taken into account
				for i := max; i >= min; i-- {
					vote := rf.logReplicas(i)
					if vote > len(rf.replicators_)-vote && rf.data_[i].Term == rf.current_term_ {
						for j := rf.commit_index_ + 1; j <= i; j++ {
							rf.apply_chan_ <- ApplyMsg{true, rf.data_[j].Command, j + 1} // according to raft paper, the first index is 1 not 0
						}
						if rf.commit_index_ < i {
							log.Printf("[%d] Apply index (%d, %d] as leader pro %d con %d", rf.me, rf.commit_index_, i, vote, len(rf.replicators_)-vote)
							rf.commit_index_ = i
							rf.persist()
						}
						break
					}
				}
				if r.NextIndex < len(rf.data_) {
					rf.refreshHeartbeatTimeout(server, 10)
				}
			} else if reply.ConflictTerm == -1 && reply.ConflictIndex == -1 {
				log.Printf("[%d] AppendEntries error, args term %d reply term %d", rf.me, args.Term, reply.Term)
				r.NextIndex, r.MatchIndex = len(rf.data_), -1 // reset
			} else {
				var found bool
				if reply.ConflictTerm != 0 {
                    for i := len(rf.data_) - 1; i >= 0; i-- {
					//for i := 0; i < len(rf.data_); i++ {
						if rf.data_[i].Term == reply.ConflictTerm {
							r.NextIndex = i
							found = true
							break
						}
					}
				}
				if !found {
					r.NextIndex = reply.ConflictIndex
				}
				rf.refreshHeartbeatTimeout(server, 10)
			}
			rf.mu.Unlock()
		}
	}
	return
}

func (rf *Raft) logReplicas(i int) (vote int) {
	for j := 0; j < len(rf.replicators_); j++ {
		if rf.replicators_[j].MatchIndex >= i {
			vote++
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
func getElectionTimeout() time.Duration {
	timeout := 150 + rand.Intn(150) // random timeout in [150, 300] ms
	return time.Millisecond * time.Duration(timeout)
}
func getHeartbeatTimeout() time.Duration {
	return time.Millisecond * time.Duration(100)
}
func args2Slice(args ...interface{}) []interface{} {
	var arg []interface{}
	return append(arg, args...)
}
