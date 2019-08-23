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

// import "bytes"
// import "labgob"

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
	Command string
}

type LogEntryDescriptor struct {
	Term  int
	Index int
}

type Replicator struct {
	NextIndex  int
	MatchIndex int
}

type TimedClosure struct {
	mtx      sync.Mutex
	duration time.Duration
	timer    *time.Timer
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
	voted_for_    string     // persisted candidate for which voted during *current* term
	data_         []LogEntry // [0, last_applied_] in persister, (last_applied_, len(data_) - 1] in WAL.
	last_applied_ int        // the index of the last applied log entry, *must* persist.
	commit_index_ int        // the index of the last commited log entry, could be infered from scratch(starting from 0).
	events_       chan Event
	apply_chan_   chan ApplyMsg
	replicators_  []Replicator  // replicators
	timer_        *TimedClosure // Timer for election/heartbeat
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term      int
	Candidate string
	LastLog   LogEntryDescriptor
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Approve bool
}

type AppendEntriesArgs struct {
	Term         int
	Leader       string
	PrevLog      LogEntryDescriptor
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	LastLog LogEntryDescriptor
	Leader  string
	Endname string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.current_term_
	isleader = rf.is_leader_
	return term, isleader
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
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.current_term_ {
		reply.Approve = false
	} else if len(rf.voted_for_) == 0 || rf.voted_for_ == args.Candidate {
		ld := rf.LogDescriptor()
		reply.Approve = (LogCompare(&args.LastLog, &ld) >= 0)
	} else {
		reply.Approve = false
	}
	if args.Term > rf.current_term_ {
		rf.current_term_ = args.Term
	}
	reply.Term = rf.current_term_
	rm.mu.Unlock()

	if reply.Approve { // TODO step down
		rf.mu.Lock()
		rf.voted_for_ = args.Candidate
		if rf.is_leader_ { // leader=>follower switch timer
			rf.is_leader_ = false
			rf.timer_.DelayFor(TimeoutDuration(),
				func(a interface{}) {
					term := a.(int)
					rf.events_ <- Event{kElectionTimeout, term}
				},
				rf.current_term_)
		}
		rm.mu.Unlock()
	}
	return
}

func (ft *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	reply.Leader = rf.voted_for_
	reply.Endname = string(rf.peers[me].endname)

	if args.Term < rf.current_term_ {
		reply.Success = false
		reply.Term = rf.current_term_
		return
	}
	cmp := args.PrevLog.Compare(&rf.LogDescriptor())
	if args.Term > rf.current_term_ || cmp > 0 {
		rf.current_term_ = args.Term
		reply.Term = rf.current_term_
		rf.voted_for_ = args.Leader
		if rf.is_leader_ { // TODO step down
			rf.is_leader_ = false
			rf.timer_.DelayFor(TimeoutDuration(),
				func(a interface{}) {
					term := a.(int)
					rf.events_ <- Event{kRequestVoteTimeout, term}
				},
				rf.current_term_)
		}
	} else if cmp <= 0 {
		reply.Success = false
		reply.Term = rf.current_term_
		reply.LastLog = rf.LogDescriptor()
		return
	}

	if cmp == 0 {
		rf.data_ = append(rf.data_, args.Entries)
		reply.Success = true
	} else if cmp > 0 {
		if args.PrevLog.Index == len(rf.data_)-1 {
			reply.Success = false
			reply.LastLog.Term = rf.data_[args.PrevLog.Index].Term
			for i := args.PrevLog.Index; i >= 0 && rf.data_[i].Term == reply.LastLog.Term; i-- {
				reply.LastLog.Index = i
			}
		} else if args.PrevLog.Index < len(rf.data_)-1 {
			LastLog := rf.LogDescriptor()
			panic("PrevLog={Term:%d, Index:%d} LastLog={Term:%d, INdex:%d}",
				args.PrevLog.Term, args.PrevLog.Index, LastLog.Term, LastLog.Index)
		} else {
			reply.Success = false
			reply.LastLog = rf.LogDescriptor()
		}
	} else {
		if len(rf.data_) > args.PrevLog.Index &&
			args.PrevLog.Term < rf.data_[args.PrevLog.Index].Term {
			panic(fmt.Sprintf("Entries from leader got elder Term %d than current %d",
				args.PrevLog.Term, rf.data_[args.PrevLog.Index].Term))
		}
		if rf.last_applied_ > args.PrevLog.Index ||
			(rf.last_applied_ == args.PrevLog.Index &&
				rf.data_[args.PrevLog.Index].Term != args.PrevLog.Term) {
			panic(fmt.Sprintf("LastApplied %d conflicted with PrevLog %d",
				rf.last_applied_, args.PrevLog.Index))
		}
		if rf.data_[args.PrevLog.Index].Term == args.PrevLog.Term {
			rf.data_ = append(rf.data_[0:args.PrevLog.Index+1], args.Entries)
			reply.Success = true
			log.Printf("purge staled logs after Index %d", args.PrevLog.Index)
		} else {
			rf.data_ = rf.data_[0:args.PrevLog.Index]
			reply.Success = false
			reply.LastLog.Term = rf.data_[args.PrevLog.Index].Term
			for i := args.PrevLog.Index; i >= 0 && rf.data_[i].Term == reply.LastLog.Term; i-- {
				reply.LastLog.Index = i
			}
		}
	}

	if len(rf.data_)-1 < rf.last_applied_ {
		panic(fmt.Sprintf("Raft logs %d shorter than applied %d", len(rf.data_)-1, rf.last_applied_))
	}

	if reply.Success { // handle args.LeaderCommit
		if args.LeaderCommit > rf.last_applied_ { // TODO apply rf.last_applied_ + 1
			rf.apply_chan_ <- ApplyMsg{true, rf.data_[rf.last_applied_+1], rf.last_applied_ + 1}
			rf.last_applied_ += 1
			rf.commit_index_ = rf.last_applied_
		}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, done chan RequestVoteReply) bool {
	reply := RequestVoteReply{}
	const kMethod string = "Raft.RequestVote"
	ok := rf.peers[server].Call(kMethod, args, &reply)
	if !ok {
		log.Println(kMethod, "() to ", rf.peers[server].endname, " error")
	}
	done <- reply
	return ok
}

// FIXME we need separate heartbeat timeout for each replicator
func (rf *Raft) IssueRequestVote(args *RequestVoteArgs) bool {
	npeers := len(rf.peers)
	majority = npeers/2 + 1
	replies := make(chan RequestVoteReply, npeers)
	replies <- RequestVoteReply{Term: args.Term, Approve: true}

	log.Printf("sending %d RequestVote", npeers-1)
	for server := 0; server < npeers; server++ {
		if server != rf.me {
			go rf.sendRequestVote(server, args, replies)
		}
	}
	log.Printf("sent %d RequestVote", npeers-1)

	maxTerm := 0
	votes := 0
	for server := 0; server < npeers; server++ {
		reply := <-replies
		if reply.Term > maxTerm {
			maxTerm = reply.Term
		}
		if reply.Approve {
			votes += 1
		}
	}
	log.Printf("RequestVote() %d ={Term:%d, Votes:%d}", npeers, maxTerm, votes)
	reply := RequestVoteReply{Term: maxTerm, Approve: votes >= majority}
	rf.events_ <- Event{kRequestVoteDone, *args, reply}

	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,
	done []chan AppendEntriesReply) bool {
	reply := AppendEntriesReply{}
	const kMethod string = "Raft.AppendEntries"
	ok := rf.peers[server].Call(kMethod, args, &reply)
	if !ok {
		log.Println(kMethod, "() to ", rf.peers[server].endname, " error")
	}
	done[rf.me] <- reply
	return ok
}

func (rf *Raft) IssueAppendEntries() {
	if !rf.is_leader_ {
		panic("IssueAppendEntries() not leader")
	}
	var args AppendEntriesArgs
	args.Term = rf.current_term_
	args.Leader = string(rf.peers[rf.me].endname)
	args.LeaderCommit = rf.commit_index_
	npeers := len(rf.peers)
	majority = npeers/2 + 1
	replies := make([]chan AppendEntriesReply, npeers)
	for i := 0; i < npeers; i++ {
		replies[i] = make(chan AppendEntriesReply, 1)
		if i == rf.me {
			replies[i] <- AppendEntriesReply{rf.current_term_, true}
		}
	}

	log.Printf("sending %d AppendEntries", npeers-1)
	for server := 0; server < npeers; server++ {
		if server != rf.me {
			r := &replicators_[server]
			prev := LogEntryDescriptor{0, -1}
			if r.NextIndex > 0 {
				prev.Index = r.NextIndex - 1
				prev.Term = rf.data_[prev.Index].Term
			}
			req := AppendEntriesArgs{args.Term, args.Leader, prev,
				rf.data_[r.NextIndex : args.LeaderCommit+1], args.LeaderCommit}
			go rf.sendAppendEntries(server, &req, replies)
		}
	}
	log.Printf("sent %d AppendEntries", npeers-1)

	maxTerm := 0
	commited := args.LeaderCommit
	for server := 0; server < npeers; server++ {
		reply := <-replies[server]
		if reply.Term > maxTerm {
			maxTerm = reply.Term
		}
		// TODO update replicators_ commited
		if reply.Success {
			replicators_[server].NextIndex = args.LeaderCommit + 1
		} else if reply.Term > rf.current_term_ {
			rf.current_term_ = reply.Term
			// TODO step down
			rf.voted_for_ = reply.Leader
			if rf.is_leader_ { // leader=>follower switch timer
				rf.is_leader_ = false
				rf.timer_.DelayFor(TimeoutDuration(),
					func(a interface{}) {
						term := a.(int)
						rf.events_ <- Event{kElectionTimeout, term}
					},
					rf.current_term_)
			}
		} else if reply.LastLog.Compare(&rf.data_[replicators_[server].NextIndex]) >= 0 {
			log.Printf("ignore AppendEntries reply LastLog={Term=%d, Index=%d}",
				reply.LastLog.Term, reply.LastLog.Index)
		} else {
		}
	}
	if commited > rf.commit_index_ {
		rf.commit_index_ = commited
	}
	if rf.commit_index_ > rf.last_applied_ {
		rf.apply_chan_ <- ApplyMsg{true, rf.data_[rf.last_applied_].Command, rf.last_applied_}
	}
	reply := AppendEntriesReply{maxTerm, maxTerm <= rf.current_term_}
	rf.events_ <- Event{kAppendEntriesDone, args, reply}
	return
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
	term := rf.current_term_
	isLeader := rf.is_leader_
	if isLeader == false {
		return index, term, isLeader
	}

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.current_term_ = 0
	rf.is_leader_ = false
	rf.data_ = make([]LogEntry, 0, 1)
	rf.last_applied_ = -1
	rf.commit_index_ = -1
	rf.events_ = make(chan Event)
	rf.apply_chan_ = applyCh
	rf.replicators_ = make([]Replicator, len(peers))
	rf.timer_ = TimedClosure{duration: TimeoutDuration()} // start election timer
	rf.Delay(func(a interface{}) {
		term := a.(int)
		e := Event{kElectionTimeout, term}
		rf.events_ <- e
	}, rf.current_term_)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start event loop
	go rf.Run()

	return rf
}

//
// Main event loop
//
func (rf *Raft) Run() {
	for _, e := range rf.events_ {
		if e.Type == kElectionTimeout {
			term := e.Args.(int)
			if term != rf.current_term_ {
				log.Printf("ignore staled election timeout: term=%d currentTerm=%d", term, rf.current_term_)
			} else {
				// sendRequestVote
				rf.current_term_ += 1
				rf.voted_for_ = string(rf.peers[me].endname)
				args := RequestVoteArgs{rf.current_term_, rf.voted_for_, rf.LogDescriptor()}
				go rf.IssueRequestVote(&args)
			}
			if rf.is_leader_ {
				panic("kElectionTimeout fired as leader")
			}
			// refesh timer
			rf.timer_.Reset(TimeoutDuration())
		} else if e.Type == kHeartbeatTimeout {
			term := e.Args.(int)
			if term != rf.current_term_ {
				log.Printf("ignore staled heartbeat timeout: term=%d currentTerm=%d", term, rf.current_term_)
			} else if rf.is_leader_ == false {
				log.Printf("ignore staled heartbeat timeout: not leader")
			} else {
				// TODO send heartbeat
				rf.timer_.Reset(time.Duration(100) * time.Millisecond)
			}
		} else if e.Type == kRequestVoteDone {
			args := e.Args.(RequestVoteArgs)
			if args.Term != rf.current_term_ {
				log.Printf("ignore staled RequestVote reply: term=%d currentTerm=%d", term, rf.current_term_)
			} else {
				reply := e.Reply.(RequestVoteReply)
				// TODO step up
				if reply.Approve { // candidate=>leader switch timer
					if rf.is_leader_ == false {
						rf.is_leader_ = true
						for i := 0; i < len(rf.replicators_); i++ {
							rf.replicators_[i].NextIndex = len(rf.data_)
							rf.replicators_[i].MatchIndex = -1
						}
						rf.timer_.DelayFor(time.Duration(100)*time.Millisecond,
							func(a interface{}) {
								rf.events_ <- Event{kHeartbeatTimout, a.(int)}
							},
							rf.current_term_)
					} else {
						log.Printf("ignore staled RequestVote done, is leader")
					}
				}
			}
		}
	}
}

///*********************** Timer, timeout **********************************///
func TimeoutDuration() time.Duration {
	timeout := (rand.Intn(150) + 150) * 1000000 // random timeout in [150, 300] ms
	return time.Duration(timeout)
}

func (t *TimedClosure) Reset(duration time.Duration) {
	t.mtx.Lock()
	t.duration = duration
	if t.timer != nil {
		t.timer.Stop()
		t.timer.Reset(duration)
	}
	t.mtx.Unlock()
}

func (t *TimedClosure) Stop() {
	t.mtx.Lock()
	if t.timer != nil {
		!t.timer.Stop()
	}
	t.timer = nil
	t.mtx.Unlock()
}

func (t *TimedClosure) Delay(f func()) {
	t.mtx.Lock()
	t.timer = time.AfterFunc(t.duration, func() {
		t.mtx.Lock()
		f()
		t.mtx.Unlock()
	})
	t.mtx.Unlock()
}

func (t *TimedClosure) DelayFor(duration time.Duration, f func()) {
	t.mtx.Lock()
	t.duration = duration
	t.timer = time.AfterFunc(t.duration, func() {
		t.mtx.Lock()
		f()
		t.mtx.Unlock()
	})
	t.mtx.Unlock()
}

///*********************** log entry ***********************************///
func (rf *Raft) LogDescriptor() LogEntryDescriptor {
	var LogEntryDescriptor ld
	rf.mu.Lock()
	ld.Term = rf.current_term_
	if rf.data_ != nil && len(rf.data_) > 0 {
		ld.Index = len(rf.data_) - 1
	}
	rf.mu.Unlock()
	return ld
}

func (ld *LogEntryDescriptor) Compare(other *LogEntryDescriptor) int {
	cmp := ld.Term - other.Term
	if cmp != 0 {
		return cmp
	}
	return ld.Index - other.Index
}

func LogCompare(l, r *LogEntryDescriptor) int {
	return l.Compare(r)
}
