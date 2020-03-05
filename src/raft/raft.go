package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



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

type Cmd struct {
	inst string
}

// Entry
type Entry struct {
	Command interface{}
	Term    int
}

func (e *Entry) equals(a Entry) bool {
	 return a.Term == e.Term && a.Command == e.Command
}

type ServerId int
// Persistent state
type PersistentState struct {
	currentTerm 	int
	votedFor		int
	logEntries		[]Entry
}

type Leader struct {
	nextIndex		[]int //
	matchIndex		[]int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	ps          PersistentState // persistent state data in memory
	commitIndex int             // index of highest log entry known to be committed
								// initialized to 0, increases monotonically
	lastApplied	int				// index of highest log entry applied to state machine
								// initialized to 0, increases monotonically
	leader		Leader
	leaderIndex	int
	voteVersion int 			// vote version

	interruptChan chan bool
	leaderChan chan bool
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}
/**
 * serialize from PersistentState to Persistent
 */
func (rf *Raft) ToSerialize() {
	// TODO
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	log.Println("GetState for ", rf.me)
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.ps.currentTerm
	isleader = rf.leaderIndex == rf.me
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// TODO: Could add the version number for each request and response in order to invalidate the outdated Reply
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's Term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
	Version 	 int // request version
	Entries 	 []Entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
	Version 	 int // reply version
}

type AppendEntriesArgs struct {
	Term               int     // leader's Term
	LeaderId           int     // so follower can redirect clients
	PrevLogIndex       int     // index of log entry immediately preceding new ones
	PrevLogTermEntries []Entry // Term of PrevLogIndex entry log entries to store (empty for heartbeat;
	LeaderCommit       int     // leader's commit index
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and prevLogTerm
}

func (rf *Raft) AdmitLeader(args *RequestVoteArgs) {
	rf.ps.currentTerm = args.Term
	rf.ps.votedFor = args.CandidateId
	rf.leaderIndex = args.CandidateId
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if len(rf.interruptChan) == 0 {
		rf.interruptChan <- true
	}
	rf.mu.Unlock()

	log.Println("RequestVote from ", args.CandidateId, " to ", rf.me)
	reply.Term = Max(args.Term, rf.ps.currentTerm)
	// Reply false if Term < currentTerm (§5.1)
	if args.Term < rf.ps.currentTerm {
		reply.VoteGranted = false
		reply.Version = args.Version
		log.Println("Not granted because ", rf.me, "'s term is ", args.Term, " current term is ", rf.ps.currentTerm)
		return
	}

	currentTerm := 0
	if len(rf.ps.logEntries) > 0 {
		currentTerm = rf.ps.logEntries[len(rf.ps.logEntries) - 1].Term
	}

	// If votedFor is null or candidateId
	if rf.ps.votedFor == -1 || rf.ps.votedFor == args.CandidateId {
		// Candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		if args.LastLogTerm > currentTerm {
			rf.AdmitLeader(args)
			// TODO 补充 entries
			rf.fillEntries(rf.ps.logEntries, args.Entries)
			reply.VoteGranted = true
		} else if args.LastLogTerm == currentTerm {
			// TODO equals OR greater?
			if args.LastLogIndex >= len(rf.ps.logEntries) {
				rf.AdmitLeader(args)
				rf.fillEntries(rf.ps.logEntries, args.Entries)
				reply.VoteGranted = true
			} else {
				log.Println("Not granted because ", rf.me, "'s logIndex is ", len(rf.ps.logEntries), " current logIndex is ", args.LastLogIndex)
				reply.VoteGranted = false
			}
		} else {
			log.Println("Not granted because ", rf.me, "'s LastLogTerm is ", rf.ps.currentTerm, " current term is ", args.LastLogTerm)
			reply.VoteGranted = false
		}
	} else {
		log.Println("Not granted because ", rf.me, "'s votedFor is ", rf.ps.votedFor , " current candidateId is ", args.CandidateId)
		reply.VoteGranted = false
	}
	//rf.ps.currentTerm = Max(args.Term, rf.ps.currentTerm)
	reply.Version = args.Version
}

// appendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.interruptChan <- true
	rf.ps.currentTerm = args.Term
	//// heartbeat entry
	//if args.PrevLogTermEntries == nil {
	//	reply.Success = true
	//	return
	//}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.ps.currentTerm {
		reply.Success = false
		return
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.ps.logEntries) || rf.ps.logEntries[args.PrevLogIndex].Term != args.PrevLogTermEntries[args.PrevLogIndex].Term {
		reply.Success = false
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	for i := args.PrevLogIndex + 1; i < len(args.PrevLogTermEntries); i ++ {
		if args.PrevLogIndex+ 1 < len(rf.ps.logEntries) {
			rf.ps.logEntries[args.PrevLogIndex + 1] = args.PrevLogTermEntries[args.PrevLogIndex + 1]
		} else {
			rf.ps.logEntries = append(rf.ps.logEntries, args.PrevLogTermEntries[i])
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(rf.commitIndex, len(rf.ps.logEntries))
	}
	reply.Success = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		log.Println("requestVote network from: ", rf.me, " to ", server, " fails.")
	} else {
		log.Println("requestVote network from: ", rf.me, " to ", server, " success.")
	}
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartbeat() bool {
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		log.Println("send heartbeat from ", rf.me, " to ", idx)
		args := &AppendEntriesArgs{}
		args.Term = rf.ps.currentTerm
		args.LeaderId = rf.me
		args.PrevLogTermEntries = rf.ps.logEntries

		reply := &AppendEntriesReply{}

		ok := rf.peers[idx].Call("Raft.AppendEntries", args, reply)
		if !ok {
			log.Println("send heartbeat from", rf.me, " to ", idx, " fail")
		} else {
			log.Println("send heartbeat from", rf.me, " to ", idx, " success")
		}
	}
	return true
}

func (rf *Raft) waitTimeout() bool {
	// Clear the interrupt channel
	rf.mu.Lock()
	for len(rf.interruptChan) > 0 {
		<- rf.interruptChan
	}
	rf.mu.Unlock()
	log.Println("waitTimeout ", rf.me, " rf.leaderChan length ", len(rf.leaderChan))
	select {
		case <- rf.leaderChan:
			rf.convertToLeader()
			return false
		case <- rf.interruptChan:
			return false
		case <- time.After(time.Duration(rand.Intn(150) + 150) * time.Millisecond):
			return true
	}
}

func (rf *Raft) convertToLeader() {
	log.Println(rf.me, " convert to leader")
	rf.leaderIndex = rf.me
	rf.ps.currentTerm ++
}

func (rf *Raft) sendCandiRequest() {
	counter := 1
	once := sync.Once{}

	// clear the channel data
	//for len(rf.leaderChan) > 0 {
	//	<- rf.leaderChan
	//}

	// send requestVote to each peer
	rf.voteVersion ++
	for idx, _ := range rf.peers {
		if idx != rf.me {
			//log.Println("idx is ", idx, " rf.me is ", rf.me)
			go func(idx int) {
				// 1. increase currentTerm
				rf.ps.currentTerm ++

				var lastLogTerm int
				if len(rf.ps.logEntries) > 0 {
					lastLogTerm = rf.ps.logEntries[len(rf.ps.logEntries) - 1].Term
				} else {
					lastLogTerm = 0
				}
				args := &RequestVoteArgs{rf.ps.currentTerm, rf.me,
					len(rf.ps.logEntries), lastLogTerm, rf.voteVersion, rf.ps.logEntries}
				reply := &RequestVoteReply{}

				log.Println("SendCandRequest from: ", rf.me, " to: ", idx)
				// 2. vote for itself
				if rf.sendRequestVote(idx, args, reply) {
					if reply.VoteGranted {
						log.Println("Get Reply from ", idx, " sent by ", rf.me, " req version ", reply.Version, " cur version ", rf.voteVersion)
						rf.mu.Lock()
						counter ++
						if reply.Version == rf.voteVersion && counter >= (len(rf.peers) + 1)/2 {
							once.Do(func() {
								log.Println("Send leader signal to self", rf.me)
								rf.leaderChan <- true
							})
						}
						rf.mu.Unlock()
					} else {
						log.Println("sendRequestVote not granted term ", reply.Term, " current term: ", rf.ps.currentTerm, " version ", reply.Version)
						rf.ps.currentTerm = Max(reply.Term, rf.ps.currentTerm)
					}
				}
			}(idx)
		}
	}
}

func (rf *Raft) resetStatus() {
	rf.ps.votedFor = -1
}

func (rf *Raft) fillEntries(to []Entry, from []Entry) {
	idx := Min(len(to), len(from))
	for ; idx > 0 && !to[idx].equals(from[idx]) ; idx -- {
	}

	for i := idx; i < len(from); i ++ {
		to[i] = from[i]
	}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.Println("begin Make for id ", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.ps = PersistentState{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderIndex = -1
	rf.leaderChan = make(chan bool, 1)
	rf.interruptChan = make(chan bool, 1)
	rf.voteVersion = 0

	go func() {
		for {
			if rf.leaderIndex == rf.me {
				time.Sleep(50 * time.Millisecond)
				rf.ps.logEntries = append(rf.ps.logEntries, Entry{1, rf.ps.currentTerm})
				ok := rf.sendHeartbeat()
				if !ok {
					log.Fatal("Error for sending heartbeat")
				}
			} else {
				// If timeout return true, current server convert to candidate and began voting for itself
				//log.Println("begin wait timeout ", rf.me)
				if rf.waitTimeout() {
					rf.resetStatus()
					rf.sendCandiRequest()
				}
			}
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
