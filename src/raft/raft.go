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

import (
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "../labrpc"
import "../labgob"

// import "bytes"
// import "../labgob"

const Follower, Leader, Candidate = 1, 2, 3
const HeartbeatDuration = time.Duration(time.Millisecond * 600)
const CandidateDuration = HeartbeatDuration * 2

var raftOnce sync.Once

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term 	int
	Index 	int
	Log 	interface{}
}

type LogSnapshot struct {
	Term 	int
	Index 	int
	Datas	[]byte
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Me 				int
	ElectionTerm 	int
	LogIndex 		int
	LogTerm 		int
}

type RequestVoteReply struct {
	// Your data here (2A).
	IsAgree 		bool
	CurrentTerm 	int
}


type AppendEntries struct {
	Me 				int // so follower can redirect clients
	Term 			int	// leader’s term
	PrevLogTerm		int // index of log entry immediately preceding new ones
	PrevLogIndex 	int // index of log entry immediately preceding new ones
	Entries 		[]LogEntry // term of prevLogIndex entry log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit 	int // leader’s commitIndex
	Snapshot		LogSnapshot
}

type RespEntries struct {
	Term 			int // currentTerm, for leader to update itself
	Succeed 		bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	LastApplied 	int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        		sync.Mutex          // Lock to protect shared access to this peer's state
	peers     		[]*labrpc.ClientEnd // RPC end points of all peers
	persister 		*Persister          // Object to hold this peer's persisted state
	me        		int                 // this peer's index into peers[]
	dead      		int32               // set by Kill()

	logs 	  		[]LogEntry
	logSnapshot 	LogSnapshot
	commitIndex 	int					// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied 	int					// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	status 			int
	currentTerm 	int
	heartbeatTimers []*time.Timer
	electionTimer   *time.Timer
	randtime 		*rand.Rand

	nextIndex 		[]int 				// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex 		[]int 				// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	applyCh			chan ApplyMsg
	isKilled 		bool
	lastLogs 		AppendEntries
	EnableDebugLog 	bool
	LastGetLock 	string
}

func (rf *Raft) println(args ...interface{}) {
	if rf.EnableDebugLog {
		log.Println(args...)
	}
}

func (rf *Raft) lock(info string) {
	rf.mu.Lock()
	rf.LastGetLock = info
}

func (rf *Raft) unlock(info string) {
	rf.LastGetLock = ""
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rstChan := make(chan bool)
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequestVote", args, reply)
		rstChan <- rst
	}()

	select {
	case ok = <- rstChan:
	case <-time.After(HeartbeatDuration):
		// rpc timeout
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, req *AppendEntries, resp *RespEntries) bool {
	rstChan := make(chan bool)
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequestAppendEntries", req, resp)
		rstChan <- rst
	}()
	select {
		case ok = <-rstChan:
		case <- time.After(time.Millisecond * 400):
	}
	return ok
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock("Raft.GetState")
	defer rf.unlock("Raft.GetState")
	isLeader := rf.status == Leader
	// Your code here (2A).
	return rf.currentTerm, isLeader
}

func (rf *Raft) setTerm(term int) {
	rf.lock("Raft.setTerm")
	defer rf.unlock("Raft.setTerm")
	rf.currentTerm = term
}

func (rf *Raft) addTerm(term int) {
	rf.lock("Raft.addTerm")
	defer rf.unlock("Raft.addTerm")
	rf.currentTerm += term
}

func (rf *Raft) setStatus(status int) {
	rf.lock("Raft.setStatus")
	defer rf.unlock("Raft.setStatus")

	if rf.status != Follower && status == Follower {
		rf.resetCandidateTimer()
	}

	if rf.status != Leader && status == Leader {
		index := len(rf.logs)
		for i := 0; i < len(rf.peers); i ++ {
			rf.nextIndex[i] = index + 1 + rf.logSnapshot.Index
			rf.matchIndex[i] = 0
		}
	}
	rf.status = status
}


func (rf *Raft) getstatus() int {
	rf.lock("Raft.getStatus")
	defer rf.unlock("Raft.getStatus")
	return rf.status
}

func (rf *Raft) setCommitIndex(index int) {
	rf.lock("Raft.setCommitIndex")
	defer rf.unlock("Raft.setCommitIndex")
	rf.commitIndex = index
}


func (rf *Raft) getLogTermAndIndex() (int, int) {
	rf.lock("Raft.getLogTermAndIndex")
	defer rf.unlock("Raft.getLogTermAndIndex")

	index := 0
	term := 0
	size := len(rf.logs)
	if size > 0 {
		index = rf.logs[size - 1].Index
		term = rf.logs[size - 1].Term
	} else {
		index  =rf.logSnapshot.Index
		term = rf.logSnapshot.Term
	}

	return term, index
}

func (rf *Raft) getLogTermOfIndex(index int) int {
	rf.lock("Raft.getLogTermOfIndex")
	defer rf.unlock("Raft.getLogTermOfIndex")
	index -= 1 + rf.logSnapshot.Index
	if index < 0 {
		return rf.logSnapshot.Term
	}
	return rf.logs[index].Term
}


func (rf *Raft) getSnapshot(index int, snapshot *LogSnapshot) int {
	if index <= rf.logSnapshot.Index {
		*snapshot = rf.logSnapshot
		index = 0
	} else {
		index -= rf.logSnapshot.Index
	}
	return index
}

func (rf *Raft) getEntriesInfo(index int, snapshot *LogSnapshot, entries *[]LogEntry) (preterm int, preindex int) {
	start := rf.getSnapshot(index, snapshot) - 1
	if start < 0 {
		preindex = 0
		preterm = 0
	} else if start == 0 {
		if rf.logSnapshot.Index  == 0 {
			preindex = 0
			preterm = 0
		} else {
			preindex = rf.logSnapshot.Index
			preterm = rf.logSnapshot.Term
		}
	} else {
		preindex = rf.logs[start - 1].Index
		preterm = rf.logs[start - 1].Term
	}

	if start < 0 {
		start = 0
	}

	for i := start; i < len(rf.logs); i ++ {
		*entries = append(*entries, rf.logs[i])
	}
	return
}

func (rf *Raft) getAppendEntries(peer int) AppendEntries {
	rf.lock("Raft.getAppendEntries")
	defer rf.unlock("Raft.getAppendEntries")

	rst := AppendEntries{
		Me:           rf.me,
		Term:         rf.currentTerm,
		LeaderCommit: rf.commitIndex,
		Snapshot:     LogSnapshot{Index: 0},
	}
	next := rf.nextIndex[peer]
	rst.PrevLogTerm, rst.PrevLogIndex = rf.getEntriesInfo(next, &rst.Snapshot, &rst.Entries)
	return rst
}

func (rf *Raft) setNext(peer int, next int) {
	rf.lock("Raft.setNext")
	defer rf.unlock("Raft.setNext")
	rf.nextIndex[peer] = next
}

func (rf *Raft) setNextAndMatch(peer int, index int) {
	rf.lock("Raft.setNextAndMatch")
	defer rf.unlock("Raft.setNextAndMatch")
	rf.nextIndex[peer] = index + 1
	rf.matchIndex[peer] = index
}

func (rf *Raft) updateLog(start int, logEntrys []LogEntry,snapshot *LogSnapshot) {
	rf.lock("Raft.updateLog")
	defer rf.unlock("Raft.updateLog")

	if snapshot.Index > 0 {
		rf.logSnapshot = *snapshot
		start = rf.logSnapshot.Index
		rf.println("update snapshot :", rf.me, rf.logSnapshot.Index, "len logs", len(logEntrys))
	}

	index := start - rf.logSnapshot.Index
	for i := 0; i < len(logEntrys); i ++ {
		if index + i < 0 {
			// if the network is bad which will cause the duplicated apply
			continue
		}

		if index + i < len(rf.logs) {
			rf.logs[index + i] = logEntrys[i]
		} else {
			rf.logs = append(rf.logs, logEntrys[i])
		}
	}

	size := index + len(logEntrys)
	if size < 0 {
		size = 0
	}
	rf.logs = rf.logs[:size]
}

func (rf *Raft) insertLog(command interface{}) int {
	rf.lock("Raft.insertLog")
	defer rf.unlock("Raft.insertLog")
	entry := LogEntry{
		Term:  rf.currentTerm,
		Index: 1,
		Log:   command,
	}

	if len(rf.logs) > 0 {
		entry.Index = rf.logs[len(rf.logs) - 1].Index + 1
	} else {
		entry.Index = rf.logSnapshot.Index + 1
	}

	rf.logs = append(rf.logs, entry)
	return entry.Index
}

func (rf *Raft) updateCommitIndex() bool {
	rst := false
	var indexes []int
	rf.matchIndex[rf.me] = 0

	if len(rf.logs) > 0 {
		rf.matchIndex[rf.me] = rf.logs[len(rf.logs) - 1].Index
	} else {
		rf.matchIndex[rf.me] = rf.logSnapshot.Index
	}

	for i := 0; i < len(rf.matchIndex); i ++ {
		indexes = append(indexes, rf.matchIndex[i])
	}
	sort.Ints(indexes)
	index := len(indexes) / 2
	commit := indexes[index]
	if commit > rf.commitIndex {
		rf.println(rf.me, "update leader commit index", commit)
		rst = true
		rf.commitIndex = commit
	}

	return rst
}

func (rf *Raft) apply() {
	rf.lock("Raft.apply")
	defer rf.unlock("Raft.apply")

	if rf.status == Leader {
		rf.updateCommitIndex()
	}

	lastapplied := rf.lastApplied
	if rf.lastApplied < rf.logSnapshot.Index {
		msg := ApplyMsg{
			CommandValid: false,
			Command:      rf.logSnapshot,
			CommandIndex: 0,
		}
		rf.applyCh <- msg
		rf.lastApplied = rf.logSnapshot.Index
		rf.println(rf.me, " apply snapshot :", rf.logSnapshot.Index, " with logs: ", len(rf.logs))
	}

	last := 0
	if len(rf.logs) > 0 {
		last = rf.logs[len(rf.logs) - 1].Index
	}
	for ; rf.lastApplied < rf.commitIndex && rf.lastApplied < last; rf.lastApplied ++ {
		index := rf.lastApplied
		msg := ApplyMsg{
			CommandValid: 		true,
			Command: 			rf.logs[index - rf.logSnapshot.Index].Log,
			CommandIndex: 		rf.logs[index - rf.logSnapshot.Index].Index,
		}
		rf.applyCh <- msg
	}

	if rf.lastApplied > lastapplied {
		appliedIndex := rf.lastApplied - 1 - rf.logSnapshot.Index
		endIndex, endTerm := 0, 0
		if appliedIndex < 0 {
			endIndex 	= rf.logSnapshot.Index
			endTerm 	= rf.logSnapshot.Term
		} else {
			endTerm 	= rf.logs[rf.lastApplied - 1 - rf.logSnapshot.Index].Term
			endIndex 	= rf.logs[rf.lastApplied - 1 - rf.logSnapshot.Index].Index
		}
		rf.println(rf.me, "apply log", rf.lastApplied - 1, endTerm, "-", endIndex, "/", last)
	}
}

func (rf *Raft) setLastLog(req *AppendEntries) {
	rf.lock("Raft.setLastLog")
	defer rf.unlock("Raft.setLastLog")
	rf.lastLogs = *req
}

func (rf *Raft) isOldRequest(req *AppendEntries) bool {
	rf.lock("Raft.isOldRequest")
	defer rf.unlock("Raft.isOldRequest")

	if req.Term == rf.lastLogs.Term && req.Me == rf.lastLogs.Me {
		lastIndex := rf.lastLogs.PrevLogIndex + rf.lastLogs.Snapshot.Index + len(rf.lastLogs.Entries)
		reqLastIndex := req.PrevLogIndex + req.Snapshot.Index + len(req.Entries)
		return lastIndex > reqLastIndex
	}
	return false
}

func (rf *Raft) resetCandidateTimer() {
	randCnt := rf.randtime.Intn(250)
	duration := time.Duration(randCnt) * time.Millisecond + CandidateDuration
	rf.electionTimer.Reset(duration)
}

func (rf *Raft) persist() {
	rf.lock("Raft.persist")
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.commitIndex)
	encoder.Encode(rf.logs)
	encoder.Encode(rf.lastLogs)
	encoder.Encode(rf.logSnapshot.Index)
	encoder.Encode(rf.logSnapshot.Term)
	data := writer.Bytes()
	rf.unlock("Raft.persist")

	rf.persister.SaveStateAndSnapshot(data, rf.logSnapshot.Datas)
	msg := ApplyMsg {
		CommandValid: 	false,
		Command: 		nil,
		CommandIndex: 	0,
	}
	rf.applyCh <- msg
}

func (rf *Raft) readPersist(data []byte) {

	rf.lock("Raft.readPersist")

	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.unlock("Raft.readPersist")
		return
	}

	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)

	var commitIndex, currentTerm int
	var logs []LogEntry
	var lastlogs AppendEntries

	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&lastlogs) != nil ||
		decoder.Decode(&rf.logSnapshot.Index) != nil ||
		decoder.Decode(&rf.logSnapshot.Term) != nil {
		rf.println("Error in unmarshal raft state")
	} else {
		rf.currentTerm = currentTerm
		rf.commitIndex = commitIndex
		rf.lastApplied = 0
		rf.logs = logs
		rf.lastLogs = lastlogs
	}
	rf.unlock("Raft.readPersist")

	rf.logSnapshot.Datas = rf.persister.ReadSnapshot()
}

func (rf *Raft) RequestVote(req *RequestVoteArgs, reply *RequestVoteReply) {
	reply.IsAgree = true
	reply.CurrentTerm, _ = rf.GetState()

	if reply.CurrentTerm >= req.ElectionTerm {
		rf.println(rf.me, "refuse", req.Me, "because of term")
		reply.IsAgree = false
		return
	}

	rf.setStatus(Follower)
	rf.setTerm(req.ElectionTerm)
	logterm, logindex := rf.getLogTermAndIndex()
	if logterm > req.LogTerm {
		rf.println(rf.me, "refuse", req.Me, "because of log's term")
		reply.IsAgree = false
	} else if logterm == req.LogTerm {
		reply.IsAgree = logindex <= req.LogIndex
		if !reply.IsAgree {
			rf.println(rf.me, "resuse", req.Me, "because of log's index")
		}
	}
	if reply.IsAgree {
		rf.println(rf.me, "agree", req.Me)
		rf.resetCandidateTimer()
	}
}

func (rf *Raft) Vote() {
	rf.addTerm(1)
	rf.println("start vote :", rf.me, "term :", rf.currentTerm)
	logterm, logindex := rf.getLogTermAndIndex()

	currentTerm, _ := rf.GetState()
	req := RequestVoteArgs {
		Me: 			rf.me,
		ElectionTerm:	currentTerm,
		LogTerm: 		logterm,
		LogIndex:		logindex,
	}

	var wait sync.WaitGroup
	peerCnt := len(rf.peers)
	wait.Add(peerCnt)
	agreeVote 	:= 0
	term 		:= currentTerm
	for i := 0; i < peerCnt; i ++ {
		go func(index int) {
			defer wait.Done()
			resp := RequestVoteReply{
				IsAgree:     false,
				CurrentTerm: -1,
			}
			if index == rf.me {
				agreeVote ++
				return
			}
			rst := rf.sendRequestVote(index, &req, &resp)
			if !rst {
				return
			}
			if resp.IsAgree {
				agreeVote ++
				return
			}
			if resp.CurrentTerm > term {
				term = resp.CurrentTerm
			}
		}(i)
	}
	wait.Wait()

	if term > currentTerm {
		rf.setTerm(term)
		rf.setStatus(Follower)
	} else if agreeVote * 2 > peerCnt {
		rf.println(rf.me, "become leader :", currentTerm)
		rf.setStatus(Leader)
		rf.replicateLogNow()
	}
}

func (rf *Raft) ElectionLoop() {
	rf.resetCandidateTimer()
	defer rf.electionTimer.Stop()

	for !rf.isKilled {
		<- rf.electionTimer.C
		if rf.isKilled {
			break
		}
		if rf.getstatus() == Candidate {
			rf.resetCandidateTimer()
			rf.Vote()
		} else if rf.getstatus() == Follower {
			rf.setStatus(Candidate)
			rf.resetCandidateTimer()
			rf.Vote()
		}
	}
	rf.println(rf.me, "Exit ElectionLoop")
}

func (rf *Raft) RequestAppendEntries(req *AppendEntries, resp *RespEntries) {
	currentTerm, _ := rf.GetState()
	resp.Term = currentTerm
	resp.Succeed = true
	if req.Term < currentTerm {
		resp.Succeed = false
		return
	}

	if rf.isOldRequest(req) {
		return
	}

	rf.resetCandidateTimer()
	rf.setTerm(req.Term)
	rf.setStatus(Follower)
	_, logindex := rf.getLogTermAndIndex()

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > logindex {
			resp.Succeed = false
			resp.LastApplied = rf.lastApplied
			return
		}
		if rf.getLogTermOfIndex(req.PrevLogIndex) != req.PrevLogTerm {
			resp.Succeed = false
			resp.LastApplied = rf.lastApplied
			return
		}
	}

	// update log
	rf.setLastLog(req)
	if len(req.Entries) > 0 || req.Snapshot.Index > 0 {
		if len(req.Entries) > 0 {
			rf.println(rf.me, "udpate log from ", req.Me, ":", req.Entries[0].Term, "-",
				req.Entries[0].Index, "to", req.Entries[len(req.Entries) - 1].Term, "-",
				req.Entries[len(req.Entries) - 1].Index)
		}
		rf.updateLog(req.PrevLogIndex, req.Entries, &req.Snapshot)
	}
	rf.setCommitIndex(req.LeaderCommit)
	rf.apply()
	rf.persist()
}

func (rf *Raft) replicateLogTo(peer int) bool {
	replicateRst := false
	if peer == rf.me {
		return replicateRst
	}
	isLoop := true
	for isLoop {
		isLoop = false
		currentTerm, isLeader := rf.GetState()
		if !isLeader || rf.isKilled {
			break
		}
		req := rf.getAppendEntries(peer)
		resp := RespEntries{Term: 0}
		rst := rf.sendAppendEntries(peer, &req, &resp)
		currentTerm, isLeader = rf.GetState()
		if rst && isLeader {
			if resp.Term > currentTerm {
				rf.println(rf.me, " become follower ", peer, " term: ", resp.Term)
				rf.setTerm(resp.Term)
				rf.setStatus(Follower)
			} else if !resp.Succeed {
				rf.setNext(peer, resp.LastApplied + 1)
				isLoop = true
			} else {
				 if len(req.Entries) > 0 {
				 	rf.setNextAndMatch(peer, req.Entries[len(req.Entries) - 1].Index)
					 replicateRst = true
				 } else if req.Snapshot.Index > 0 {
				 	rf.setNextAndMatch(peer, req.Snapshot.Index)
				 	replicateRst = true
				 }
			}
		} else {
			isLoop = true
		}
	}
	return replicateRst
}

func (rf *Raft) replicateLogNow() {
	rf.lock("Raft.replicateLogNow")
	defer rf.unlock("Raft.replicateLogNow")
	for i := 0; i < len(rf.peers); i ++ {
		rf.heartbeatTimers[i].Reset(0)
	}
}

func (rf *Raft) ReplicateLogLoop(peer int) {
	defer func() {
		rf.heartbeatTimers[peer].Stop()
	}()

	for !rf.isKilled {
		<- rf.heartbeatTimers[peer].C
		if rf.isKilled {
			break
		}
		rf.lock("Raft.ReplicateLogLoop")
		rf.heartbeatTimers[peer].Reset(HeartbeatDuration)
		rf.unlock("Raft.ReplicateLogLoop")

		_, isLeader := rf.GetState()
		if isLeader {
			success := rf.replicateLogTo(peer)
			if success {
				rf.apply()
				rf.replicateLogNow()
				rf.persist()
			}
		}
	}
	rf.println(rf.me,"-",peer,"Exit ReplicateLogLoop")
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = 0
	term, isLeader = rf.GetState()
	if isLeader {
		index = rf.insertLog(command)
		rf.println("leader", rf.me, ":", "append log", term, "-", index)
		rf.replicateLogNow()
	}

	return
}

func (rf *Raft) Kill() {
	rf.isKilled = true
	rf.electionTimer.Reset(0)
	rf.replicateLogNow()
}

func (rf *Raft) SaveSnapshot(index int,snapshot []byte) {
	rf.lock("Raft.SaveSnapshot")
	if index > rf.logSnapshot.Index {
		// save snapshot log entries
		start := rf.logSnapshot.Index
		rf.logSnapshot.Index = index
		rf.logSnapshot.Datas = snapshot
		rf.logSnapshot.Term = rf.logs[index-start-1].Term
		// delete snapshot log entries
		if len(rf.logs) >0 {
			rf.logs = rf.logs[(index-start):]
		}
		rf.println("save snapshot :",rf.me,index,",len logs:",len(rf.logs))
		rf.unlock("Raft.SaveSnapshot")
		rf.persist()
	} else {
		rf.unlock("Raft.SaveSnapshot")
	}

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.randtime = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.isKilled = false
	rf.heartbeatTimers = make([]*time.Timer, len(rf.peers))
	rf.electionTimer = time.NewTimer(CandidateDuration)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.setStatus(Follower)
	rf.EnableDebugLog = false

	rf.lastLogs = AppendEntries{
		Me: 	-1,
		Term: 	-1,
	}
	rf.logSnapshot = LogSnapshot{
		Index: 	0,
		Term: 	0,
	}

	for i := 0; i < len(rf.peers); i ++ {
		rf.heartbeatTimers[i] = time.NewTimer(HeartbeatDuration)
		go rf.ReplicateLogLoop(i)
	}
	rf.readPersist(persister.ReadRaftState())
	rf.apply()
	raftOnce.Do(func() {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
	})

	go rf.ElectionLoop()

	return rf
}
