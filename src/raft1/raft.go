package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"cmp"
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

func majority(x int) int {
	return x/2 + 1
}

type Role int

const (
	ROLE_FOLLOWER  Role = 0
	ROLE_CANDIDATE Role = 1
	ROLE_LEADER    Role = 2
)

const ELECTION_TIMEOUT time.Duration = 350 * time.Millisecond
const BEAT_COOLDOWN time.Duration = 40 * time.Millisecond

type RaftLog struct {
	Term    int
	Command interface{}
}

// Persistent State for Raft peers
type RaftState struct {
	CurrentTerm      int
	VotedFor         int
	Logs             []RaftLog
	LastIncludedIdx  int
	LastIncludedTerm int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Ctx              context.Context
	Role             Role
	PStates          RaftState
	CommitIdx        int
	LastApplied      int
	LastHeartBeat    time.Time
	ElectionCanceler context.CancelFunc
	LeaderID         int
	// Snapshot
	SnapshotBuf []byte
	// Leader specific states
	NextIndex    []int
	MatchIndex   []int
	Processing   []bool
	BeatCanceler context.CancelFunc
	BeatSignals  []chan bool
	// ApplyCh
	ApplyCh chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.Role == ROLE_LEADER
	term = rf.PStates.CurrentTerm
	return term, isleader
}

// Check node with expected node & term.
func (rf *Raft) CheckNodeConsistency(erole Role, eterm int) bool {
	return rf.Role == erole && rf.PStates.CurrentTerm == eterm
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.PStates)
	state := w.Bytes()
	rf.persister.Save(state, rf.SnapshotBuf)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

	var state RaftState
	if d.Decode(&state) != nil {
		log.Printf("%d: Failed to decode state...", rf.me)
	} else {
		rf.PStates = state
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	sidx := index - rf.PStates.LastIncludedIdx
	if sidx < 0 || sidx >= len(rf.PStates.Logs) {
		// Invalid index to work with
		return
	}
	term := rf.PStates.Logs[sidx].Term

	newLogs := []RaftLog{
		{
			Term:    term,
			Command: nil,
		},
	}
	if sidx+1 < len(rf.PStates.Logs) {
		newLogs = append(newLogs, rf.PStates.Logs[sidx+1:]...)
	}
	rf.PStates.Logs = newLogs
	rf.PStates.LastIncludedIdx = index
	rf.PStates.LastIncludedTerm = term
	rf.SnapshotBuf = snapshot

	rf.CommitIdx = max(rf.CommitIdx, index)
	rf.LastApplied = max(rf.LastApplied, index)

	rf.persist()
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderID         int
	LastIncludedIdx  int
	LastIncludedTerm int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.PStates.CurrentTerm

	switch cmp.Compare(args.Term, rf.PStates.CurrentTerm) {
	case -1:
		return
	case 1:
		// Update term & become follower
		rf.InitFollower(args.Term)
		rf.persist()
	}

	rf.LastHeartBeat = time.Now()
	rf.LeaderID = args.LeaderID

	if args.LastIncludedIdx <= rf.CommitIdx {
		return
	}

	rf.PStates.LastIncludedIdx = args.LastIncludedIdx
	rf.PStates.LastIncludedTerm = args.LastIncludedTerm
	rf.SnapshotBuf = args.Data

	rf.CommitIdx = args.LastIncludedIdx
	rf.LastApplied = args.LastIncludedIdx

	newLogs := []RaftLog{
		{
			Term:    args.LastIncludedTerm,
			Command: nil,
		},
	}

	rf.PStates.Logs = newLogs
	rf.persist()

	rf.ApplySnapshot(args.LastIncludedIdx, args.LastIncludedTerm, rf.SnapshotBuf)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateID int
	LastLogIdx  int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIdx, lastLogTerm := rf.LastLogIdxAndTerm()

	reply.Term = rf.PStates.CurrentTerm
	reply.VoteGranted = false

	switch cmp.Compare(args.Term, rf.PStates.CurrentTerm) {
	case -1:
		// args.Term < self term
		// Reject voting
		return
	case 1:
		// args.Term > self term
		// Step down as follower
		rf.InitFollower(args.Term)
		reply.Term = args.Term
	}

	canVote := rf.PStates.VotedFor == -1 || rf.PStates.VotedFor == args.CandidateID
	logsUpdated := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIdx >= lastLogIdx)

	if canVote && logsUpdated {
		reply.VoteGranted = true
		rf.PStates.VotedFor = args.CandidateID
		rf.persist()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []RaftLog
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.PStates.CurrentTerm
	reply.Success = false

	// Check term consistency
	switch cmp.Compare(args.Term, rf.PStates.CurrentTerm) {
	case -1:
		// src is less recent, return
		return
	case 1:
		// src is more recent, convert current to follower
		rf.InitFollower(args.Term)
		reply.Term = args.Term
	}
	// Update HeartBeat time & leader ID
	rf.LastHeartBeat = time.Now()
	rf.LeaderID = args.LeaderID
	prevIdx := args.PrevLogIdx - rf.PStates.LastIncludedIdx

	// Check if PrevLogIdx & PrevLogTerm matches
	if prevIdx < 0 {
		reply.XIndex = rf.PStates.LastIncludedIdx
		return
	} else if prevIdx >= len(rf.PStates.Logs) {
		reply.XTerm = -1
		reply.XLen = len(rf.PStates.Logs)
		return
	}

	term := rf.PStates.Logs[prevIdx].Term
	if term != args.PrevLogTerm {
		reply.XTerm = term

		idx := prevIdx
		for idx > 0 && rf.PStates.Logs[idx-1].Term == term {
			idx--
		}
		reply.XIndex = idx + rf.PStates.LastIncludedIdx
		return
	}
	// Success when matched
	reply.Success = true
	// Update log etries in current node
	for i, e := range args.Entries {
		idx := prevIdx + 1 + i
		if idx < len(rf.PStates.Logs) {
			// Overwrite inconsistent entries in current node
			if rf.PStates.Logs[idx].Term != e.Term {
				rf.PStates.Logs = rf.PStates.Logs[:idx]
				rf.PStates.Logs = append(rf.PStates.Logs, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			// Append rest of entries to logs.
			rf.PStates.Logs = append(rf.PStates.Logs, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	// Update commit index
	if args.LeaderCommit > rf.CommitIdx {
		lidx, _ := rf.LastLogIdxAndTerm()
		newCommitIdx := min(args.LeaderCommit, lidx)
		rf.CommitIdx = newCommitIdx

		go rf.ApplyMessages()
		return
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && reply.Term > rf.PStates.CurrentTerm {
		rf.InitFollower(reply.Term)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && reply.Term > rf.PStates.CurrentTerm {
		rf.InitFollower(reply.Term)
		if rf.BeatCanceler != nil {
			rf.BeatCanceler()
		}
	}
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && reply.Term > rf.PStates.CurrentTerm {
		rf.InitFollower(reply.Term)
		if rf.BeatCanceler != nil {
			rf.BeatCanceler()
		}
	}
	return ok
}

func (rf *Raft) PeerInstallSnapshot(peer int) {
	snapshot := rf.persister.ReadSnapshot()
	rf.mu.Lock()
	if rf.Role != ROLE_LEADER {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:             rf.PStates.CurrentTerm,
		LeaderID:         rf.me,
		LastIncludedIdx:  rf.PStates.LastIncludedIdx,
		LastIncludedTerm: rf.PStates.LastIncludedTerm,
		Data:             snapshot,
	}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	ok := rf.sendInstallSnapshot(peer, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.CheckNodeConsistency(ROLE_LEADER, args.Term) {
		return
	}

	rf.NextIndex[peer] = rf.PStates.LastIncludedIdx + 1
	rf.MatchIndex[peer] = rf.PStates.LastIncludedIdx
}

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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.Role == ROLE_LEADER
	if !isLeader {
		return
	}
	rlog := RaftLog{
		Term:    rf.PStates.CurrentTerm,
		Command: command,
	}
	rf.PStates.Logs = append(rf.PStates.Logs, rlog)
	rf.persist()
	index, term = rf.LastLogIdxAndTerm()

	for p := range rf.peers {
		if rf.killed() {
			return
		}
		if p == rf.me {
			continue
		}
		go func() {
			select {
			case rf.BeatSignals[p] <- true:
			case <-time.After(BEAT_COOLDOWN):
			}
		}()
	}

	return
}

func (rf *Raft) PeerBeater(ctx context.Context, peer int, sig chan bool) {
	go rf.ReplicateToPeer(peer)
	ticker := time.NewTicker(BEAT_COOLDOWN)
	isStart := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-rf.BeatSignals[peer]:
			ticker.Reset(10 * time.Millisecond)
			isStart = true
		case <-ticker.C:
			go rf.ReplicateToPeer(peer)
			if isStart {
				ticker.Reset(BEAT_COOLDOWN)
			}
		}
	}
}

func (rf *Raft) ReplicateToPeer(peer int) {
	rf.mu.Lock()

	if rf.Role != ROLE_LEADER {
		rf.mu.Unlock()
		return
	}

	lidx, _ := rf.LastLogIdxAndTerm()
	nidx := rf.NextIndex[peer]
	if nidx <= rf.PStates.LastIncludedIdx {
		// Peer fall behind, install snapshot
		rf.mu.Unlock()
		rf.PeerInstallSnapshot(peer)
		return
	}
	prevIdx := nidx - 1
	prevTerm := rf.PStates.Logs[prevIdx-rf.PStates.LastIncludedIdx].Term
	entries := []RaftLog{}
	if lidx >= nidx {
		entries = rf.PStates.Logs[nidx-rf.PStates.LastIncludedIdx:]
	}

	args := AppendEntriesArgs{
		Term:         rf.PStates.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIdx:   prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.CommitIdx,
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		// Peer failed for now, giveup
		return
	}
	rf.mu.Lock()

	if !rf.CheckNodeConsistency(ROLE_LEADER, args.Term) {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		// Update NextIndex & MatchIndex
		newNextIndex := args.PrevLogIdx + len(args.Entries) + 1
		rf.NextIndex[peer] = newNextIndex
		rf.MatchIndex[peer] = newNextIndex - 1
		rf.UpdateCommitIndex()
	} else {
		// Retry with a smaller index
		if reply.XTerm == -1 {
			// Peer's log size is smaller than us
			rf.NextIndex[peer] = reply.XLen
		} else {
			// Find first log in leader that has matching term with reply.XTerm
			idx := -1
			for i := len(rf.PStates.Logs) - 1; i >= 0; i-- {
				if rf.PStates.Logs[i].Term == reply.XTerm {
					idx = i
					break
				}
			}

			if idx != -1 {
				// There's a matching term @ idx, use its next as NextIndex
				rf.NextIndex[peer] = idx + 1 + rf.PStates.LastIncludedIdx
			} else if reply.XIndex > rf.PStates.LastIncludedIdx {
				// Otherwise, set NextIndex to the first index that the peer has that term.
				rf.NextIndex[peer] = reply.XIndex
			} else {
				// Peer fall behind, install snapshot
				rf.mu.Unlock()
				rf.PeerInstallSnapshot(peer)
				return
			}
		}
	}
	rf.mu.Unlock()
}

// Update leader's commit index to commit the logs to the state machine
func (rf *Raft) UpdateCommitIndex() {
	lidx, _ := rf.LastLogIdxAndTerm()
	for N := lidx; N > rf.CommitIdx; N-- {
		idx := N - rf.PStates.LastIncludedIdx
		if rf.PStates.CurrentTerm != rf.PStates.Logs[idx].Term {
			break
		}

		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.MatchIndex[i] >= N {
				count++
			}
		}

		if count >= majority(len(rf.peers)) {
			rf.CommitIdx = N
			go rf.ApplyMessages()

			return
		}
	}
}

func (rf *Raft) ApplyMessages() {
	for rf.LastApplied < rf.CommitIdx {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.LastApplied++
		aidx := rf.LastApplied - rf.PStates.LastIncludedIdx
		if aidx >= len(rf.PStates.Logs) {
			log.Printf("%d: Weird CommitIdx problem", rf.me)
			log.Printf("%d: LastApplied = %d, CommitIdx = %d, len(logs) = %d", rf.me, rf.LastApplied-rf.PStates.LastIncludedIdx, rf.CommitIdx-rf.PStates.LastIncludedIdx, len(rf.PStates.Logs))
			rf.mu.Unlock()
			return
		}
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.LastApplied,
			Command:      rf.PStates.Logs[aidx].Command,
		}
		rf.mu.Unlock()
		rf.ApplyCh <- msg
	}
}

func (rf *Raft) ApplySnapshot(index, term int, snapshot []byte) {
	msg := raftapi.ApplyMsg{
		CommandValid: false,

		SnapshotValid: true,
		SnapshotIndex: index,
		SnapshotTerm:  term,
		Snapshot:      snapshot,
	}

	if rf.killed() {
		return
	}
	rf.ApplyCh <- msg
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	if rf.ElectionCanceler != nil {
		rf.ElectionCanceler()
	}
	if rf.BeatCanceler != nil {
		rf.BeatCanceler()
	}

	// Active drain ApplyCh before closing it since there can be
	// senders waiting for receiver on this channel and it'll panic
	// on close.
	go func() {
		select {
		case <-rf.ApplyCh:
		case <-time.After(5 * time.Millisecond):
		}
		close(rf.ApplyCh)
	}()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// NOTE: Need to be locked before using
func (rf *Raft) LastLogIdxAndTerm() (idx int, term int) {
	sidx := len(rf.PStates.Logs) - 1
	idx = rf.PStates.LastIncludedIdx + sidx
	if sidx >= 0 {
		term = rf.PStates.Logs[sidx].Term
	}
	return

}

func (rf *Raft) InitFollower(new_term int) {
	oldRole := rf.Role
	rf.Role = ROLE_FOLLOWER
	rf.PStates.CurrentTerm = new_term
	rf.PStates.VotedFor = -1
	rf.persist()

	if oldRole == ROLE_LEADER {
		if rf.killed() {
			return
		}
		rf.ApplyCh <- raftapi.ApplyMsg{IsDemotion: true, DemotedTerm: new_term}
	}
}

func (rf *Raft) InitLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Role = ROLE_LEADER
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	cancelers := []context.CancelFunc{}

	rf.LastHeartBeat = time.Now()
	// HeartBeat args
	// No need to success, so only the first 2 argument is meaningful
	lidx, _ := rf.LastLogIdxAndTerm()
	for i := range rf.peers {
		rf.NextIndex[i] = lidx + 1
		rf.MatchIndex[i] = 0
		if i != rf.me {
			ctx, cancel := context.WithCancel(rf.Ctx)
			cancelers = append(cancelers, cancel)
			go rf.PeerBeater(ctx, i, rf.BeatSignals[i])
		}
	}

	rf.BeatCanceler = func() {
		for _, c := range cancelers {
			c()
		}
	}
}

func (rf *Raft) SingleElection(pidx int, args *RequestVoteArgs, votec chan int) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(pidx, args, &reply)
	if !ok {
		votec <- 0
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.CheckNodeConsistency(ROLE_CANDIDATE, args.Term) {
		// -1 to abort election
		votec <- -1
		return
	}

	switch cmp.Compare(reply.Term, rf.PStates.CurrentTerm) {
	case 0:
		if reply.VoteGranted {
			votec <- 1
		} else {
			votec <- 0
		}
	case 1:
		// Become follower
		rf.InitFollower(reply.Term)
		votec <- -1
	case -1:
		// Ignore
		votec <- 0
	}
}

func (rf *Raft) Election(ctx context.Context, args RequestVoteArgs) {
	votes := 1
	votec := make(chan int, len(rf.peers)-1)
	// defer close(votec)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.SingleElection(i, &args, votec)
	}

	for {
		select {
		case <-ctx.Done():
			// Election cancelled
			return
		case v := <-votec:
			// Election abort
			if v == -1 {
				return
			}
			votes += v
		}

		if votes >= majority(len(rf.peers)) {
			rf.InitLeader()
			return
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		tdelta := time.Since(rf.LastHeartBeat)
		if rf.Role != ROLE_LEADER && tdelta > ELECTION_TIMEOUT {
			// Cancel previous election
			if rf.ElectionCanceler != nil {
				rf.ElectionCanceler()
			}
			// Start election
			// Change to candidate & vote for self
			rf.Role = ROLE_CANDIDATE
			rf.PStates.CurrentTerm++
			rf.PStates.VotedFor = rf.me
			rf.persist()
			// Create args
			lidx, lterm := rf.LastLogIdxAndTerm()
			args := RequestVoteArgs{
				Term:        rf.PStates.CurrentTerm,
				CandidateID: rf.me,
				LastLogIdx:  lidx,
				LastLogTerm: lterm,
			}
			// Update LastHeartBeat, create context, fire Election goroutine
			rf.LastHeartBeat = time.Now()
			ctx, cancel := context.WithTimeout(rf.Ctx, ELECTION_TIMEOUT)
			rf.ElectionCanceler = cancel
			go rf.Election(ctx, args)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.Ctx = context.Background()
	rf.Role = ROLE_FOLLOWER
	rf.PStates.CurrentTerm = 0
	rf.PStates.VotedFor = -1
	rf.PStates.Logs = []RaftLog{}
	rf.CommitIdx = 0
	rf.LastApplied = 0
	rf.LastHeartBeat = time.Time{}
	rf.ApplyCh = applyCh
	rf.BeatSignals = make([]chan bool, len(rf.peers))

	for p := range rf.peers {
		rf.BeatSignals[p] = make(chan bool)
	}
	// Snapshot
	rf.PStates.LastIncludedIdx = 0
	rf.PStates.LastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// read snapshot
	rf.SnapshotBuf = persister.ReadSnapshot()

	rf.CommitIdx = rf.PStates.LastIncludedIdx
	rf.LastApplied = rf.PStates.LastIncludedIdx

	if len(rf.PStates.Logs) == 0 {
		rf.PStates.Logs = append(rf.PStates.Logs, RaftLog{
			Term:    0,
			Command: nil,
		})
		rf.persist()
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
