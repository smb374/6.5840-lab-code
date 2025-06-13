package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	// "bytes"
	"cmp"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

func ceildiv(x int, y int) int {
	return (x + y - 1) / y
}

type Role int

const (
	ROLE_FOLLOWER  Role = 0
	ROLE_CANDIDATE Role = 1
	ROLE_LEADER    Role = 2
)

const ELECTION_TIMEOUT time.Duration = 350 * time.Millisecond

type RaftLog struct {
	Term    int
	Command interface{} // TODO: Update command structure
}

// Persistent State for Raft peers
type RaftState struct {
	CurrentTerm int
	VotedFor    int
	Logs        []RaftLog
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
	Role             Role
	PStates          RaftState
	CommitIdx        int
	LastApplied      int
	LastHeartBeat    time.Time
	ElectionCanceler context.CancelFunc
	LeaderID         int
	// Leader specific states
	NextIndex  []int
	MatchIndex []int
	PeerCond   *sync.Cond
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
	lastLogIdx, lastLogTerm := rf.LastLogIdxAndTerm()
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.PStates.CurrentTerm
	reply.Success = false

	// Check term consistency
	switch cmp.Compare(args.Term, rf.PStates.CurrentTerm) {
	case -1:
		// src is less recent, return
		rf.mu.Unlock()
		return
	case 1:
		// src is more recent, convert current to follower
		rf.InitFollower(args.Term)
		reply.Term = args.Term
	}
	// Update HeartBeat time & leader ID
	rf.LastHeartBeat = time.Now()
	rf.LeaderID = args.LeaderID

	// Check if PrevLogIdx & PrevLogTerm matches
	if args.PrevLogIdx >= len(rf.PStates.Logs) {
		// TODO: Need proper handling, reject for now
		rf.mu.Unlock()
		return
	} else if rf.PStates.Logs[args.PrevLogIdx].Term != args.PrevLogTerm {
		rf.mu.Unlock()
		return
	}
	// Success when matched
	reply.Success = true
	// Update log etries in current node
	for i, e := range args.Entries {
		idx := args.PrevLogIdx + 1 + i
		if idx < len(rf.PStates.Logs) {
			// Overwrite inconsistent entries in current node
			if rf.PStates.Logs[idx].Term != e.Term {
				rf.PStates.Logs = rf.PStates.Logs[:idx]
				rf.PStates.Logs = append(rf.PStates.Logs, args.Entries[i:]...)
				break
			}
		} else {
			// Append rest of entries to logs.
			rf.PStates.Logs = append(rf.PStates.Logs, args.Entries[i:]...)
		}
	}
	// Update commit index
	if args.LeaderCommit > rf.CommitIdx {
		newCommitIdx := min(args.LeaderCommit, len(rf.PStates.Logs)-1)

		messagesToApply := []raftapi.ApplyMsg{}
		for i := rf.CommitIdx + 1; i <= newCommitIdx; i++ {
			messagesToApply = append(messagesToApply, raftapi.ApplyMsg{
				CommandValid: true,
				CommandIndex: i,
				Command:      rf.PStates.Logs[i].Command,
			})
		}
		rf.CommitIdx = newCommitIdx

		rf.mu.Unlock()

		for _, msg := range messagesToApply {
			rf.ApplyCh <- msg
		}
		return
	}
	rf.mu.Unlock()
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
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.PStates.CurrentTerm {
			rf.InitFollower(reply.Term)
		}
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.PStates.CurrentTerm {
			rf.InitFollower(reply.Term)
			if rf.PeerCond != nil {
				rf.PeerCond.Broadcast()
			}
		}
		rf.mu.Unlock()
	}
	return ok
}

// NOTE: Should be locked before using.
func (rf *Raft) SendHeartBeats() {
	rf.LastHeartBeat = time.Now()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// Send HeartBeat to peers.
		prevIdx := rf.NextIndex[i] - 1
		prevTerm := rf.PStates.Logs[prevIdx].Term
		args := AppendEntriesArgs{
			Term:         rf.PStates.CurrentTerm,
			LeaderID:     rf.me,
			PrevLogIdx:   prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      []RaftLog{},
			LeaderCommit: rf.CommitIdx,
		}
		go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
	}
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
	index, term = rf.LastLogIdxAndTerm()

	rf.PeerCond.Broadcast()

	return
}

func (rf *Raft) PeerWatcher(peer int) {
	for {
		rf.mu.Lock()
		for len(rf.PStates.Logs) <= rf.NextIndex[peer] {
			if rf.Role != ROLE_LEADER {
				rf.mu.Unlock()
				return
			}
			rf.PeerCond.Wait()
		}
		rf.mu.Unlock()
		for {
			rf.mu.Lock()
			if rf.Role != ROLE_LEADER {
				rf.mu.Unlock()
				return
			}
			nidx := rf.NextIndex[peer]
			prevIdx := nidx - 1
			prevTerm := rf.PStates.Logs[prevIdx].Term

			args := AppendEntriesArgs{
				Term:         rf.PStates.CurrentTerm,
				LeaderID:     rf.me,
				PrevLogIdx:   prevIdx,
				PrevLogTerm:  prevTerm,
				Entries:      rf.PStates.Logs[nidx:],
				LeaderCommit: rf.CommitIdx,
			}
			rf.mu.Unlock()

			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				// Peer failed for now, giveup
				break
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
				break
			} else {
				// Retry with a smaller index
				rf.NextIndex[peer] /= 2
			}
			rf.mu.Unlock()
		}
	}
}

// Update leader's commit index to commit the logs to the state machine
func (rf *Raft) UpdateCommitIndex() {
	for N := len(rf.PStates.Logs) - 1; N > rf.CommitIdx; N-- {
		if rf.PStates.CurrentTerm != rf.PStates.Logs[N].Term {
			break
		}

		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.MatchIndex[i] >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			messagesToApply := []raftapi.ApplyMsg{}
			for i := rf.CommitIdx + 1; i <= N; i++ {
				messagesToApply = append(messagesToApply, raftapi.ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      rf.PStates.Logs[i].Command,
				})
			}
			rf.CommitIdx = N
			rf.mu.Unlock()

			for _, msg := range messagesToApply {
				rf.ApplyCh <- msg
			}

			return
		}
	}
	rf.mu.Unlock()
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// NOTE: Need to be locked before using
func (rf *Raft) LastLogIdxAndTerm() (idx int, term int) {
	idx = len(rf.PStates.Logs) - 1
	if idx >= 0 {
		term = rf.PStates.Logs[idx].Term
	}
	return

}

func (rf *Raft) InitFollower(new_term int) {
	rf.Role = ROLE_FOLLOWER
	rf.PStates.CurrentTerm = new_term
	rf.PStates.VotedFor = -1
}

func (rf *Raft) InitLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Role = ROLE_LEADER
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	rf.PeerCond = sync.NewCond(&rf.mu)

	rf.LastHeartBeat = time.Now()
	// HeartBeat args
	// No need to success, so only the first 2 argument is meaningful
	lidx, lterm := rf.LastLogIdxAndTerm()
	args := AppendEntriesArgs{
		Term:         rf.PStates.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIdx:   lidx,
		PrevLogTerm:  lterm,
		Entries:      []RaftLog{},
		LeaderCommit: rf.CommitIdx,
	}
	for i := range rf.peers {
		rf.NextIndex[i] = lidx + 1
		rf.MatchIndex[i] = 0
		if i == rf.me {
			// Send HeartBeat to peers.
			continue
		}
		go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
		go rf.PeerWatcher(i)
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

		if votes >= ceildiv(len(rf.peers), 2) {
			rf.InitLeader()
			break
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		tdelta := time.Now().Sub(rf.LastHeartBeat)
		if rf.Role == ROLE_LEADER {
			rf.SendHeartBeats()
		} else if tdelta > ELECTION_TIMEOUT {
			// Cancel previous election
			if rf.ElectionCanceler != nil {
				rf.ElectionCanceler()
			}
			// Start election
			// Change to candidate & vote for self
			rf.Role = ROLE_CANDIDATE
			rf.PStates.CurrentTerm++
			rf.PStates.VotedFor = rf.me
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
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(ELECTION_TIMEOUT))
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
	rf.Role = ROLE_FOLLOWER
	rf.PStates.CurrentTerm = 0
	rf.PStates.VotedFor = -1
	rf.PStates.Logs = []RaftLog{}
	rf.CommitIdx = 0
	rf.LastApplied = 0
	rf.LastHeartBeat = time.Now()
	rf.ApplyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if len(rf.PStates.Logs) == 0 {
		rf.PStates.Logs = append(rf.PStates.Logs, RaftLog{
			Term:    0,
			Command: nil,
		})
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
