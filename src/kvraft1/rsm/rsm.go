package rsm

import (
	"context"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	ID  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	Lock         sync.Mutex
	Ctx          context.Context
	ReaderCancel context.CancelFunc
	StateSize    int
	OpCounter    int
	OpResult     map[int]OpChan
	Killed       bool
}

type OpChan struct {
	ch   chan any
	term int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		Ctx:          context.Background(),
		OpResult:     make(map[int]OpChan),
		Lock:         sync.Mutex{},
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	ctx, cancel := context.WithCancel(rsm.Ctx)
	rsm.ReaderCancel = cancel
	go rsm.Reader(ctx)
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.Lock.Lock()
	term, isLeader := rsm.Raft().GetState()
	if rsm.Killed || !isLeader {
		rsm.Lock.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	op := Op{Me: rsm.me, ID: rsm.OpCounter, Req: req}
	rsm.OpCounter++

	index, nterm, isLeader := rsm.Raft().Start(op)
	// A term change indicates that the node won't be a leader until next election
	// so we can reject this submit request.
	if !isLeader || nterm != term {
		rsm.Lock.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	opc := OpChan{ch: make(chan any), term: nterm}
	rsm.OpResult[index] = opc
	rsm.Lock.Unlock()

	result, ok := <-opc.ch
	rsm.Lock.Lock()
	defer rsm.Lock.Unlock()
	if ok {
		close(opc.ch)
		delete(rsm.OpResult, index)
		return rpc.OK, result
	} else {
		delete(rsm.OpResult, index)
		return rpc.ErrWrongLeader, nil
	}
}

func (rsm *RSM) Reader(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case msg, ok := <-rsm.applyCh:
			if !ok {
				rsm.Lock.Lock()
				rsm.ReaderCancel()
				rsm.Killed = true
				for index, opc := range rsm.OpResult {
					close(opc.ch)
					delete(rsm.OpResult, index)
				}
				rsm.Lock.Unlock()
				break loop
			}
			if msg.IsDemotion {
				rsm.Lock.Lock()
				for index, opc := range rsm.OpResult {
					close(opc.ch)
					delete(rsm.OpResult, index)
				}
				rsm.Lock.Unlock()
			} else if msg.CommandValid {
				op := msg.Command.(Op)
				res := rsm.sm.DoOp(op.Req)
				rsm.Lock.Lock()
				opc, ok := rsm.OpResult[msg.CommandIndex]
				term, _ := rsm.Raft().GetState()
				rsm.StateSize++
				rsm.Lock.Unlock()
				if ok {
					if opc.term == term {
						// Leader need to write back DoOp result to Submit.
						opc.ch <- res
					} else {
						// Term changed
						close(opc.ch)
					}
				}
			} else if msg.SnapshotValid {
				// TODO: Snapshot related stuff.
			}
		}
	}
}
