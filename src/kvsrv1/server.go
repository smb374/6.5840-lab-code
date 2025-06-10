package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Slot struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	Store map[string]*Slot
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.Store = make(map[string]*Slot)

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	slot, ok := kv.Store[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Err = rpc.OK
	reply.Value = slot.Value
	reply.Version = slot.Version

	return
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	slot, ok := kv.Store[args.Key]
	reply.Err = rpc.OK

	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
		} else {
			slot := new(Slot)
			slot.Value = args.Value
			slot.Version = rpc.Tversion(1)
			kv.Store[args.Key] = slot
		}
	} else {
		if args.Version != slot.Version {
			reply.Err = rpc.ErrVersion
		} else {
			slot.Value = args.Value
			slot.Version += 1
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
