package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type Slot struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	Lock  sync.Mutex
	Store map[string]Slot
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		kv.Lock.Lock()
		defer kv.Lock.Unlock()
		var reply rpc.GetReply
		slot, ok := kv.Store[args.Key]
		if !ok {
			reply.Err = rpc.ErrNoKey
			return reply
		}

		reply.Err = rpc.OK
		reply.Value = slot.Value
		reply.Version = slot.Version

		return reply
	case rpc.PutArgs:
		kv.Lock.Lock()
		defer kv.Lock.Unlock()
		var reply rpc.PutReply
		slot, ok := kv.Store[args.Key]
		reply.Err = rpc.OK

		if !ok {
			if args.Version != 0 {
				reply.Err = rpc.ErrNoKey
			} else {
				slot := Slot{
					Value:   args.Value,
					Version: rpc.Tversion(1),
				}
				kv.Store[args.Key] = slot
			}
		} else {
			if args.Version != slot.Version {
				reply.Err = rpc.ErrVersion
			} else {
				slot.Value = args.Value
				slot.Version += 1
				kv.Store[args.Key] = slot
			}
		}

		return reply
	default:
		log.Printf("Unknown request type.")
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.Lock.Lock()
	defer kv.Lock.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Store)

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.Lock.Lock()
	defer kv.Lock.Unlock()

	var store map[string]Slot
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&store) != nil {
		log.Print("kv: Failed to decode state...")
	} else {
		kv.Store = store
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	*reply = rep.(rpc.PutReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.Lock.Lock()
	defer kv.Lock.Unlock()
	if kv.Store == nil {
		kv.Store = make(map[string]Slot)
	}
	return []tester.IService{kv, kv.rsm.Raft()}
}
