package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true
const INFO = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType uint8

const (
	PUT OpType = iota
	APPEND
	GET
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int
	Type     OpType
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db            map[string]string
	latestResults map[int64]RequestResult
	rpcWakers     map[int64]*RpcWaker
	persister     *raft.Persister
}

type RpcWaker struct {
	seq  int // required when crashing, re-applying previous ops
	term int // without comparing seq may wrongly wake up rpcs
	done chan string
	lost chan bool
}

type RequestResult struct {
	Seq   int
	Value string
}

func (kv *KVServer) debug(format string, args ...interface{}) {
	args = append([]interface{}{kv.me}, args...)
	DPrintf("KVServer %v "+format, args...)
}

func (kv *KVServer) info(format string, args ...interface{}) {
	if INFO {
		args = append([]interface{}{kv.me}, args...)
		log.Printf("KVServer %v "+format, args...)
	}
}

func (kv *KVServer) checkLeadership() {
	for !kv.killed() {
		currentTerm, _ := kv.rf.GetState()
		wakers := []*RpcWaker{}
		kv.mu.Lock()
		for clientId, waker := range kv.rpcWakers {
			if waker.term != -1 && currentTerm != waker.term {
				kv.debug(" waking up %v for leadership lost", clientId)
				wakers = append(wakers, kv.rpcWakers[clientId])
				delete(kv.rpcWakers, clientId)
			}
		}
		kv.mu.Unlock()

		for _, waker := range wakers {
			waker.lost <- true
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *KVServer) readApplyMsg() {
	for applyMsg := range kv.applyCh {
		kv.debug("received applyMsg:%+v", applyMsg)

		if applyMsg.SnapshotValid {
			kv.readSnapshot(applyMsg.Snapshot)
			continue
		}

		op := applyMsg.Command.(Op)
		kv.executeOp(op)

		kv.checkRaftStateSize(applyMsg.CommandIndex)

		if kv.killed() {
			return
		}
	}
}

func (kv *KVServer) executeOp(op Op) {
	kv.mu.Lock()

	if result, ok := kv.latestResults[op.ClientId]; ok && op.Seq <= result.Seq {
		kv.mu.Unlock()
		return
	}

	kv.debug("executing op: %+v", op)

	value := ""
	switch op.Type {
	case PUT:
		kv.db[op.Key] = op.Value
		kv.debug("db: %v", kv.db)
	case APPEND:
		kv.db[op.Key] += op.Value
		kv.debug("db: %v", kv.db)
	case GET:
		value = kv.db[op.Key]
	}

	kv.latestResults[op.ClientId] = RequestResult{op.Seq, value}
	kv.debug("updated latestResult for %v to %+v", op.ClientId, kv.latestResults[op.ClientId])

	if waker, ok := kv.rpcWakers[op.ClientId]; ok && waker.seq == op.Seq {
		delete(kv.rpcWakers, op.ClientId)
		kv.mu.Unlock()
		kv.debug("waking up rpc for %v:%v", op.ClientId, op.Seq)
		waker.done <- value
		kv.debug("woke up rpc for %v:%v", op.ClientId, op.Seq)
		return
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	kv.debug("received GET request: %+v", args)
	if result, ok := kv.latestResults[args.ClientId]; ok && args.Seq <= result.Seq {
		// It's ok to return latest value to request with smaller seq
		// for this request must have been obselete, client only
		// send new request once it receives correct response
		// kv.info("result: %+v, args :%+v", result, args)
		kv.mu.Unlock()
		reply.Err = OK
		reply.Value = result.Value
		kv.debug("reply to %+v with latest value %v", args, result.Value)
		return
	}

	// Is is possible that op arrives before rpcWakers is created
	// if you put it after Start()
	// To prevent waker been cleared by checkLeadership()
	// initialize term to -1, suggesting waiting is not started

	done := make(chan string, 1)
	lost := make(chan bool, 1)

	// A new request arriving means client won't care about older replies
	// So just wake previous rpcs regardless of their replies
	// Now that the waker is still in the map, it has not receive done
	// signal. Also we are holding the lock so it is safe to write to
	// the channel
	oldWaker := kv.rpcWakers[args.ClientId]
	kv.rpcWakers[args.ClientId] = &RpcWaker{args.Seq, -1, done, lost}
	kv.mu.Unlock()

	if oldWaker != nil {
		kv.debug("waking up the old rpc for %v:%v", args.ClientId, oldWaker.seq)
		oldWaker.done <- ""
		kv.debug("woke up the old rpc %v:%v", args.ClientId, oldWaker.seq)
	}

	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     GET,
		Key:      args.Key,
	}

	kv.debug("start op: %+v for %+v", op, args)
	_, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.debug("is not leader for %+v", args)
		return
	}

	kv.debug("start waiting for %+v", args)
	kv.mu.Lock()
	// if op has not already been executed and delete the waker, change
	// the term of the waker
	if waker, ok := kv.rpcWakers[args.ClientId]; ok {
		waker.term = term
	}
	kv.mu.Unlock()

	select {
	case reply.Value = <-done:
		reply.Err = OK
	case <-lost:
		reply.Err = ErrWrongLeader
	}

	kv.debug("reply to %+v with %+v", args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opType := PUT
	if args.Op == "Append" {
		opType = APPEND
	}

	kv.mu.Lock()
	kv.debug("received %v request: %+v", opType, args)
	if result, ok := kv.latestResults[args.ClientId]; ok && args.Seq <= result.Seq {
		// It's ok to return latest value to request with smaller seq
		// for this request must have been obselete, client only
		// send new request once it receives correct response
		kv.mu.Unlock()
		reply.Err = OK
		kv.debug("reply to %+v with OK", args)
		return
	}

	// Is is possible that op arrives before rpcWakers is created
	// if you put it after Start()
	// To prevent waker been cleared by checkLeadership()
	// initialize term to -1, suggesting waiting is not started

	done := make(chan string, 1)
	lost := make(chan bool, 1)

	// A new request arriving means client won't care about older replies
	// So just wake previous rpcs regardless of their replies
	// Now that the waker is still in the map, it has not receive done
	// signal. Also we are holding the lock so it is safe to write to
	// the channel
	oldWaker := kv.rpcWakers[args.ClientId]
	kv.rpcWakers[args.ClientId] = &RpcWaker{args.Seq, -1, done, lost}
	kv.mu.Unlock()

	if oldWaker != nil {
		kv.debug("waking up the old rpc")
		oldWaker.done <- ""
		kv.debug("woke up the old rpc")
	}

	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     opType,
		Key:      args.Key,
		Value:    args.Value,
	}

	kv.debug("start op: %+v for %+v", op, args)
	_, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.debug("is not leader for %+v", args)
		return
	}

	kv.debug("start waiting for %+v", args)

	kv.mu.Lock()
	// if op has not already been executed and delete the waker, change
	// the term of the waker
	if waker, ok := kv.rpcWakers[args.ClientId]; ok {
		waker.term = term
	}
	kv.mu.Unlock()

	select {
	case <-done:
		reply.Err = OK
	case <-lost:
		reply.Err = ErrWrongLeader
	}
	kv.debug("reply to %+v with %+v", args, reply)
}

func (kv *KVServer) checkRaftStateSize(index int) {
	if kv.maxraftstate != -1 &&
		kv.persister.RaftStateSize() > kv.maxraftstate-100 {
		w := &bytes.Buffer{}
		e := labgob.NewEncoder(w)

		kv.mu.Lock()
		e.Encode(kv.db)
		e.Encode(kv.latestResults)
		kv.mu.Unlock()

		snapshot := w.Bytes()
		kv.rf.Snapshot(index, snapshot)
	}
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var latestResults map[int64]RequestResult

	if d.Decode(&db) != nil || d.Decode(&latestResults) != nil {
		kv.debug("failed to read snapshot")
	}

	kv.mu.Lock()
	kv.db = db
	kv.latestResults = latestResults
	kv.mu.Unlock()
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
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(make(map[string]string))
	labgob.Register(make(map[int64]RequestResult))

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.latestResults = make(map[int64]RequestResult)
	kv.rpcWakers = make(map[int64]*RpcWaker)
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.readApplyMsg()
	go kv.checkLeadership()

	return kv
}
