package shardkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

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
	RECONF
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int
	Type     OpType
	Params   interface{}
}

type KVPair struct {
	Key   string
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
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

func (kv *ShardKV) debug(format string, args ...interface{}) {
	args = append([]interface{}{kv.me}, args...)
	DPrintf("KVServer %v "+format, args...)
}

func (kv *ShardKV) checkLeadership() {
	for {
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

func (kv *ShardKV) readApplyMsg() {
	for applyMsg := range kv.applyCh {
		kv.debug("received applyMsg:%+v", applyMsg)

		if applyMsg.SnapshotValid {
			kv.readSnapshot(applyMsg.Snapshot)
			continue
		}

		kv.mu.Lock()
		kv.debug("before execute latestResult: %+v", kv.latestResults)
		kv.mu.Unlock()

		op := applyMsg.Command.(Op)
		kv.executeOp(op)

		kv.mu.Lock()
		kv.debug("after execute latestResult: %+v", kv.latestResults)
		kv.mu.Unlock()

		kv.checkRaftStateSize(applyMsg.CommandIndex)
	}
}

func (kv *ShardKV) executeOp(op Op) {
	kv.mu.Lock()

	if result, ok := kv.latestResults[op.ClientId]; ok && op.Seq <= result.Seq {
		kv.mu.Unlock()
		return
	}

	kv.debug("executing op: %+v", op)

	value := ""
	switch op.Type {
	case PUT:
		pair := op.Params.(KVPair)
		kv.db[pair.Key] = pair.Value
		kv.debug("db: %v", kv.db)
	case APPEND:
		pair := op.Params.(KVPair)
		kv.db[pair.Key] += pair.Value
		kv.debug("db: %v", kv.db)
	case GET:
		key := op.Params.(string)
		value = kv.db[key]
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
		Params:   args.Key,
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
		Params:   KVPair{args.Key, args.Value},
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

func (kv *ShardKV) checkRaftStateSize(index int) {
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

func (kv *ShardKV) readSnapshot(snapshot []byte) {
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

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(make(map[string]string))
	labgob.Register(make(map[int64]RequestResult))
	labgob.Register(KVPair{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.latestResults = make(map[int64]RequestResult)
	kv.rpcWakers = make(map[int64]*RpcWaker)
	kv.persister = persister

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.readApplyMsg()
	go kv.checkLeadership()

	return kv
}
