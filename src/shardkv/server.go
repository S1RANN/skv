package shardkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const DEBUG = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG {
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

func (ot OpType) String() string {
	switch ot {
	case PUT:
		return "PUT"
	case APPEND:
		return "APPEND"
	case GET:
		return "GET"
	case RECONF:
		return "RECONF"
	}
	return "UNKNOWN"
}

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
	stages        []ReconfStage
	ck            *shardctrler.Clerk
}

type ReconfStage struct {
	Config        shardctrler.Config
	DroppedShards map[int]map[string]string
	LatestResults map[int64]RequestResult
}

type RpcWaker struct {
	seq         int // required when crashing, re-applying previous ops
	term        int // without comparing seq may wrongly wake up rpcs
	done        chan string
	wrongLeader chan bool
	wrongGroup  chan bool
}

type RequestResult struct {
	Seq   int
	Value string
}

func (kv *ShardKV) debug(format string, args ...interface{}) {
	args = append([]interface{}{kv.gid, kv.me, kv.stages[len(kv.stages)-1].Config.Num}, args...)
	DPrintf("[\033[32m%v\033[0m:\033[33m%v\033[0m|\033[31m%v\033[0m] "+format, args...)
}

func (kv *ShardKV) checkReconfiguration() {
	for {
		kv.mu.Lock()
		latestConfigNum := kv.stages[len(kv.stages)-1].Config.Num
		kv.mu.Unlock()
		conf := kv.ck.Query(latestConfigNum + 1)

		if conf.Num > latestConfigNum {
			op := Op{
				ClientId: -1,
				Seq:      conf.Num,
				Type:     RECONF,
				Params:   conf,
			}

			kv.mu.Lock()
			kv.debug("detected configuration update %v > %v, start op: %+v", conf.Num, latestConfigNum, op)
			kv.mu.Unlock()

			kv.rf.Start(op)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) checkLeadership() {
	for {
		currentTerm, _ := kv.rf.GetState()
		wakers := []*RpcWaker{}
		kv.mu.Lock()
		for clientId, waker := range kv.rpcWakers {
			if waker.term != -1 && currentTerm != waker.term {
				kv.debug("waking up %v for leadership lost", clientId)
				wakers = append(wakers, kv.rpcWakers[clientId])
				delete(kv.rpcWakers, clientId)
			}
		}
		kv.mu.Unlock()

		for _, waker := range wakers {
			waker.wrongLeader <- true
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) readApplyMsg() {
	for applyMsg := range kv.applyCh {
		if applyMsg.SnapshotValid {
			kv.readSnapshot(applyMsg.Snapshot)
			continue
		}

		op := applyMsg.Command.(Op)
		kv.executeOp(op)

		kv.checkRaftStateSize(applyMsg.CommandIndex)
	}
}

func (kv *ShardKV) executeOp(op Op) {
	kv.mu.Lock()

	kv.debug("checking op: %+v", op)

	if result, ok := kv.latestResults[op.ClientId]; ok && op.Seq <= result.Seq {
		// It's possible that a request arrives before reconfiguration done.
		// In this case the latestRequest.Seq for that clientId is yet to be updated.
		// Hence this request will pass the first check.
		// Here we need to double check this and wake up the rpc if there exists one.
		kv.debug("op.Seq %v <= result.Seq %v, ignored", op.Seq, result.Seq)
		if waker, ok := kv.rpcWakers[op.ClientId]; ok && waker.seq == op.Seq {
			value := kv.latestResults[op.ClientId].Value
			delete(kv.rpcWakers, op.ClientId)
			kv.debug("waking up waker for %v:%v with done", op.ClientId, op.Seq)
			kv.mu.Unlock()
			waker.done <- value
			kv.mu.Lock()
			kv.debug("woke up waker for %v:%v with done", op.ClientId, op.Seq)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		return
	}

	kv.debug("executing op: %+v", op)

	if op.Type == RECONF {
		oldStage := kv.stages[len(kv.stages)-1]
		newConfig := op.Params.(shardctrler.Config)

		oldShards, newShards := map[int]bool{}, map[int]bool{}
		newlyOwnedShards := map[int]bool{}
		droppedShards := map[int]map[string]string{}

		for shardId, groupId := range oldStage.Config.Shards {
			if groupId == kv.gid {
				oldShards[shardId] = true
			}
		}
		for shardId, groupId := range newConfig.Shards {
			if groupId == kv.gid {
				newShards[shardId] = true
			}
		}

		for shardId := range newShards {
			if _, ok := oldShards[shardId]; !ok {
				newlyOwnedShards[shardId] = true
			}
		}

		for shardId := range oldShards {
			if _, ok := newShards[shardId]; !ok {
				droppedShards[shardId] = map[string]string{}
			}
		}

		for k, v := range kv.db {
			shardId := key2shard(k)
			if shards, ok := droppedShards[shardId]; ok {
				delete(kv.db, k)
				shards[k] = v
			}
		}

		newStage := ReconfStage{
			Config:        newConfig,
			DroppedShards: droppedShards,
			LatestResults: map[int64]RequestResult{},
		}

		newStage.Config.Groups = map[int][]string{}

		for groupId, shards := range newConfig.Groups {
			s := make([]string, len(shards))
			copy(s, shards)
			newStage.Config.Groups[groupId] = s
		}

		for k, v := range kv.latestResults {
			newStage.LatestResults[k] = v
		}

		kv.stages = append(kv.stages, newStage)

		kv.debug("updated config to %v", newConfig.Num)
		kv.debug("now has stages: %+v", kv.stages)
		kv.debug("now owns extra shards: %v, no longer owns shards: %v", newlyOwnedShards, droppedShards)
		kv.debug("starts waiting for shard migration")

		kv.mu.Unlock()

		shardsToMigrateIn := []int{}

		for shardId := range newlyOwnedShards {
			groupIdToAsk := oldStage.Config.Shards[shardId]
			if groupIdToAsk != 0 {
				shardsToMigrateIn = append(shardsToMigrateIn, shardId)
			}
		}

		wg := &sync.WaitGroup{}
		wg.Add(len(shardsToMigrateIn))

		for _, shardId := range shardsToMigrateIn {
			groupIdToAsk := oldStage.Config.Shards[shardId]
			groupNamesToAsk := oldStage.Config.Groups[groupIdToAsk]

			ends := []*labrpc.ClientEnd{}

			for _, groupName := range groupNamesToAsk {
				ends = append(ends, kv.make_end(groupName))
			}

			args := &MigrateArgs{shardId, newConfig.Num}

			kv.mu.Lock()
			kv.debug("launching Migrate requests for shard %v to group %v", shardId, groupIdToAsk)
			kv.mu.Unlock()

			go func() {
				defer wg.Done()

				for {
					for _, end := range ends {
						reply := &MigrateReply{}
						ok := kv.sendMigrate(end, args, reply)
						if ok && reply.Err == OK {
							kv.mu.Lock()
							kv.debug("received shard %v", args.ShardId)

							oldLatestResults := map[int64]RequestResult{}

							for k, v := range kv.latestResults {
								oldLatestResults[k] = v
							}

							for k, v := range reply.Pairs {
								kv.db[k] = v
							}
							for clientId, incomingResult := range reply.LatestResults {
								if clientId == -1 {
									continue
								}
								if myResult, ok := kv.latestResults[clientId]; ok {
									if myResult.Seq < incomingResult.Seq {
										kv.latestResults[clientId] = incomingResult
									}
								} else {
									kv.latestResults[clientId] = incomingResult
								}
							}

							kv.debug("oldLatestResult: %v", oldLatestResults)
							kv.debug("latestResult: %v", kv.latestResults)

							kv.mu.Unlock()
							return
						}
					}
					time.Sleep(200 * time.Millisecond)
				}
			}()
		}

		wg.Wait()

		kv.mu.Lock()
		kv.debug("completed shard migration")
		kv.latestResults[op.ClientId] = RequestResult{op.Seq, ""}
		kv.debug("updated latestResult for %v to %+v", op.ClientId, kv.latestResults[op.ClientId])
		kv.mu.Unlock()
	} else {
		key := ""
		if op.Type == PUT || op.Type == APPEND {
			key = op.Params.(KVPair).Key
		} else {
			key = op.Params.(string)
		}

		shardId := key2shard(key)
		if kv.stages[len(kv.stages)-1].Config.Shards[shardId] != kv.gid {
			kv.debug("does not own shard %v for %v, ignored op", shardId, key)
			if waker, ok := kv.rpcWakers[op.ClientId]; ok && waker.seq == op.Seq {
				delete(kv.rpcWakers, op.ClientId)
				kv.debug("waking up waker for %v:%v with wrongGroup", op.ClientId, op.Seq)
				kv.mu.Unlock()
				waker.wrongGroup <- true
				kv.mu.Lock()
				kv.debug("woke up waker for %v:%v with wrongGroup", op.ClientId, op.Seq)
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
			return
		}

		value := ""
		switch op.Type {
		case PUT:
			pair := op.Params.(KVPair)
			kv.db[pair.Key] = pair.Value
		case APPEND:
			pair := op.Params.(KVPair)
			kv.db[pair.Key] += pair.Value
		case GET:
			key := op.Params.(string)
			value = kv.db[key]
		}

		kv.latestResults[op.ClientId] = RequestResult{op.Seq, value}
		kv.debug("updated latestResult for %v to %+v", op.ClientId, kv.latestResults[op.ClientId])

		if waker, ok := kv.rpcWakers[op.ClientId]; ok && waker.seq == op.Seq {
			delete(kv.rpcWakers, op.ClientId)
			kv.debug("waking up waker for %v:%v with done", op.ClientId, op.Seq)
			kv.mu.Unlock()
			waker.done <- value
			kv.mu.Lock()
			kv.debug("woke up waker for %v:%v with done", op.ClientId, op.Seq)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
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
		kv.debug("reply to %+v with latest value %v", args, result.Value)
		kv.mu.Unlock()
		reply.Err = OK
		reply.Value = result.Value
		return
	}

	// Is is possible that op arrives before rpcWakers is created
	// if you put it after Start()
	// To prevent waker been cleared by checkLeadership()
	// initialize term to -1, suggesting waiting is not started

	done := make(chan string, 1)
	wrongLeader := make(chan bool, 1)
	wrongGroup := make(chan bool, 1)

	// A new request arriving means client won't care about older replies
	// So just wake previous rpcs regardless of their replies
	// Now that the waker is still in the map, it has not receive done
	// signal. Also we are holding the lock so it is safe to write to
	// the channel
	oldWaker := kv.rpcWakers[args.ClientId]
	kv.rpcWakers[args.ClientId] = &RpcWaker{args.Seq, -1, done, wrongLeader, wrongGroup}

	if oldWaker != nil {
		oldWaker.done <- ""
	}

	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     GET,
		Params:   args.Key,
	}

	kv.debug("start op: %+v for %+v", op, args)
	kv.mu.Unlock()

	_, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		kv.debug("is not leader for %+v", args)
		kv.mu.Unlock()
		return
	}

	kv.mu.Lock()
	kv.debug("start waiting for %+v", args)
	// if op has not already been executed and delete the waker, change
	// the term of the waker
	if waker, ok := kv.rpcWakers[args.ClientId]; ok {
		waker.term = term
	}
	kv.mu.Unlock()

	select {
	case reply.Value = <-done:
		reply.Err = OK
	case <-wrongLeader:
		reply.Err = ErrWrongLeader
	case <-wrongGroup:
		reply.Err = ErrWrongGroup
	}

	kv.mu.Lock()
	kv.debug("reply to %+v with %+v", args, reply)
	kv.mu.Unlock()
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
		kv.debug("reply to %+v with OK", args)
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	// Is is possible that op arrives before rpcWakers is created
	// if you put it after Start()
	// To prevent waker been cleared by checkLeadership()
	// initialize term to -1, suggesting waiting is not started

	done := make(chan string, 1)
	wrongLeader := make(chan bool, 1)
	wrongGroup := make(chan bool, 1)

	// A new request arriving means client won't care about older replies
	// So just wake previous rpcs regardless of their replies
	// Now that the waker is still in the map, it has not receive done
	// signal. Also we are holding the lock so it is safe to write to
	// the channel
	oldWaker := kv.rpcWakers[args.ClientId]
	kv.rpcWakers[args.ClientId] = &RpcWaker{args.Seq, -1, done, wrongLeader, wrongGroup}

	if oldWaker != nil {
		oldWaker.done <- ""
	}

	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     opType,
		Params:   KVPair{args.Key, args.Value},
	}

	kv.debug("start op: %+v for %+v", op, args)
	kv.mu.Unlock()

	_, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		kv.debug("is not leader for %+v", args)
		kv.mu.Unlock()
		return
	}

	kv.mu.Lock()
	kv.debug("start waiting for %+v", args)
	// if op has not already been executed and delete the waker, change
	// the term of the waker
	if waker, ok := kv.rpcWakers[args.ClientId]; ok {
		waker.term = term
	}
	kv.mu.Unlock()

	select {
	case <-done:
		reply.Err = OK
	case <-wrongLeader:
		reply.Err = ErrWrongLeader
	case <-wrongGroup:
		reply.Err = ErrWrongGroup
	}
	kv.mu.Lock()
	kv.debug("reply to %+v with %+v", args, reply)
	kv.mu.Unlock()
}

func (kv *ShardKV) sendMigrate(end *labrpc.ClientEnd, args *MigrateArgs, reply *MigrateReply) bool {
	return end.Call("ShardKV.Migrate", args, reply)
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.debug("received MIGRATE request %v:%v", args.ConfigNum, args.ShardId)

	for _, stage := range kv.stages {
		if stage.Config.Num == args.ConfigNum {
			if shards, ok := stage.DroppedShards[args.ShardId]; ok {
				reply.Err = OK
				reply.Pairs = shards
				reply.LatestResults = stage.LatestResults
				kv.debug("reply to MIGRATE request %v:%v with %+v", args.ConfigNum, args.ShardId, reply)
				return
			}
		}
	}

	kv.debug("does not have shard %v in dropped shards in config %v", args.ShardId, args.ConfigNum)
	reply.Err = ErrWrongLeader
}

func (kv *ShardKV) checkRaftStateSize(index int) {
	if kv.maxraftstate != -1 &&
		kv.persister.RaftStateSize() > kv.maxraftstate-100 {
		w := &bytes.Buffer{}
		e := labgob.NewEncoder(w)

		kv.mu.Lock()
		e.Encode(kv.db)
		e.Encode(kv.latestResults)
		e.Encode(kv.stages)
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
	var stages []ReconfStage

	if d.Decode(&db) != nil || d.Decode(&latestResults) != nil || d.Decode(&stages) != nil {
		kv.mu.Lock()
		kv.debug("failed to read snapshot")
		kv.mu.Unlock()
	}

	kv.mu.Lock()
	kv.db = db
	kv.latestResults = latestResults
	kv.stages = stages
	kv.debug("done read snapshot")
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
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.stages = []ReconfStage{{
		Config: shardctrler.Config{
			Num:    0,
			Shards: [10]int{},
			Groups: nil,
		},
		DroppedShards: nil,
	}}
	kv.ck = shardctrler.MakeClerk(ctrlers)

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
	go kv.checkReconfiguration()

	return kv
}
