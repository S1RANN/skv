package shardctrler

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const DEBUG = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardCtrler) debug(format string, args ...interface{}) {
	args = append([]interface{}{sc.me}, args...)
	DPrintf("ShardCtrler %v "+format, args...)
}

type RpcWaker struct {
	seq  int // required when crashing, re-applying previous ops
	term int // without comparing seq may wrongly wake up rpcs
	done chan interface{}
	lost chan bool
}
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead       int32
	latestSeqs map[int64]int
	rpcWakers  map[int64]*RpcWaker

	configs []Config // indexed by config num
}

type OpType uint8

const (
	JOIN OpType = iota
	LEAVE
	MOVE
	QUERY
)

type Op struct {
	// Your data here.
	ClientId int64
	Seq      int
	Type     OpType
	Params   interface{}
}

func (sc *ShardCtrler) checkLeadership() {
	for !sc.killed() {
		currentTerm, _ := sc.rf.GetState()
		wakers := []*RpcWaker{}
		sc.mu.Lock()
		for clientId, waker := range sc.rpcWakers {
			if waker.term != -1 && currentTerm != waker.term {
				sc.debug(" waking up %v for leadership lost", clientId)
				wakers = append(wakers, sc.rpcWakers[clientId])
				delete(sc.rpcWakers, clientId)
			}
		}
		sc.mu.Unlock()

		for _, waker := range wakers {
			waker.lost <- true
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (sc *ShardCtrler) readApplyMsg() {
	for applyMsg := range sc.applyCh {
		sc.debug("received applyMsg:%+v", applyMsg)

		op := applyMsg.Command.(Op)
		sc.executeOp(op)

		if sc.killed() {
			return
		}
	}
}

func reallocate(shards, newGroups []int) {
	if len(newGroups) == 0 {
		return
	}

	sort.Slice(newGroups, func(i, j int) bool {
		return newGroups[i] < newGroups[j]
	})

	newGroupLen := len(newGroups)
	oldAllocation := map[int]int{}
	newAllocation := map[int]int{}
	totalGroups := map[int]bool{}

	for _, g := range shards {
		oldAllocation[g]++
		totalGroups[g] = true
	}

	for _, g := range newGroups {
		totalGroups[g] = true
	}

	sort.Slice(newGroups, func(i, j int) bool {
		return oldAllocation[newGroups[i]] > oldAllocation[newGroups[j]]
	})

	quotient := NShards / newGroupLen
	remainder := NShards % newGroupLen

	for _, g := range newGroups {
		newAllocation[g] = quotient
		if remainder > 0 {
			newAllocation[g]++
			remainder--
		}
	}

	increasingGroups := []int{}

	for g := range totalGroups {
		if newAllocation[g] > oldAllocation[g] {
			increasingGroups = append(increasingGroups, g)
		}
	}

	sort.Slice(increasingGroups, func(i, j int) bool {
		return increasingGroups[i] < increasingGroups[j]
	})

	for shardId, groupId := range shards {
		if oldAllocation[groupId] > newAllocation[groupId] {
			increasingGroupId := increasingGroups[0]
			shards[shardId] = increasingGroupId
			oldAllocation[groupId]--
			oldAllocation[increasingGroupId]++
			if oldAllocation[increasingGroupId] == newAllocation[increasingGroupId] {
				increasingGroups = increasingGroups[1:]
				if len(increasingGroups) == 0 {
					break
				}
			}
		}
	}
}

func (sc *ShardCtrler) executeOp(op Op) {
	sc.mu.Lock()

	if seq, ok := sc.latestSeqs[op.ClientId]; ok && op.Seq <= seq {
		sc.mu.Unlock()
		return
	}

	sc.debug("executing op: %+v", op)

	var result interface{}
	result = true
	switch op.Type {
	case JOIN:
		sc.join(op.Params.(map[int][]string))
	case LEAVE:
		sc.leave(op.Params.([]int))
	case MOVE:
		sc.move(op.Params.(MovePair))
	case QUERY:
		result = sc.query(op.Params.(int))
	}

	sc.latestSeqs[op.ClientId] = op.Seq
	sc.debug("updated latestResult for %v to %+v", op.ClientId, sc.latestSeqs[op.ClientId])

	if waker, ok := sc.rpcWakers[op.ClientId]; ok && waker.seq == op.Seq {
		delete(sc.rpcWakers, op.ClientId)
		sc.mu.Unlock()
		sc.debug("waking up rpc for %v:%v", op.ClientId, op.Seq)
		waker.done <- result
		sc.debug("woke up rpc for %v:%v", op.ClientId, op.Seq)
		return
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) join(servers map[int][]string) {
	configNum := len(sc.configs) - 1

	newConfig := Config{
		Num:    configNum + 1,
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}

	copy(newConfig.Shards[:], sc.configs[configNum].Shards[:])

	newGroups := []int{}

	for groupId, servers := range sc.configs[configNum].Groups {
		newGroups = append(newGroups, groupId)
		s := make([]string, len(servers))
		copy(s, servers)
		newConfig.Groups[groupId] = s
	}

	for groupId, servers := range servers {
		newGroups = append(newGroups, groupId)
		newConfig.Groups[groupId] = servers
	}

	reallocate(newConfig.Shards[:], newGroups)

	sc.configs = append(sc.configs, newConfig)

	sc.debug("after JOIN: %+v", sc.configs[configNum+1].Shards)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()

	sc.debug("received JOIN request: %+v", args)
	if seq, ok := sc.latestSeqs[args.ClientId]; ok && args.Seq <= seq {
		// It's ok to return latest value to request with smaller seq
		// for this request must have been obselete, client only
		// send new request once it receives correct response
		// kv.info("result: %+v, args :%+v", result, args)
		sc.mu.Unlock()
		return
	}

	// Is is possible that op arrives before rpcWakers is created
	// if you put it after Start()
	// To prevent waker been cleared by checkLeadership()
	// initialize term to -1, suggesting waiting is not started

	done := make(chan interface{}, 1)
	lost := make(chan bool, 1)

	// A new request arriving means client won't care about older replies
	// So just wake previous rpcs regardless of their replies
	// Now that the waker is still in the map, it has not receive done
	// signal. Also we are holding the lock so it is safe to write to
	// the channel
	oldWaker := sc.rpcWakers[args.ClientId]
	sc.rpcWakers[args.ClientId] = &RpcWaker{args.Seq, -1, done, lost}
	sc.mu.Unlock()

	if oldWaker != nil {
		sc.debug("waking up the old rpc for %v:%v", args.ClientId, oldWaker.seq)
		oldWaker.done <- true
		sc.debug("woke up the old rpc %v:%v", args.ClientId, oldWaker.seq)
	}

	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     JOIN,
		Params:   args.Servers,
	}

	sc.debug("start op: %+v for %+v", op, args)
	_, term, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		sc.debug("is not leader for %+v", args)
		return
	}

	sc.mu.Lock()
	// if op has not already been executed and delete the waker, change
	// the term of the waker
	if waker, ok := sc.rpcWakers[args.ClientId]; ok {
		waker.term = term
	}
	sc.mu.Unlock()

	sc.debug("start waiting for %+v", args)

	select {
	case <-done:
		reply.WrongLeader = false
	case <-lost:
		reply.WrongLeader = true
	}

	sc.debug("reply to %+v with %+v", args, reply)
}

func (sc *ShardCtrler) leave(gids []int) {
	configNum := len(sc.configs) - 1

	newConfig := Config{
		Num:    configNum + 1,
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}

	copy(newConfig.Shards[:], sc.configs[configNum].Shards[:])

	newGroups := []int{}

	for groupId, servers := range sc.configs[configNum].Groups {
		s := make([]string, len(servers))
		copy(s, servers)
		newConfig.Groups[groupId] = s
	}

	for _, groupId := range gids {
		delete(newConfig.Groups, groupId)
	}

	for g := range newConfig.Groups {
		newGroups = append(newGroups, g)
	}

	reallocate(newConfig.Shards[:], newGroups)

	sc.configs = append(sc.configs, newConfig)
	sc.debug("after LEAVE: %+v", sc.configs[configNum+1].Shards)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()

	sc.debug("received LEAVE request: %+v", args)
	if seq, ok := sc.latestSeqs[args.ClientId]; ok && args.Seq <= seq {
		// It's ok to return latest value to request with smaller seq
		// for this request must have been obselete, client only
		// send new request once it receives correct response
		// kv.info("result: %+v, args :%+v", result, args)
		sc.mu.Unlock()
		return
	}

	// Is is possible that op arrives before rpcWakers is created
	// if you put it after Start()
	// To prevent waker been cleared by checkLeadership()
	// initialize term to -1, suggesting waiting is not started

	done := make(chan interface{}, 1)
	lost := make(chan bool, 1)

	// A new request arriving means client won't care about older replies
	// So just wake previous rpcs regardless of their replies
	// Now that the waker is still in the map, it has not receive done
	// signal. Also we are holding the lock so it is safe to write to
	// the channel
	oldWaker := sc.rpcWakers[args.ClientId]
	sc.rpcWakers[args.ClientId] = &RpcWaker{args.Seq, -1, done, lost}
	sc.mu.Unlock()

	if oldWaker != nil {
		sc.debug("waking up the old rpc for %v:%v", args.ClientId, oldWaker.seq)
		oldWaker.done <- true
		sc.debug("woke up the old rpc %v:%v", args.ClientId, oldWaker.seq)
	}

	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     LEAVE,
		Params:   args.GIDs,
	}

	sc.debug("start op: %+v for %+v", op, args)
	_, term, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		sc.debug("is not leader for %+v", args)
		return
	}

	sc.debug("start waiting for %+v", args)
	sc.mu.Lock()
	// if op has not already been executed and delete the waker, change
	// the term of the waker
	if waker, ok := sc.rpcWakers[args.ClientId]; ok {
		waker.term = term
	}
	sc.mu.Unlock()

	select {
	case <-done:
		reply.WrongLeader = false
	case <-lost:
		reply.WrongLeader = true
	}
	sc.debug("reply to %+v with %+v", args, reply)
}

type MovePair struct {
	Shard int
	GID   int
}

func (sc *ShardCtrler) move(pair MovePair) {
	configNum := len(sc.configs) - 1

	newConfig := Config{
		Num:    configNum + 1,
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}

	copy(newConfig.Shards[:], sc.configs[configNum].Shards[:])

	for groupId, servers := range sc.configs[configNum].Groups {
		s := make([]string, len(servers))
		copy(s, servers)
		newConfig.Groups[groupId] = s
	}

	newConfig.Shards[pair.Shard] = pair.GID

	sc.configs = append(sc.configs, newConfig)

	sc.debug("after MOVE: %+v", sc.configs[configNum+1].Shards)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()

	sc.debug("received MOVE request: %+v", args)
	if seq, ok := sc.latestSeqs[args.ClientId]; ok && args.Seq <= seq {
		// It's ok to return latest value to request with smaller seq
		// for this request must have been obselete, client only
		// send new request once it receives correct response
		// kv.info("result: %+v, args :%+v", result, args)
		sc.mu.Unlock()
		return
	}

	// Is is possible that op arrives before rpcWakers is created
	// if you put it after Start()
	// To prevent waker been cleared by checkLeadership()
	// initialize term to -1, suggesting waiting is not started

	done := make(chan interface{}, 1)
	lost := make(chan bool, 1)

	// A new request arriving means client won't care about older replies
	// So just wake previous rpcs regardless of their replies
	// Now that the waker is still in the map, it has not receive done
	// signal. Also we are holding the lock so it is safe to write to
	// the channel
	oldWaker := sc.rpcWakers[args.ClientId]
	sc.rpcWakers[args.ClientId] = &RpcWaker{args.Seq, -1, done, lost}
	sc.mu.Unlock()

	if oldWaker != nil {
		sc.debug("waking up the old rpc for %v:%v", args.ClientId, oldWaker.seq)
		oldWaker.done <- true
		sc.debug("woke up the old rpc %v:%v", args.ClientId, oldWaker.seq)
	}

	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     MOVE,
		Params:   MovePair{args.Shard, args.GID},
	}

	sc.debug("start op: %+v for %+v", op, args)
	_, term, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		sc.debug("is not leader for %+v", args)
		return
	}

	sc.debug("start waiting for %+v", args)
	sc.mu.Lock()
	// if op has not already been executed and delete the waker, change
	// the term of the waker
	if waker, ok := sc.rpcWakers[args.ClientId]; ok {
		waker.term = term
	}
	sc.mu.Unlock()

	select {
	case <-done:
		reply.WrongLeader = false
	case <-lost:
		reply.WrongLeader = true
	}
	sc.debug("reply to %+v with %+v", args, reply)
}

func (sc *ShardCtrler) query(num int) Config {
	// Your code here.
	configNum := len(sc.configs) - 1

	if num == -1 || num > configNum {
		return sc.configs[configNum]
	}

	return sc.configs[num]
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()

	sc.debug("received QUERY request: %+v", args)
	if seq, ok := sc.latestSeqs[args.ClientId]; ok && args.Seq <= seq {
		// It's ok to return latest value to request with smaller seq
		// for this request must have been obselete, client only
		// send new request once it receives correct response
		// kv.info("result: %+v, args :%+v", result, args)
		sc.mu.Unlock()
		return
	}

	// Is is possible that op arrives before rpcWakers is created
	// if you put it after Start()
	// To prevent waker been cleared by checkLeadership()
	// initialize term to -1, suggesting waiting is not started

	done := make(chan interface{}, 1)
	lost := make(chan bool, 1)

	// A new request arriving means client won't care about older replies
	// So just wake previous rpcs regardless of their replies
	// Now that the waker is still in the map, it has not receive done
	// signal. Also we are holding the lock so it is safe to write to
	// the channel
	oldWaker := sc.rpcWakers[args.ClientId]
	sc.rpcWakers[args.ClientId] = &RpcWaker{args.Seq, -1, done, lost}
	sc.mu.Unlock()

	if oldWaker != nil {
		sc.debug("waking up the old rpc for %v:%v", args.ClientId, oldWaker.seq)
		oldWaker.done <- Config{}
		sc.debug("woke up the old rpc %v:%v", args.ClientId, oldWaker.seq)
	}

	op := Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Type:     QUERY,
		Params:   args.Num,
	}

	sc.debug("start op: %+v for %+v", op, args)
	_, term, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		sc.debug("is not leader for %+v", args)
		return
	}

	sc.debug("start waiting for %+v", args)
	sc.mu.Lock()
	// if op has not already been executed and delete the waker, change
	// the term of the waker
	if waker, ok := sc.rpcWakers[args.ClientId]; ok {
		waker.term = term
	}
	sc.mu.Unlock()

	select {
	case c := <-done:
		reply.Config = c.(Config)
		reply.WrongLeader = false
	case <-lost:
		reply.WrongLeader = true
	}

	sc.debug("reply to %+v with %+v", args, reply)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(map[int][]string{})
	labgob.Register(MovePair{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.latestSeqs = make(map[int64]int)
	sc.rpcWakers = map[int64]*RpcWaker{}

	go sc.readApplyMsg()
	go sc.checkLeadership()

	return sc
}
