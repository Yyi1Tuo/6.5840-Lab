package kvsrv

import (
	"log"
	"sync"
)

const Debug = false
const (
	RequestPhase=0
	ReplyPhase=1
)
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	kv map[string]string
	// Your definitions here.
	seqMap map[int]int //记录每个请求的执行状态，以便面对重复请求时，能够正确处理
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value=kv.kv[args.Key]
	kv.seqMap[args.Seq]=ReplyPhase
	reply.Seq=args.Seq
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kv[args.Key]=args.Value
	reply.Value=args.Value
	kv.seqMap[args.Seq]=ReplyPhase
	reply.Seq=args.Seq
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value=kv.kv[args.Key]
	kv.kv[args.Key]+=args.Value
	kv.seqMap[args.Seq]=ReplyPhase
	reply.Seq=args.Seq
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kv=make(map[string]string)
	kv.seqMap=make(map[int]int)
	// You may need initialization code here.

	return kv
}
