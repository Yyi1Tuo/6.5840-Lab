package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync"

//import "fmt"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	seq int//为了实现linearizability，需要实现一个单调递增的序列号，用来唯一确定操作的顺序
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.seq=0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//fmt.Println("Trying to Get")
	//fmt.Println("clerk key is:",key)
	ck.mu.Lock()
	ck.seq++
	privateSeq:=ck.seq//防止重传时拿到了更新过的seq，所以提前把序列保存到本地
	ck.mu.Unlock()
    args:=GetArgs{Key:key,Seq:privateSeq}
	reply:=GetReply{}
	for !ck.server.Call("KVServer.Get",&args,&reply){}//一直尝试
	reportDoneArgs:=ReportDoneArgs{Seq:privateSeq}
	reportDoneReply:=ReportDoneReply{}
	for !ck.server.Call("KVServer.ReportDone",&reportDoneArgs,&reportDoneReply){}
	return reply.Value
	
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.mu.Lock()
	ck.seq++
	privateSeq:=ck.seq//防止重传时拿到了更新过的seq，所以提前把序列保存到本地
	ck.mu.Unlock()
	
	for {
		var args PutAppendArgs
		var reply PutAppendReply
		var reportDoneArgs ReportDoneArgs
		var reportDoneReply ReportDoneReply

		reportDoneArgs = ReportDoneArgs{Seq: privateSeq}
		args = PutAppendArgs{Key: key, Value: value, Seq: privateSeq}
		
		if op == "Put" {
			for !ck.server.Call("KVServer.Put",&args,&reply){}
			for !ck.server.Call("KVServer.ReportDone",&reportDoneArgs,&reportDoneReply){}
			return ""
		} else if op == "Append" {
			for !ck.server.Call("KVServer.Append",&args,&reply){}
			for !ck.server.Call("KVServer.ReportDone",&reportDoneArgs,&reportDoneReply){}
			return reply.Value
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	//fmt.Println("Trying to put :",key,"with value:",value)
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	//fmt.Println("Trying to append",key,"with value:",value)
	return ck.PutAppend(key, value, "Append")
}
