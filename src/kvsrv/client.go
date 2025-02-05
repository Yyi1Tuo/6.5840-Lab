package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"
import "fmt"

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
	fmt.Println("Trying to Get")

	ck.mu.Lock()
	ck.seq++
	privateSeq:=ck.seq//防止重传时拿到了更新过的seq，所以提前把序列保存到本地
	ck.mu.Unlock()

	for{
		args:=GetArgs{Key:key,Seq:privateSeq}
		reply:=GetReply{}
		ck.server.Call("KVServer.Get",&args,&reply)
		if reply.Value!=""{
			//汇报请求完成
			for{
				reportDoneArgs:=ReportDoneArgs{Seq:privateSeq}
				reportDoneReply:=ReportDoneReply{}	
				ck.server.Call("KVServer.ReportDone",&reportDoneArgs,&reportDoneReply)
				if reportDoneReply.Seq==privateSeq{
					break;
				}
				time.Sleep(100*time.Millisecond)
			}
			fmt.Println("Done Get")
			return reply.Value
		}//如果reply.Value不为空，说明已经获取到值，直接返回
		time.Sleep(100*time.Millisecond)//如果reply.Value为空，说明还没有获取到值，等待100ms后重试
	}
	
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
	// You will have to modify this function.
	ck.mu.Lock()
	ck.seq++
	privateSeq:=ck.seq//防止重传时拿到了更新过的seq，所以提前把序列保存到本地
	ck.mu.Unlock()

	switch op{
	case "Put":
		{
			for{
				args:=PutAppendArgs{Key:key,Value:value,Seq:privateSeq}
				reply:=PutAppendReply{}
				ck.server.Call("KVServer.Put",&args,&reply)
				if reply.Value!=""{
					//汇报请求完成
					for{
						reportDoneArgs:=ReportDoneArgs{Seq:privateSeq}
						reportDoneReply:=ReportDoneReply{}	
						ck.server.Call("KVServer.ReportDone",&reportDoneArgs,&reportDoneReply)
						if reportDoneReply.Seq==privateSeq{
							break;
						}
						time.Sleep(100*time.Millisecond)
					}
					break;
				}
				time.Sleep(100*time.Millisecond)
			}
			return ""
		}
	case "Append":
		{
			for{
				args:=PutAppendArgs{Key:key,Value:value,Seq:privateSeq}
				reply:=PutAppendReply{}
				ck.server.Call("KVServer.Append",&args,&reply)
				if reply.Value!=""{
					//汇报请求完成
					for{
						reportDoneArgs:=ReportDoneArgs{Seq:privateSeq}
						reportDoneReply:=ReportDoneReply{}	
						ck.server.Call("KVServer.ReportDone",&reportDoneArgs,&reportDoneReply)
						if reportDoneReply.Seq==privateSeq{
							break;
						}
						time.Sleep(100*time.Millisecond)
					}
					return reply.Value
				}
				time.Sleep(100*time.Millisecond)
			}
			
		}
	}
	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
