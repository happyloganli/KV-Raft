package kvsrv

import (
	"6.5840/labrpc"
	"fmt"
	"sync/atomic"
	"time"
)

type Clerk struct {
	server         *labrpc.ClientEnd
	requestCounter uint32
	clientId       string
}

func (ck *Clerk) getRequestId() string {
	atomic.AddUint32(&ck.requestCounter, 1)
	return fmt.Sprintf("%s-%d", ck.clientId, ck.requestCounter)
}

func generateClientID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.requestCounter = 0
	ck.clientId = generateClientID()
	return ck
}

func (ck *Clerk) Get(key string) string {
	requestId := ck.getRequestId()
	getRequest := GetArgs{Key: key, RequestId: requestId}
	getReply := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &getRequest, &getReply)
		if ok && getReply.Ok {
			ackRequest := AckArgs{RequestId: requestId}
			ackReply := AckReply{}
			ck.server.Call("KVServer.Ack", &ackRequest, &ackReply)
			return getReply.Value
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	putAppendReply := PutAppendReply{}
	requestId := ck.getRequestId()
	putAppendRequest := PutAppendArgs{Key: key, Value: value, RequestId: requestId}
	ok := false
	for {
		switch op {
		case "Put":
			ok = ck.server.Call("KVServer.Put", &putAppendRequest, &putAppendReply)
		case "Append":
			ok = ck.server.Call("KVServer.Append", &putAppendRequest, &putAppendReply)
		}
		if ok && putAppendReply.Ok {
			ackRequest := AckArgs{RequestId: requestId}
			ackReply := AckReply{}
			ck.server.Call("KVServer.Ack", &ackRequest, &ackReply)
			return putAppendReply.Value
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {

	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
