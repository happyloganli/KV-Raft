package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu    sync.Mutex
	data  map[string]string
	reply map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if result, exists := kv.reply[args.RequestId]; exists {
		reply.Value = result
		reply.Ok = true
		return
	}

	value, exist := kv.data[args.Key]
	if !exist {
		value = ""
	}

	reply.Value = value
	reply.Ok = true
	kv.reply[args.RequestId] = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exists := kv.reply[args.RequestId]; exists {
		reply.Ok = true
		return
	}

	kv.data[args.Key] = args.Value
	reply.Ok = true
	kv.reply[args.RequestId] = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, exists := kv.reply[args.RequestId]; exists {
		reply.Ok = true
		reply.Value = value
		return
	}

	value, exist := kv.data[args.Key]
	if !exist {
		value = ""
	}
	reply.Value = value
	kv.data[args.Key] += args.Value
	reply.Ok = true
	kv.reply[args.RequestId] = value
}

func (kv *KVServer) Ack(args *AckArgs, reply *AckReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.reply, args.RequestId)
	reply.Ok = true
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.reply = make(map[string]string)

	return kv
}
