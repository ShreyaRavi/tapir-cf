// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * cftransport.cc:
 *   message-passing network interface that uses Cornflakes message 
 *   delivery
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/cftransport.h"

#include <google/protobuf/message.h>
#include <event2/event.h>
#include <event2/thread.h>

#include <memory>
#include <random>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <signal.h>
#include <stdlib.h>

#include "mlx5_datapath_cpp.h"
#include "tapir_serialized_cpp.h"

using std::pair;

CFTransportAddress::CFTransportAddress(const uintptr_t conn_id, const uint32_t msg_id)
    : conn_id(conn_id), msg_id(msg_id)
{

}

CFTransportAddress *
CFTransportAddress::clone() const
{
    CFTransportAddress *c = new CFTransportAddress(*this);
    return c;    
}

bool operator==(const CFTransportAddress &a, const CFTransportAddress &b)
{
    return a.conn_id == b.conn_id;
}

bool operator!=(const CFTransportAddress &a, const CFTransportAddress &b)
{
    return !(a == b);
}

bool operator<(const CFTransportAddress &a, const CFTransportAddress &b)
{
    return (memcmp(&a.conn_id, &b.conn_id, sizeof(a.conn_id)) < 0);
}

CFTransportAddress
CFTransport::LookupAddress(const transport::ReplicaAddress &addr)
{
    Panic("LookupAddress was called in CFTransport.");
}

CFTransportAddress
CFTransport::LookupAddress(const transport::Configuration &config,
                            int idx)
{
    // can keep this the same.
    // addr just has a string host and port populated from config file.
    const transport::ReplicaAddress &addr = config.replica(idx);
    return LookupAddress(addr);
}

CFTransport::CFTransport(void* mlx5Connection, void* bumpArena)
    : connection(mlx5Connection), arena(bumpArena), stopLoop(false)
{
    
}

CFTransport::~CFTransport()
{
    
}

void
CFTransport::Register(TransportReceiver *receiver,
                       const transport::Configuration &config,
                       int replicaIdx)
{
    ASSERT(replicaIdx < config.n);
    // struct sockaddr_in sin;
    
    RegisterConfiguration(receiver, config, replicaIdx);

    // don't need to do SetAddress because it only sets an address
    // that is used when GetAddress is called.

    // GetAddress is only called for the client (not server).


    //CFTransportAddress *addr = new CFTransportAddress(sin);
    //receiver->SetAddress(addr);

    // Set up receiver to processes calls
    this->receiver = receiver;    

}

static size_t
SerializeMessage(const ::google::protobuf::Message &m,
                 std::unique_ptr<char[]> *out)
{
    string data = m.SerializeAsString();
    string type = m.GetTypeName();
    int msg_type = -1;
    if (type == "replication.ir.proto.FinalizeInconsistentMessage") {
        msg_type = FINALIZE_INCONSISTENT_MESSAGE;
    } else if (type == "replication.ir.proto.ProposeInconsistentMessage") {
        msg_type = PROPOSE_INCONSISTENT_MESSAGE;
    } else if (type == "replication.ir.proto.FinalizeConsensusMessage") {
        msg_type = FINALIZE_CONSENSUS_MESSAGE;
    } else if (type == "replication.ir.proto.ProposeConsensusMessage") {
        msg_type = PROPOSE_CONSENSUS_MESSAGE;
    } else if (type == "replication.ir.proto.UnloggedRequestMessage") {
        msg_type = UNLOGGED_REQUEST_MESSAGE;
    } else if (type == "replication.ir.proto.ReplyInconsistentMessage") {
        msg_type = REPLY_INCONSISTENT_MESSAGE;
    } else if (type == "replication.ir.proto.ReplyConsensusMessage") {
        msg_type = REPLY_CONSENSUS_MESSAGE;
    } else if (type == "replication.ir.proto.ConfirmMessage") {
        msg_type = CONFIRM_MESSAGE;
    } else if (type == "replication.ir.proto.UnloggedReplyMessage") {
        msg_type = UNLOGGED_REPLY_MESSAGE;
    }

    if (msg_type == -1) {
        printf("type: %s\n", type.c_str());
	Panic("Invalid message type");
    }

    // ignore type len and type. just write the enum and the data.
    size_t dataLen = data.length();
    ssize_t totalLen = (sizeof(int) +
                       dataLen + sizeof(dataLen));

    std::unique_ptr<char[]> unique_buf(new char[totalLen]);
    char *buf = unique_buf.get();

    char *ptr = buf;
    *((int *) ptr) = msg_type;
    ptr += sizeof(int);
    *((size_t *) ptr) = dataLen;
    ptr += sizeof(size_t);
    ASSERT(ptr-buf < totalLen);
    ASSERT(ptr+dataLen-buf == totalLen);
    memcpy(ptr, data.c_str(), dataLen);
    ptr += dataLen;

    *out = std::move(unique_buf);
    return totalLen;
}

bool
CFTransport::SendMessageInternal(TransportReceiver *src,
                                  const CFTransportAddress &dst,
                                  const Message &m,
                                  bool multicast)
{
    (void)multicast;

    if (connection == NULL) {
        printf("Could not find connection.");
        return false;
    }
    uintptr_t conn_id = dynamic_cast<const CFTransportAddress &>(dst).conn_id;
    uint32_t msg_id = dynamic_cast<const CFTransportAddress &>(dst).msg_id;

    // Serialize message
    std::unique_ptr<char[]> unique_buf;
    size_t msgLen = SerializeMessage(m, &unique_buf);
    char *buf = unique_buf.get(); 

    uint32_t status = Mlx5Connection_queue_single_buffer_with_copy(connection, msg_id, conn_id, (uint8_t*)buf, msgLen, true);
    if (status != 0) {
        Panic("Error queuing single buffer.");
    }

    return true;
}

bool
CFTransport::SendCFMessageInternal(TransportReceiver *src,
                                    const CFTransportAddress &dst,
                                    void* m,
                                    const MessageType type,
                                    bool multicast)
{
    (void)multicast;
    uintptr_t conn_id = dynamic_cast<const CFTransportAddress &>(dst).conn_id;
    uint32_t msg_id = dynamic_cast<const CFTransportAddress &>(dst).msg_id;

    switch(type) {
        case REPLY_INCONSISTENT_MESSAGE:
            printf("sending reply inconsistent message\n");
            Mlx5Connection_ReplyInconsistentMessage_queue_cornflakes_arena_object(connection, msg_id, conn_id, m, true);
            break;
        case REPLY_CONSENSUS_MESSAGE:
            break;
        case CONFIRM_MESSAGE:
            break;
        case UNLOGGED_REPLY_MESSAGE:
            break;
        default:
            Panic("Message type in SendCFMessageInternal is unexpected.");
            return false;
    }
    return true;
}

static void
DecodePacket(const char *buf, size_t sz, string &type, string &msg)
{   
    // first sizeof(int) bytes represent the type which we can look up in the enum

    const char *ptr = buf;
    int msg_type = *((int *)ptr);
    ptr += sizeof(int);
    if (msg_type == FINALIZE_INCONSISTENT_MESSAGE) {
        type = "replication.ir.proto.FinalizeInconsistentMessage";
    } else if (msg_type == PROPOSE_INCONSISTENT_MESSAGE) {
        type = "replication.ir.proto.ProposeInconsistentMessage";
    } else if (msg_type == FINALIZE_CONSENSUS_MESSAGE) {
        type = "replication.ir.proto.FinalizeConsensusMessage";
    } else if (msg_type == PROPOSE_CONSENSUS_MESSAGE) {
        type = "replication.ir.proto.ProposeConsensusMessage";
    } else if (msg_type == UNLOGGED_REQUEST_MESSAGE) {
        type = "replication.ir.proto.UnloggedRequestMessage";
    } else if (msg_type == REPLY_INCONSISTENT_MESSAGE) {
        type = "replication.ir.proto.ReplyInconsistentMessage";
    } else if (msg_type == REPLY_CONSENSUS_MESSAGE) {
        type = "replication.ir.proto.ReplyConsensusMessage";
    } else if (msg_type == CONFIRM_MESSAGE){
        type = "replication.ir.proto.ConfirmMessage";
    } else if (msg_type == UNLOGGED_REPLY_MESSAGE) {
        type = "replication.ir.proto.UnloggedReplyMessage";
    } else {
	printf("message type: %d\n", msg_type);
        Panic("Decoding unknown message type.");
    }

    size_t msgLen = *((size_t *)ptr);
    ptr += sizeof(size_t);

    ASSERT(ptr-buf < (int)sz);
    ASSERT(ptr+msgLen-buf <= (int)sz);

    msg = string(ptr, msgLen);
    ptr += msgLen;
}

void
CFTransport::Run()
{
    while (!stopLoop) {
        
        size_t n = 0;
        void** pkts = Mlx5Connection_pop_raw_packets(connection, &n);
        // if n = 0, continue
        for (size_t i = 0; i < n; i++) {
            string msgType, msg;
	    uint32_t msg_id = Mlx5Connection_RxPacket_msg_id(pkts[i]);
	    uintptr_t conn_id = Mlx5Connection_RxPacket_conn_id(pkts[i]);
	    uintptr_t data_len = Mlx5Connection_RxPacket_data_len(pkts[i]);
	    const unsigned char* data = Mlx5Connection_RxPacket_data(pkts[i]);
	    DecodePacket((const char*)data, data_len, msgType, msg);
            // we should pass in the conn id ptr so that the sendMessage() function which sends replies
            // can know which conn id to send the reply on

            // we don't have to worry about sending messages that are not replies
            // (like do view change messages) because we ignore those entirely.

            // if we do have to worry about that, things become more complicated
            // because we don't know which connection id to send messages on
            
            // we would be acting like a "client" in that case and register connections
            // with each other server and then send messages like a client.

            // we would have to call connect(Address) -> conn id to get the 
            // conn id to send stuff and create the mapping in LookupAddresses

            // 1. DONE. run a test to make sure LookupAddresses(), LookupAddress(), 
            //    SendMessageToAll(), and SendMessageToReplica() are never called.   

            // 2. DONE. create an address with the conn id pointer and pass it in.
            CFTransportAddress senderAddr(conn_id, msg_id);
            receiver->ReceiveMessage(senderAddr, msgType, msg);
	    Mlx5Connection_RxPacket_free(pkts[i]);
        }
        Bump_reset(arena);
    }
}

void
CFTransport::Stop()
{
    stopLoop = true;
}

int
CFTransport::Timer(uint64_t ms, timer_callback_t cb)
{
    Panic("Timer created in CFTransport. Unimplemented.");
}

bool
CFTransport::CancelTimer(int id)
{
    Panic("Timer cancelled in CFTransport. Unimplemented.");
}

void
CFTransport::CancelAllTimers()
{
    Panic("All timers canceled in CFTransport. Unimplemented.");
}


