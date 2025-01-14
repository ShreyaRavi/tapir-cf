// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/shardclient.cc:
 *   Single shard tapir transactional client.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#include "store/tapirstore/shardclient.h"
#include "tapir_serialized_cpp.h"

namespace tapirstore {

using namespace std;
using namespace proto;

ShardClient::ShardClient(const string &configPath,
                       Transport *transport, uint64_t client_id, int
                       shard, int closestReplica, bool useCornflakes)
    : client_id(client_id), transport(transport), shard(shard), useCornflakes(useCornflakes)
{
    ifstream configStream(configPath);
    if (configStream.fail()) {
        Panic("Unable to read configuration file: %s\n", configPath.c_str());
    }

    transport::Configuration config(configStream);
    this->config = &config;

    client = new replication::ir::IRClient(config, transport, client_id, useCornflakes);

    if (closestReplica == -1) {
        replica = client_id % config.n;
    } else {
        replica = closestReplica;
    }
    Debug("Sending unlogged to replica %i", replica);

    waiting = NULL;
    blockingBegin = NULL;
}

ShardClient::~ShardClient()
{
    delete client;
}

void
ShardClient::Begin(uint64_t id)
{
    Debug("[shard %i] BEGIN: %lu", shard, id);

    // Wait for any previous pending requests.
    if (blockingBegin != NULL) {
        blockingBegin->GetReply();
        delete blockingBegin;
        blockingBegin = NULL;
    }
}

void
ShardClient::Get(uint64_t id, const string &key, Promise *promise)
{
    printf("in GET that we don't want to be calling (1)\n");
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending GET [%lu : %s]", shard, id, key.c_str());

    /*
    // create request
    string request_str;
    TapirRequest request;
    request.set_op(GET);
    request.set_txnid(id);
    request.mutable_get()->set_key(key);
    request.SerializeToString(&request_str);

    // set to 1 second by default
    int timeout = (promise != NULL) ? promise->GetTimeout() : 1000;

    transport->Timer(0, [=]() {
	    waiting = promise;
        client->InvokeUnlogged(replica,
                               request_str,
                               bind(&ShardClient::GetCallback,
                                    this,
                                    placeholders::_1,
                                    placeholders::_2),
                               bind(&ShardClient::GetTimeout,
                                    this),
                               timeout); // timeout in ms
    });
    */
}

void
ShardClient::Get(uint64_t id, const string &key, uint64_t command_id)
{
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending GET [%lu : %s]", shard, id, key.c_str());

    // create request
    string request_str;
    TapirRequest request;
    request.set_op(GET);
    request.set_txnid(id);
    request.mutable_get()->set_key(key);
    request.SerializeToString(&request_str);

    // set to 1 second by default
    int timeout = 1000;

    transport->Timer(0, [=]() {
        client->InvokeUnlogged(replica,
                               request_str,
                               bind(&ShardClient::GetCallback,
                                    this,
                                    command_id,
                                    placeholders::_1,
                                    placeholders::_2),
                               bind(&ShardClient::GetTimeout,
                                    this),
                               timeout); // timeout in ms
    });
}

void
ShardClient::Get(uint64_t id, const string &key,
                const Timestamp &timestamp, Promise *promise)
{
    printf("in GET that we don't want to be calling (2)\n");
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending GET [%lu : %s]", shard, id, key.c_str());

    /*
    // create request
    string request_str;
    TapirRequest request;
    request.set_op(GET);
    request.set_txnid(id);
    request.mutable_get()->set_key(key);
    timestamp.serialize(request.mutable_get()->mutable_timestamp());
    request.SerializeToString(&request_str);

    // set to 1 second by default
    int timeout = (promise != NULL) ? promise->GetTimeout() : 1000;

    transport->Timer(0, [=]() {
	    waiting = promise;
        client->InvokeUnlogged(
            replica,
            request_str,
            bind(&ShardClient::GetCallback, this,
                placeholders::_1,
                placeholders::_2),
            bind(&ShardClient::GetTimeout, this),
            timeout); // timeout in ms
    });
    */
}

void
ShardClient::Put(uint64_t id,
               const string &key,
               const string &value,
               Promise *promise)
{
    Panic("Unimplemented PUT");
    return;
}

void
ShardClient::Prepare(uint64_t id, const Transaction &txn,
                    const Timestamp &timestamp, Promise *promise)
{
    Debug("[shard %i] Sending PREPARE [%lu]", shard, id);

    // create prepare request
    string request_str;
    TapirRequest request;
    request.set_op(PREPARE);
    request.set_txnid(id);
    txn.serialize(request.mutable_prepare()->mutable_txn());
    timestamp.serialize(request.mutable_prepare()->mutable_timestamp());
    request.SerializeToString(&request_str);

    transport->Timer(0, [=]() {
        waiting = promise;
        client->InvokeConsensus(
            request_str,
            bind(&ShardClient::TapirDecide, this,
                placeholders::_1),
            bind(&ShardClient::PrepareCallback, this,
                placeholders::_1,
                placeholders::_2));
    });
}

replication::Reply
ShardClient::TapirDecide(const std::map<std::string, std::size_t> &results)
{

    // If a majority say prepare_ok,
    int ok_count = 0;
    Timestamp ts = 0;
    string final_reply_str;
    TapirReply final_reply;
    replication::Reply replicationReply;
    for (const auto& string_and_count : results) {
        const std::string &s = string_and_count.first;
        const std::size_t count = string_and_count.second;

        TapirReply reply;
        reply.ParseFromString(s);

	if (reply.status() == REPLY_OK) {
	    ok_count += count;
	} else if (reply.status() == REPLY_FAIL) {
	    *replicationReply.mutable_result() = reply;
            return replicationReply;
	} else if (reply.status() == REPLY_RETRY) {
	    Timestamp t(reply.timestamp());
	    if (t > ts) {
		ts = t;
	    }
	}
    }

    if (ok_count >= config->QuorumSize()) {
	final_reply.set_status(REPLY_OK);
    } else {
       final_reply.set_status(REPLY_RETRY);
       ts.serialize(final_reply.mutable_timestamp());
    }

    *replicationReply.mutable_result() = final_reply;
    return replicationReply;
}

void
ShardClient::Commit(uint64_t id, const Transaction &txn,
                   uint64_t timestamp, Promise *promise)
{

    Debug("[shard %i] Sending COMMIT [%lu]", shard, id);

    // create commit request
    string request_str;
    TapirRequest request;
    request.set_op(COMMIT);
    request.set_txnid(id);
    request.mutable_commit()->set_timestamp(timestamp);
    request.SerializeToString(&request_str);

    blockingBegin = new Promise(COMMIT_TIMEOUT);
    transport->Timer(0, [=]() {
        waiting = promise;
        client->InvokeInconsistent(
            request_str,
            bind(&ShardClient::CommitCallback, this,
                placeholders::_1,
                placeholders::_2));
    });
}

void
ShardClient::Abort(uint64_t id, const Transaction &txn, Promise *promise)
{
    Debug("[shard %i] Sending ABORT [%lu]", shard, id);

    // create abort request
    string request_str;
    TapirRequest request;
    request.set_op(ABORT);
    request.set_txnid(id);
    txn.serialize(request.mutable_abort()->mutable_txn());
    request.SerializeToString(&request_str);

    blockingBegin = new Promise(ABORT_TIMEOUT);
    transport->Timer(0, [=]() {
	    waiting = promise;
	    client->InvokeInconsistent(
            request_str,
            bind(&ShardClient::AbortCallback, this,
                placeholders::_1,
                placeholders::_2));
    });
}

void
ShardClient::GetTimeout()
{
    if (waiting != NULL) {
        Promise *w = waiting;
        waiting = NULL;
        w->Reply(REPLY_TIMEOUT);
    }
}

int
ShardClient::GetStatus(const uint64_t command_id, string& value)
{
    if (finishedGets.find(command_id) != finishedGets.end()) {
        value = finishedGets[command_id];
        finishedGets.erase(command_id);
        return 1;
    }
    return -1;
}

/* Callback from a shard replica on get operation completion. */
void
ShardClient::GetCallback(const uint64_t command_id, const string &request_str, const void* replication_reply)
{
    /* Replies back from a shard. */

    // this is where we should unpack the value from the cf struct

    if (useCornflakes) {
        void* tapirReply;
        Reply_get_result(replication_reply, &tapirReply);

        int32_t status;
        TapirReply_get_status(tapirReply, &status);

        void* cfValue;
        TapirReply_get_value(tapirReply, &cfValue);
        const unsigned char* replyValue;
        uintptr_t replyLen;
        CFString_unpack(cfValue, &replyValue, &replyLen);
        // printf("unlogged reply message value len: %lu\n",replyLen );
        string replyStr((char*)replyValue, replyLen);
        
        void* timestamp;
        TapirReply_get_mut_timestamp(tapirReply, &timestamp);
        
        uint64_t timestampId;
        TimestampMessage_get_id(timestamp, &timestampId);
        uint64_t timestampVal;
        TimestampMessage_get_timestamp(timestamp, &timestampVal);

        // set status for the req id
        finishedGets[command_id] = replyStr;
        printf("Received get response for command id: %lu\n", command_id);

        if (waiting != NULL) {
            Promise *w = waiting;
            waiting = NULL;
            w->Reply(status, Timestamp(timestampVal), replyStr);
        }

        // printf("value in unlogged reply message tapir reply: %s\n", replyStr.c_str());

    } else {
        const TapirReply& reply = ((replication::Reply*)replication_reply)->result();
        Debug("[shard %lu:%i] GET callback [%d]", client_id, shard, reply.status());
        if (waiting != NULL) {
            Promise *w = waiting;
            waiting = NULL;
            if (reply.has_timestamp()) {
                w->Reply(reply.status(), Timestamp(reply.timestamp()), reply.value());
            } else {
                w->Reply(reply.status(), reply.value());
            }
        }
    }
    
}

/* Callback from a shard replica on get operation completion. */
/*
void
ShardClient::GetCallback(const string &request_str, const void* replication_reply)
{
    // Replies back from a shard.

    // this is where we should unpack the value from the cf struct

    if (useCornflakes) {
        void* tapirReply;
        Reply_get_result(replication_reply, &tapirReply);

        int32_t status;
        TapirReply_get_status(tapirReply, &status);

        void* cfValue;
        TapirReply_get_value(tapirReply, &cfValue);
        const unsigned char* replyValue;
        uintptr_t replyLen;
        CFString_unpack(cfValue, &replyValue, &replyLen);
        // printf("unlogged reply message value len: %lu\n",replyLen );
        string replyStr((char*)replyValue, replyLen);
        
        void* timestamp;
        TapirReply_get_mut_timestamp(tapirReply, &timestamp);
        
        uint64_t timestampId;
        TimestampMessage_get_id(timestamp, &timestampId);
        uint64_t timestampVal;
        TimestampMessage_get_timestamp(timestamp, &timestampVal);

        if (waiting != NULL) {
            Promise *w = waiting;
            waiting = NULL;
            w->Reply(status, Timestamp(timestampVal), replyStr);
        }

        // printf("value in unlogged reply message tapir reply: %s\n", replyStr.c_str());

    } else {
        const TapirReply& reply = ((replication::Reply*)replication_reply)->result();
        Debug("[shard %lu:%i] GET callback [%d]", client_id, shard, reply.status());
        if (waiting != NULL) {
            Promise *w = waiting;
            waiting = NULL;
            if (reply.has_timestamp()) {
                w->Reply(reply.status(), Timestamp(reply.timestamp()), reply.value());
            } else {
                w->Reply(reply.status(), reply.value());
            }
        }
    }
    
}
*/

/* Callback from a shard replica on prepare operation completion. */
void
ShardClient::PrepareCallback(const string &request_str, const void* replication_reply)
{
    // this will ALWAYS be a protobuf
    const TapirReply& reply = ((replication::Reply*)replication_reply)->result();
    Debug("[shard %lu:%i] PREPARE callback [%d]", client_id, shard, reply.status());
    if (waiting != NULL) {
        Promise *w = waiting;
        waiting = NULL;
        if (reply.has_timestamp()) {
            w->Reply(reply.status(), Timestamp(reply.timestamp()));
        } else {
            w->Reply(reply.status(), Timestamp());
        }
    }
}

/* Callback from a shard replica on commit operation completion. */
void
ShardClient::CommitCallback(const string &request_str, const void* reply_str)
{
    // COMMITs always succeed.

    ASSERT(blockingBegin != NULL);
    blockingBegin->Reply(0);

    if (waiting != NULL) {
        waiting = NULL;
    }
    Debug("[shard %lu:%i] COMMIT callback", client_id, shard);
}

/* Callback from a shard replica on abort operation completion. */
void
ShardClient::AbortCallback(const string &request_str, const void* reply_str)
{
    // ABORTs always succeed.

    ASSERT(blockingBegin != NULL);
    blockingBegin->Reply(0);

    if (waiting != NULL) {
        waiting = NULL;
    }
    Debug("[shard %lu:%i] ABORT callback", client_id, shard);
}

} // namespace tapir

