// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/server.cc:
 *   Implementation of a single transactional key-value server.
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

#include "store/tapirstore/server.h"
#include "mlx5_datapath_cpp.h"
#include "tapir_serialized_cpp.h"
namespace tapirstore {

using namespace std;
using namespace proto;

Server::Server(bool linearizable, void* arena, void* connection, void* mempool_ids_ptr,  bool useCornflakes)
    : useCornflakes(useCornflakes), arena(arena), connection(connection), mempool_ids_ptr(mempool_ids_ptr)
{
    store = new Store(linearizable);
    //store->Put();

}

Server::~Server()
{
    delete store;
}

void
Server::ExecInconsistentUpcall(const string &str1)
{
    Debug("Received Inconsistent Request: %s",  str1.c_str());

    TapirRequest request;

    request.ParseFromString(str1);
    Operation op = static_cast<Operation>(request.op());
    switch (op) {
    case COMMIT:
        store->Commit(request.txnid(), request.commit().timestamp());
        break;
    case ABORT:
        store->Abort(request.txnid(), Transaction(request.abort().txn()));
        break;
    default:
        Panic("Unrecognized inconsistent operation.");
    }
}

void
Server::ExecConsensusUpcall(const string &str1, void* reply)
{
    Debug("Received Consensus Request: %s", str1.c_str());

    TapirRequest request;
    
    int status;
    Timestamp proposed;

    request.ParseFromString(str1);
    Operation op = static_cast<Operation>(request.op());
    switch (op) {
    case PREPARE:
        status = store->Prepare(request.txnid(),
                                Transaction(request.prepare().txn()),
                                Timestamp(request.prepare().timestamp()),
                                proposed);
        if (useCornflakes) {
            void* result;
            Reply_get_mut_result(reply, &result);
            
            TapirReply_set_status(result, status);
            void* timestamp;
            TapirReply_get_mut_timestamp(result, &timestamp);

            if (proposed.isValid()) {
                proposed.serialize(timestamp, true);
            }
        } else {
            replication::Reply* replicationReply = (replication::Reply*) reply;
            TapirReply tapirReply;
            tapirReply.set_status(status);
            if (proposed.isValid()) {
                proposed.serialize(tapirReply.mutable_timestamp());
            }
            *(replicationReply->mutable_result()) = tapirReply;
        }
        break;
    default:
        Panic("Unrecognized consensus operation.");
    }

}

void
Server::UnloggedUpcall(const string &str1, void* reply)
{
    Debug("Received Consensus Request: %s", str1.c_str());
    
    TapirRequest request;
    
    int status;

    request.ParseFromString(str1);
    Operation op = static_cast<Operation>(request.op());
    switch (op) {
    case GET:
        if (useCornflakes) {
            void* tapirReply;
            Reply_get_mut_result(reply, &tapirReply);

            if (request.get().has_timestamp()) {
                pair<Timestamp, VersionedKVStore::KVStoreValue> val;
                status = store->Get(request.txnid(), request.get().key(),
                                request.get().timestamp(), val);
                if (status == 0) {
                    void* cfString;
                    CFString_new(val.second.zeroCopyString.ptr, val.second.zeroCopyString.len, connection, arena, &cfString);
                    TapirReply_set_value(tapirReply, cfString);
                }
            } else {
                pair<Timestamp, VersionedKVStore::KVStoreValue> val;
                status = store->Get(request.txnid(), request.get().key(), val);
                if (status == 0) {
                    void* cfString;
                    CFString_new(val.second.zeroCopyString.ptr, val.second.zeroCopyString.len, connection, arena, &cfString);
                    TapirReply_set_value(tapirReply, cfString);
                    void* timestamp;
                    TapirReply_get_mut_timestamp(tapirReply, &timestamp);
                    val.first.serialize(timestamp);
                }
            }
            TapirReply_set_status(tapirReply, status);
            break;
        } else {
            replication::Reply* replicationReply = (replication::Reply*) reply;
            TapirReply tapirReply;
            if (request.get().has_timestamp()) {
                pair<Timestamp, VersionedKVStore::KVStoreValue> val;
                status = store->Get(request.txnid(), request.get().key(),
                                request.get().timestamp(), val);
                if (status == 0) {
                    tapirReply.set_value(val.second.copyString);
                }
            } else {
                pair<Timestamp, VersionedKVStore::KVStoreValue> val;
                status = store->Get(request.txnid(), request.get().key(), val);
                if (status == 0) {
                    tapirReply.set_value(val.second.copyString);
                    val.first.serialize(tapirReply.mutable_timestamp());
                }
            }
            tapirReply.set_status(status);
            *(replicationReply->mutable_result()) = tapirReply;
            break;
        }
    default:
        Panic("Unrecognized Unlogged request.");
    }
}

void
Server::Sync(const std::map<opid_t, RecordEntry>& record)
{
    Panic("Unimplemented!");
}

std::map<opid_t, std::string>
Server::Merge(const std::map<opid_t, std::vector<RecordEntry>> &d,
              const std::map<opid_t, std::vector<RecordEntry>> &u,
              const std::map<opid_t, std::string> &majority_results_in_d)
{
    Panic("Unimplemented!");
}

void
Server::Load(const string &key, const VersionedKVStore::KVStoreValue  &value, const Timestamp timestamp)
{
    store->Load(key, value, timestamp);
}

} // namespace tapirstore


int
main(int argc, char **argv)
{
    int index = -1;
    unsigned int myShard = 0, maxShard = 1, nKeys = 1;
    const char *configPath = NULL;
    const char *keyPath = NULL;
    bool linearizable = true;

    // Parse arguments
    int opt;
    while ((opt = getopt(argc, argv, "c:i:m:e:s:f:n:N:k:")) != -1) {
        switch (opt) {
        case 'c':
            configPath = optarg;
            break;

        case 'i':
        {
            char *strtolPtr;
            index = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (index < 0))
            {
                fprintf(stderr, "option -i requires a numeric arg\n");
            }
            break;
        }

        case 'm':
        {
            if (strcasecmp(optarg, "txn-l") == 0) {
                linearizable = true;
            } else if (strcasecmp(optarg, "txn-s") == 0) {
                linearizable = false;
            } else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
            }
            break;
        }

        case 'k':
        {
            char *strtolPtr;
            nKeys = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr, "option -e requires a numeric arg\n");
            }
            break;
        }

        case 'n':
        {
            char *strtolPtr;
            myShard = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr, "option -e requires a numeric arg\n");
            }
            break;
        }

        case 'N':
        {
            char *strtolPtr;
            maxShard = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr, "option -e requires a numeric arg\n");
            }
            break;
        }

        case 'f':   // Load keys from file
        {
            keyPath = optarg;
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind-2]);
        }
    }

    if (!configPath) {
        fprintf(stderr, "option -c is required\n");
    }

    if (index == -1) {
        fprintf(stderr, "option -i is required\n");
    }

    // Load configuration
    std::ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n", configPath);
    }
    transport::Configuration config(configStream);

    if (index >= config.n) {
        fprintf(stderr, "replica index %d is out of bounds; "
                "only %d replicas defined\n", index, config.n);
    }

    const char *cf_config = getenv("CONFIG_PATH");
    const char *server_ip = getenv("SERVER_IP");
    // construct new connection and store the pointer
    void* connection = Mlx5Connection_new(cf_config, server_ip);
    void* arena = Bump_with_capacity(
            32,   // batch_size
            1024, // max_packet_size
            64   // max_entries
        );
    bool useCornflakes = true;

    size_t key_size = 64;
    size_t value_size = 512;
    size_t num_keys = nKeys;

    void* rust_backing_db;
    void* mempool_ids_ptr;
    int ret = Mlx5Connection_load_retwis_db(
                connection,
                key_size,
                value_size,
                num_keys,
                &rust_backing_db,
                &mempool_ids_ptr);
    if (ret != 0) {
        printf("Error: Could not run retwis Mlx5Connection_load_retwis_db\n");
        exit(1);
    }


    CFTransport transport(connection, arena);

    tapirstore::Server server(linearizable, arena, connection, mempool_ids_ptr, useCornflakes);

    replication::ir::IRReplica replica(config, index, &transport, &server, connection, arena, useCornflakes);
 

    void *db_keys_vec;
    size_t db_keys_len = 0;
    Mlx5Connection_get_db_keys_vec(rust_backing_db, &db_keys_vec, &db_keys_len);
    if (db_keys_vec == NULL) {
        printf("Error: Could not get iterator over rust backing db.\n");
        exit(1);
    } else {
        printf("Got rust backing db with len %lu\n", db_keys_len);
    }
    void *key_ptr = NULL;
    size_t key_len = 0;
    void *value_ptr = NULL;
    size_t value_len = 0;
    void *value_box_ptr = NULL;

    for (size_t key_idx = 0; key_idx < db_keys_len; key_idx++) {
        Mlx5Connection_get_db_value_at(rust_backing_db, db_keys_vec, key_idx, &key_ptr, &key_len, &value_ptr, &value_len, &value_box_ptr);
        if (key_ptr != NULL && value_ptr != NULL) {
            if (useCornflakes) {
                /* for keys - just use plain redis sds strings */
                // robj *k = createStringObject((char *)key_ptr, key_len);
                // robj *v = createZeroCopyStringObject((unsigned char *)value_ptr, value_len, value_box_ptr);
                string key = string((char*) key_ptr, key_len);
                VersionedKVStore::KVStoreValue value = VersionedKVStore::KVStoreValue((unsigned char *)value_ptr, value_len, value_box_ptr);
                server.Load(key, value, Timestamp());
            } else {
                /* for keys and values - just use plain redis sds strings */
                // void *k = createStringObject((char *)key_ptr, key_len);
                
                // normal string key
                // normal string value

                // NOTE: smartptr is ptr to box of mlx5buffer

                string key = string((char*) key_ptr, key_len);
                VersionedKVStore::KVStoreValue value = VersionedKVStore::KVStoreValue((char*) value_ptr, value_len);
                server.Load(key, value, Timestamp());
                Mlx5Connection_free_datapath_buffer(value_box_ptr);
                
            }
        } else {
            printf("ERROR: Could not load value for key %lu from kv store", key_idx);
        }
    }

    Mlx5Connection_drop_db(rust_backing_db);

    if (keyPath) {
        string key;
        std::ifstream in;
        in.open(keyPath);
        if (!in) {
            fprintf(stderr, "Could not read keys from: %s\n", keyPath);
            exit(0);
        }

        for (unsigned int i = 0; i < nKeys; i++) {
            getline(in, key);

            uint64_t hash = 5381;
            const char* str = key.c_str();
            for (unsigned int j = 0; j < key.length(); j++) {
                hash = ((hash << 5) + hash) + (uint64_t)str[j];
            }

            if (hash % maxShard == myShard) {
                server.Load(key, VersionedKVStore::KVStoreValue("null"), Timestamp());
            }
        }
        in.close();
    }

    fprintf(stdout, "Completed setup. Running server ...\n");
    transport.Run();

    return 0;
}

