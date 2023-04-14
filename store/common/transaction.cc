// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/transaction.cc
 *   A transaction implementation.
 *
 **********************************************************************/

#include "store/common/transaction.h"
#include "mlx5_datapath_cpp.h"

using namespace std;

Transaction::Transaction() :
    readSet(), writeSet() { }

Transaction::Transaction(const TransactionMessage &msg, void* connection, void* mempool_ids_ptr, bool useCornflakes)
{
    for (int i = 0; i < msg.readset_size(); i++) {
        ReadMessage readMsg = msg.readset(i);
        readSet[readMsg.key()] = Timestamp(readMsg.readtime());
    }

    for (int i = 0; i < msg.writeset_size(); i++) {
        WriteMessage writeMsg = msg.writeset(i);
        if (useCornflakes) {
            void* new_val_buffer;
            // allocate into mlx5 buffer and get zero copy reference
            void* smart_ptr_buffer = Mlx5Connection_allocate_and_copy_into_original_datapath_buffer(
                                        connection,
                                        mempool_ids_ptr,
                                        (unsigned char*) writeMsg.value().c_str(),
                                        writeMsg.value().length(),
                                        &new_val_buffer
                                     );

            // create new value
            writeSet[writeMsg.key()] = VersionedKVStore::KVStoreValue((unsigned char *)new_val_buffer, writeMsg.value().length(), smart_ptr_buffer);
        } else {
            writeSet[writeMsg.key()] = VersionedKVStore::KVStoreValue(writeMsg.value());
        }
    }
}

Transaction::~Transaction() { }

const unordered_map<string, Timestamp>&
Transaction::getReadSet() const
{
    return readSet;
}

const unordered_map<string, VersionedKVStore::KVStoreValue>&
Transaction::getWriteSet() const
{
    return writeSet;
}

void
Transaction::addReadSet(const string &key,
                        const Timestamp &readTime)
{
    readSet[key] = readTime;
}

void
Transaction::addWriteSet(const string &key,
                         const VersionedKVStore::KVStoreValue &value)
{
    writeSet[key] = value;
}

void
Transaction::serialize(TransactionMessage *msg) const
{
    for (auto read : readSet) {
        ReadMessage *readMsg = msg->add_readset();
        readMsg->set_key(read.first);
        read.second.serialize(readMsg->mutable_readtime());
    }

    for (auto write : writeSet) {
        WriteMessage *writeMsg = msg->add_writeset();
        writeMsg->set_key(write.first);
        writeMsg->set_value(write.second.copyString);
    }
}
