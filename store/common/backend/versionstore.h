// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/versionstore.cc:
 *   Timestamped version store
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
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

#ifndef _VERSIONED_KV_STORE_H_
#define _VERSIONED_KV_STORE_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"

#include <set>
#include <map>
#include <unordered_map>

class VersionedKVStore
{
public:
    struct ZeroCopyString {
        ZeroCopyString() : len(0), ptr(NULL), smart_ptr(NULL) {};
        ZeroCopyString(const unsigned char *ptr, size_t len, void *smart_ptr) {
            this->ptr = ptr;
            this->len = len;
            this->smart_ptr = smart_ptr;
        }
        size_t len;
        const unsigned char* ptr;
        void* smart_ptr;
    };

    struct KVStoreValue {
        KVStoreValue(const unsigned char *ptr, size_t len, void *smart_ptr) {
            this->zeroCopyString = ZeroCopyString(ptr, len, smart_ptr);
        }
        KVStoreValue(const char* ptr, size_t len) {
            this->copyString = std::string(ptr, len);
        }
        KVStoreValue(std::string str) {
            this->copyString = str;
        }
        KVStoreValue() {}
        ZeroCopyString zeroCopyString;
        std::string copyString;
    };
    VersionedKVStore();
    ~VersionedKVStore();

    bool get(const std::string &key, std::pair<Timestamp, KVStoreValue> &value);
    bool get(const std::string &key, const Timestamp &t, std::pair<Timestamp, KVStoreValue> &value);
    bool getRange(const std::string &key, const Timestamp &t, std::pair<Timestamp, Timestamp> &range);
    bool getLastRead(const std::string &key, Timestamp &readTime);
    bool getLastRead(const std::string &key, const Timestamp &t, Timestamp &readTime);
    void put(const std::string &key, const KVStoreValue &value, const Timestamp &t);
    void commitGet(const std::string &key, const Timestamp &readTime, const Timestamp &commit);

private:

    struct VersionedValue {
        Timestamp write;
        KVStoreValue value;

        VersionedValue(Timestamp commit) : write(commit), value(KVStoreValue()) { };
        VersionedValue(Timestamp commit, KVStoreValue val) : write(commit), value(val) { };

        friend bool operator> (const VersionedValue &v1, const VersionedValue &v2) {
            return v1.write > v2.write;
        };
        friend bool operator< (const VersionedValue &v1, const VersionedValue &v2) {
            return v1.write < v2.write;
        };
    };

    /* Global store which keeps key -> (timestamp, value) list. */
    std::unordered_map< std::string, std::set<VersionedValue> > store;
    std::unordered_map< std::string, std::map< Timestamp, Timestamp > > lastReads;
    bool inStore(const std::string &key);
    void getValue(const std::string &key, const Timestamp &t, std::set<VersionedValue>::iterator &it);
};

#endif  /* _VERSIONED_KV_STORE_H_ */
