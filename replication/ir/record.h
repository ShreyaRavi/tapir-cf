// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * log.h:
 *   a replica's log of pending and committed operations
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

#ifndef _IR_RECORD_H_
#define _IR_RECORD_H_

#include <map>
#include <string>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/common/request.pb.h"
#include "replication/common/viewstamp.h"
#include "replication/ir/ir-proto.pb.h"

namespace replication {
namespace ir {

typedef std::pair<uint64_t, uint64_t> opid_t;

enum RecordEntryState {
    RECORD_STATE_TENTATIVE = 0,
    RECORD_STATE_FINALIZED = 1
};

enum RecordEntryType {
    RECORD_TYPE_INCONSISTENT = 0,
    RECORD_TYPE_CONSENSUS = 1
};

struct RecordEntry
{
    view_t view;
    opid_t opid;
    RecordEntryState state;
    RecordEntryType type;
    Request request;
    Reply result;

    void* resultCf;
    
    RecordEntry(){};
    RecordEntry(view_t view, opid_t opid, RecordEntryState state,
                RecordEntryType type, const Request &request,
                void* resultPtr, bool useCornflakes = false)
        : view(view),
          opid(opid),
          state(state),
          type(type),
          request(request){
            if (useCornflakes) {
                resultCf = resultPtr;
            } else {
                Reply* replyPtr = (Reply*) resultPtr;
                result = *replyPtr;
            }
          }
    virtual ~RecordEntry() {}
};

class Record
{
public:
    // Use the copy-and-swap idiom to make Record moveable but not copyable
    // [1]. We make it non-copyable to avoid unnecessary copies.
    //
    // [1]: https://stackoverflow.com/a/3279550/3187068
    Record(){ Panic("unimplemented record default constructor"); };
    Record(void* arena, bool useCornflakes);
    Record(const proto::RecordProto &record_proto);
    Record(Record &&other) : Record() { swap(*this, other); }
    Record(const Record &) = delete;
    Record &operator=(const Record &) = delete;
    Record &operator=(Record &&other) {
        swap(*this, other);
        return *this;
    }
    friend void swap(Record &x, Record &y) {
        std::swap(x.entries, y.entries);
    }

    RecordEntry &Add(const RecordEntry& entry);
    RecordEntry &Add(view_t view, opid_t opid, const Request &request,
                     RecordEntryState state,
                     RecordEntryType type);
    RecordEntry &Add(view_t view, opid_t opid, const Request &request,
                     RecordEntryState state, RecordEntryType type,
                     void* resultPtr);
    RecordEntry *Find(opid_t opid);
    bool SetStatus(opid_t opid, RecordEntryState state);
    bool SetResult(opid_t opid, const Reply &result);
    bool SetRequest(opid_t opid, const Request &req);
    void Remove(opid_t opid);
    bool Empty() const;
    void ToProto(proto::RecordProto *proto) const;
    const std::map<opid_t, RecordEntry> &Entries() const;

private:
    std::map<opid_t, RecordEntry> entries;
    void* arena;
    bool useCornflakes;
};

}      // namespace ir
}      // namespace replication
#endif  /* _IR_RECORD_H_ */

