// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/timestamp.cc:
 *   A transaction timestamp implementation
 *
 **********************************************************************/

#include "store/common/timestamp.h"
#include "tapir_serialized_cpp.h"

void
Timestamp::operator=(const Timestamp &t)
{
    timestamp = t.timestamp;
    id = t.id;
}

bool
Timestamp::operator==(const Timestamp &t) const
{
    return timestamp == t.timestamp && id == t.id;
}

bool
Timestamp::operator!=(const Timestamp &t) const
{
    return timestamp != t.timestamp || id != t.id;
}

bool
Timestamp::operator>(const Timestamp &t) const
{
    return (timestamp == t.timestamp) ? id > t.id : timestamp > t.timestamp; 
}

bool
Timestamp::operator<(const Timestamp &t) const
{
    return (timestamp == t.timestamp) ? id < t.id : timestamp < t.timestamp; 
}

bool
Timestamp::operator>=(const Timestamp &t) const
{
    return (timestamp == t.timestamp) ? id >= t.id : timestamp >= t.timestamp; 
}

bool
Timestamp::operator<=(const Timestamp &t) const
{
    return (timestamp == t.timestamp) ? id <= t.id : timestamp <= t.timestamp; 
}

bool
Timestamp::isValid() const
{
    return timestamp > 0 && id > 0;
}

void
Timestamp::serialize(void* msg, bool useCornflakes) const
{
    if (useCornflakes) {
        TimestampMessage_set_timestamp(msg, timestamp);
        TimestampMessage_set_id(msg, id);
    } else {
        TimestampMessage* protoMsg = (TimestampMessage*) msg;
        protoMsg->set_timestamp(timestamp);
        protoMsg->set_id(id);
    }
    
}
