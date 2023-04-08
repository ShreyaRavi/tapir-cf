// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * cftransport.h:
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

#ifndef _LIB_CFTRANSPORT_H_
#define _LIB_CFTRANSPORT_H_

#include "lib/configuration.h"
#include "lib/transport.h"
#include "lib/transportcommon.h"

#include <event2/event.h>

#include <map>
#include <list>
#include <vector>
#include <unordered_map>
#include <random>
#include <mutex>
#include <netinet/in.h>

class CFTransportAddress : public TransportAddress
{
public:
    CFTransportAddress * clone() const;
private:
    CFTransportAddress(const uintptr_t conn_id, const uint32_t msg_id);
    uintptr_t conn_id;
    uint32_t msg_id;
    friend class CFTransport;
    friend bool operator==(const CFTransportAddress &a,
                           const CFTransportAddress &b);
    friend bool operator!=(const CFTransportAddress &a,
                           const CFTransportAddress &b);
    friend bool operator<(const CFTransportAddress &a,
                          const CFTransportAddress &b);
};

class CFTransport : public TransportCommon<CFTransportAddress>
{
public:
    CFTransport(void* mlx5Connection, void* bumpArena);
    virtual ~CFTransport();
    void Register(TransportReceiver *receiver,
                  const transport::Configuration &config,
                  int replicaIdx);
    void Run();
    void Stop();
    int Timer(uint64_t ms, timer_callback_t cb);
    bool CancelTimer(int id);
    void CancelAllTimers();
    
private:
    void* connection;
    void* arena;
    bool stopLoop;

    unordered_map<uint32_t, MessageType> msgid2respType;

    TransportReceiver* receiver;
    
    bool SendMessageInternal(TransportReceiver *src,
                             const CFTransportAddress &dst,
                             const Message &m, bool multicast = false);
    bool SendCFMessageInternal(TransportReceiver *src,
                             const CFTransportAddress &dst,
			     void* m, const MessageType type,
                             bool multicast = false);

    CFTransportAddress
    LookupAddress(const transport::ReplicaAddress &addr);
    CFTransportAddress
    LookupAddress(const transport::Configuration &cfg,
                  int replicaIdx);
    const CFTransportAddress *
    LookupMulticastAddress(const transport::Configuration*config) { return NULL; };
};

#endif  // _LIB_CFTRANSPORT_H_

