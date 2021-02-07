// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/indicusstore/server.cc:
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


#include "store/indicusstore/server.h"

#include <bitset>
#include <queue>
#include <ctime>
#include <chrono>
#include <sys/time.h>

#include "lib/assert.h"
#include "lib/tcptransport.h"
#include "store/indicusstore/common.h"
#include "store/indicusstore/phase1validator.h"
#include "store/indicusstore/localbatchsigner.h"
#include "store/indicusstore/sharedbatchsigner.h"
#include "store/indicusstore/basicverifier.h"
#include "store/indicusstore/localbatchverifier.h"
#include "store/indicusstore/sharedbatchverifier.h"
#include "lib/batched_sigs.h"
#include <valgrind/memcheck.h>

namespace indicusstore {

Server::Server(const transport::Configuration &config, int groupIdx, int idx,
    int numShards, int numGroups, Transport *transport, KeyManager *keyManager,
    Parameters params, uint64_t timeDelta, OCCType occType, Partitioner *part,
    unsigned int batchTimeoutMicro, TrueTime timeServer) :
    PingServer(transport),
    config(config), groupIdx(groupIdx), idx(idx), numShards(numShards),
    numGroups(numGroups), id(groupIdx * config.n + idx),
    transport(transport), occType(occType), part(part),
    params(params), keyManager(keyManager),
    timeDelta(timeDelta),
    timeServer(timeServer)
     {



  store.KVStore_Reserve(4200000);
  //Used for Fallback all to all:
  CreateSessionKeys();
  //////

  Debug("Starting Indicus replica %d.", id);
  transport->Register(this, config, groupIdx, idx);
  _Latency_Init(&committedReadInsertLat, "committed_read_insert_lat");
  _Latency_Init(&verifyLat, "verify_lat");
  _Latency_Init(&signLat, "sign_lat");
  _Latency_Init(&waitingOnLocks, "lock_lat");
  //_Latency_Init(&waitOnProtoLock, "proto_lock_lat");
  //_Latency_Init(&store.storeLockLat, "store_lock_lat");

  if (params.signatureBatchSize == 1) {
    //verifier = new BasicVerifier(transport);
    verifier = new BasicVerifier(transport, batchTimeoutMicro, params.validateProofs &&
      params.signedMessages && params.adjustBatchSize, params.verificationBatchSize);
    batchSigner = nullptr;
  } else {
    if (params.sharedMemBatches) {
      batchSigner = new SharedBatchSigner(transport, keyManager, GetStats(),
          batchTimeoutMicro, params.signatureBatchSize, id,
          params.validateProofs && params.signedMessages &&
          params.signatureBatchSize > 1 && params.adjustBatchSize,
          params.merkleBranchFactor);
    } else {
      batchSigner = new LocalBatchSigner(transport, keyManager, GetStats(),
          batchTimeoutMicro, params.signatureBatchSize, id,
          params.validateProofs && params.signedMessages &&
          params.signatureBatchSize > 1 && params.adjustBatchSize,
          params.merkleBranchFactor);
    }

    if (params.sharedMemVerify) {
      verifier = new SharedBatchVerifier(params.merkleBranchFactor, stats); //add transport if using multithreading
    } else {
      //verifier = new LocalBatchVerifier(params.merkleBranchFactor, stats, transport);
      verifier = new LocalBatchVerifier(params.merkleBranchFactor, stats, transport,
        batchTimeoutMicro, params.validateProofs && params.signedMessages &&
        params.signatureBatchSize > 1 && params.adjustBatchSize, params.verificationBatchSize);
    }
  }

  // this is needed purely from loading data without executing transactions
  proto::CommittedProof *proof = new proto::CommittedProof();
  proof->mutable_txn()->set_client_id(0);
  proof->mutable_txn()->set_client_seq_num(0);
  proof->mutable_txn()->mutable_timestamp()->set_timestamp(0);
  proof->mutable_txn()->mutable_timestamp()->set_id(0);

  committed.insert(std::make_pair("", proof));
}

Server::~Server() {
  std::cerr << "KVStore size: " << store.KVStore_size() << std::endl;
  std::cerr << "ReadStore size: " << store.ReadStore_size() << std::endl;
  std::cerr << "commitGet count: " << commitGet_count << std::endl;

  std::cerr << "Hash count: " << BatchedSigs::hashCount << std::endl;
  std::cerr << "Hash cat count: " << BatchedSigs::hashCatCount << std::endl;
  std::cerr << "Total count: " << BatchedSigs::hashCount + BatchedSigs::hashCatCount << std::endl;

  std::cerr << "Store wait latency (ms): " << store.lock_time << std::endl;
  std::cerr << "parallel OCC lock wait latency (ms): " << total_lock_time_ms << std::endl;
  Latency_Dump(&waitingOnLocks);
  //Latency_Dump(&waitOnProtoLock);
  //Latency_Dump(&batchSigner->waitOnBatchLock);
  //Latency_Dump(&(store.storeLockLat));
  Notice("Freeing verifier.");
  delete verifier;
   //if(params.mainThreadDispatching) committedMutex.lock();
  for (const auto &c : committed) {   ///XXX technically not threadsafe
    delete c.second;
  }
   //if(params.mainThreadDispatching) committedMutex.unlock();
   ////if(params.mainThreadDispatching) ongoingMutex.lock();
  for (const auto &o : ongoing) {     ///XXX technically not threadsafe
    delete o.second;
  }
   ////if(params.mainThreadDispatching) ongoingMutex.unlock();
   if(params.mainThreadDispatching) readReplyProtoMutex.lock();
  for (auto r : readReplies) {
    delete r;
  }
   if(params.mainThreadDispatching) readReplyProtoMutex.unlock();
   if(params.mainThreadDispatching) p1ReplyProtoMutex.lock();
  for (auto r : p1Replies) {
    delete r;
  }
   if(params.mainThreadDispatching) p1ReplyProtoMutex.unlock();
   if(params.mainThreadDispatching) p2ReplyProtoMutex.lock();
  for (auto r : p2Replies) {
    delete r;
  }
   if(params.mainThreadDispatching) p2ReplyProtoMutex.unlock();
  Notice("Freeing signer.");
  if (batchSigner != nullptr) {
    delete batchSigner;
  }
  Latency_Dump(&verifyLat);
  Latency_Dump(&signLat);

}

 void PrintSendCount(){
   send_count++;
   fprintf(stderr, "send count: %d \n", send_count);
 }

 void PrintRcvCount(){
   rcv_count++;
   fprintf(stderr, "rcv count: %d\n", rcv_count);
 }

 void ParseProto(::google::protobuf::Message *msg, std::string &data){
   msg->ParseFromString(data);
 }
// use like this: main dispatches lambda
// auto f = [this, msg, type, data](){
//   ParseProto;
//   auto g = [this, msg, type](){
//     this->HandleType(msg, type);  //HandleType checks which handler to use...
//   };
//   this->transport->DispatchTP_main(g);
//   return (void*) true;
// };
// DispatchTP_noCB(f);
// Main dispatches serialization, and lets serialization thread dispatch to main2


 void Server::ReceiveMessage(const TransportAddress &remote,
       const std::string &type, const std::string &data, void *meta_data) {

  if(params.dispatchMessageReceive){
    Debug("Dispatching message handling to Support Main Thread");
    //using this path results in an extra copy
    //Can I move the data or release the message to avoid duplicates?
   transport->DispatchTP_main([this, &remote, type, data, meta_data]() {
     this->ReceiveMessageInternal(remote, type, data, meta_data);
     return (void*) true;
   });
  }
  else{
    ReceiveMessageInternal(remote, type, data, meta_data);
  }
 }
//TODO: Full CPU utilization parallelism: Assign all handler functions to different threads.
void Server::ReceiveMessageInternal(const TransportAddress &remote,
      const std::string &type, const std::string &data, void *meta_data) {

  if (type == read.GetTypeName()) {

    //if no dispatching OR if dispatching both deser and Handling to 2nd main thread (no workers)
    if(!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.parallel_reads) ){
      read.ParseFromString(data);
      HandleRead(remote, read);
    }
    //if dispatching to second main or other workers
    else{
      proto::Read* readCopy = GetUnusedReadmessage();
      readCopy->ParseFromString(data);
      auto f = [this, &remote, readCopy](){
        this->HandleRead(remote, *readCopy);
        return (void*) true;
      };
      if(params.parallel_reads){
        transport->DispatchTP_noCB(f);
      }
      else{
        transport->DispatchTP_main(f);
      }
    }
  } else if (type == phase1.GetTypeName()) {

    //TODO:: FIX: even if just dispatching to 2nd main thred need to copy,
    if(!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.parallel_CCC)){  //TODO add parallelOCC option
     phase1.ParseFromString(data);
     HandlePhase1(remote, phase1);
    }
    else{
      proto::Phase1 *phase1Copy = GetUnusedPhase1message();
      phase1Copy->ParseFromString(data);
      auto f = [this, &remote, phase1Copy]() {
        this->HandlePhase1(remote, *phase1Copy);
        return (void*) true;
      };
      if(params.dispatchMessageReceive){
        f();
      }
      else{
        Debug("Dispatching HandlePhase1");
        transport->DispatchTP_main(f);
        //transport->DispatchTP_noCB(f); //use if want to dispatch to all workers
      }
    }

  } else if (type == phase2.GetTypeName()) {
      if(params.multiThreading){
          proto::Phase2* p2 = GetUnusedPhase2message();
          p2->ParseFromString(data);
          if(!params.mainThreadDispatching || params.dispatchMessageReceive){
            HandlePhase2(remote, *p2);
          }
          else{
            auto f = [this, &remote, p2](){
              this->HandlePhase2(remote, *p2);
              return (void*) true;
            };
            transport->DispatchTP_main(f);
          }
      }
      else{
        if(!params.mainThreadDispatching || params.dispatchMessageReceive){
          phase2.ParseFromString(data);
          HandlePhase2(remote, phase2);
        }
        else{
          proto::Phase2* p2 = GetUnusedPhase2message();
          p2->ParseFromString(data);
          auto f = [this, &remote, p2](){
            this->HandlePhase2(remote, *p2);
            return (void*) true;
          };
          transport->DispatchTP_main(f);
        }
      }

  } else if (type == writeback.GetTypeName()) {
      if(params.multiThreading){
          proto::Writeback *wb = GetUnusedWBmessage();
          wb->ParseFromString(data);
          if(!params.mainThreadDispatching || params.dispatchMessageReceive){
            HandleWriteback(remote, *wb);
          }
          else{
            auto f = [this, &remote, wb](){
              this->HandleWriteback(remote, *wb);
              return (void*) true;
            };
            transport->DispatchTP_main(f);
          }
      }
      else{
        if(!params.mainThreadDispatching || params.dispatchMessageReceive){
          writeback.ParseFromString(data);
          HandleWriteback(remote, writeback);
        }
        else{
          proto::Writeback *wb = GetUnusedWBmessage();
          wb->ParseFromString(data);
          auto f = [this, &remote, wb](){
            this->HandleWriteback(remote, *wb);
            return (void*) true;
          };
          transport->DispatchTP_main(f);
        }

      }
  } else if (type == abort.GetTypeName()) {
    abort.ParseFromString(data);
    HandleAbort(remote, abort);
  } else if (type == ping.GetTypeName()) {
    ping.ParseFromString(data);
    Debug("Ping is called");
    HandlePingMessage(this, remote, ping);

// Add all Fallback signedMessages
  } else if (type == phase1FB.GetTypeName()) {
    phase1FB.ParseFromString(data);
    HandlePhase1FB(remote, phase1FB);
  } else if (type == phase2FB.GetTypeName()) {
    phase2FB.ParseFromString(data);
    HandlePhase2FB(remote, phase2FB);
  } else if (type == invokeFB.GetTypeName()) {
    invokeFB.ParseFromString(data);
    HandleInvokeFB(remote, invokeFB); //DONT send back to remote, but instead to FB, calculate based on view. (need to include this in TX state thats kept locally.)
  } else if (type == electFB.GetTypeName()) {
    electFB.ParseFromString(data);
    HandleElectFB(remote, electFB);
  } else if (type == decisionFB.GetTypeName()) {
    decisionFB.ParseFromString(data);
    HandleDecisionFB(remote, decisionFB); //DONT send back to remote, but instead to interested clients. (need to include list of interested clients as part of local tx state)
  //} else if (type == moveView.GetTypeName()) {
  //  moveView.ParseFromString(data);
  //  HandleMoveView(remote, moveView); //Send only to other replicas
  } else {
    Panic("Received unexpected message type: %s", type.c_str());
  }
}

//Debug("Stuck at line %d", __LINE__);
void Server::Load(const std::string &key, const std::string &value,
    const Timestamp timestamp) {
  Value val;
  val.val = value;
  auto committedItr = committed.find("");
  UW_ASSERT(committedItr != committed.end());
  val.proof = committedItr->second;
  store.put(key, val, timestamp);
  if (key.length() == 5 && key[0] == 0) {
    std::cerr << std::bitset<8>(key[0]) << ' '
              << std::bitset<8>(key[1]) << ' '
              << std::bitset<8>(key[2]) << ' '
              << std::bitset<8>(key[3]) << ' '
              << std::bitset<8>(key[4]) << ' '
              << std::endl;
  }
}

void Server::HandleRead(const TransportAddress &remote,
     proto::Read &msg) {

  Debug("READ[%lu:%lu] for key %s with ts %lu.%lu.", msg.timestamp().id(),
      msg.req_id(), BytesToHex(msg.key(), 16).c_str(),
      msg.timestamp().timestamp(), msg.timestamp().id());
  Timestamp ts(msg.timestamp());
  if (CheckHighWatermark(ts)) {
    // ignore request if beyond high watermark
    Debug("Read timestamp beyond high watermark.");
    return;
  }

  std::pair<Timestamp, Server::Value> tsVal;
  bool exists = store.get(msg.key(), ts, tsVal);

  proto::ReadReply* readReply = GetUnusedReadReply();
  readReply->set_req_id(msg.req_id());
  readReply->set_key(msg.key());
  if (exists) {
    Debug("READ[%lu] Committed value of length %lu bytes with ts %lu.%lu.",
        msg.req_id(), tsVal.second.val.length(), tsVal.first.getTimestamp(),
        tsVal.first.getID());
    readReply->mutable_write()->set_committed_value(tsVal.second.val);
    tsVal.first.serialize(readReply->mutable_write()->mutable_committed_timestamp());
    if (params.validateProofs) {
      *readReply->mutable_proof() = *tsVal.second.proof;
    }
  }

  TransportAddress *remoteCopy = remote.clone();

  auto sendCB = [this, remoteCopy, readReply]() {
    this->transport->SendMessage(this, *remoteCopy, *readReply);
    delete remoteCopy;
    FreeReadReply(readReply);
  };

  if (occType == MVTSO) {
    /* update rts */
    // TODO: For "proper Aborts": how to track RTS by transaction without knowing transaction digest?

    //XXX multiple RTS as set:
    //  if(params.mainThreadDispatching) rtsMutex.lock();
    // rts[msg.key()].insert(ts);
    //  if(params.mainThreadDispatching) rtsMutex.unlock();
    //XXX single RTS that updates:
     auto itr = rts.find(msg.key());
     if(itr != rts.end()){
       if(ts.getTimestamp() > itr->second ) {
         rts[msg.key()] = ts.getTimestamp();
       }
     }
     else{
       rts[msg.key()] = ts.getTimestamp();
     }


    /* add prepared deps */
    if (params.maxDepDepth > -2) {
      const proto::Transaction *mostRecent = nullptr;

      //TODO:: make threadsafe.
      //std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[write.key()];
      auto itr = preparedWrites.find(msg.key());
      if (itr != preparedWrites.end()){

        //std::pair &x = preparedWrites[write.key()];
        std::shared_lock lock(itr->second.first);
        if(itr->second.second.size() > 0) {

          // there is a prepared write for the key being read
          for (const auto &t : itr->second.second) {
            if (mostRecent == nullptr || t.first > Timestamp(mostRecent->timestamp())) { //TODO: for efficiency only use it if its bigger than the committed write..
              mostRecent = t.second;
            }
          }

          if (mostRecent != nullptr) {
            std::string preparedValue;
            for (const auto &w : mostRecent->write_set()) {
              if (w.key() == msg.key()) {
                preparedValue = w.value();
                break;
              }
            }

            Debug("Prepared write with most recent ts %lu.%lu.",
                mostRecent->timestamp().timestamp(), mostRecent->timestamp().id());

            if (params.maxDepDepth == -1 || DependencyDepth(mostRecent) <= params.maxDepDepth) {
              readReply->mutable_write()->set_prepared_value(preparedValue);
              *readReply->mutable_write()->mutable_prepared_timestamp() = mostRecent->timestamp();
              *readReply->mutable_write()->mutable_prepared_txn_digest() = TransactionDigest(*mostRecent, params.hashDigest);
            }
          }
        }
      }
    }
  }


  if (params.validateProofs && params.signedMessages &&
      (readReply->write().has_committed_value() || (params.verifyDeps && readReply->write().has_prepared_value()))) {

//If readReplyBatch is false then respond immediately, otherwise respect batching policy
    if (params.readReplyBatch) {
      proto::Write* write = new proto::Write(readReply->write());
      // move: sendCB = std::move(sendCB) or {std::move(sendCB)}
      MessageToSign(write, readReply->mutable_signed_write(), [sendCB, write]() {
        sendCB();
        delete write;
      });

    } else if (params.signatureBatchSize == 1) {

      if(params.multiThreading){
        proto::Write* write = new proto::Write(readReply->write());
        auto f = [this, readReply, sendCB = std::move(sendCB), write]()
        {
          SignMessage(write, keyManager->GetPrivateKey(id), id, readReply->mutable_signed_write());
          sendCB();
          delete write;
          return (void*) true;
        };
        transport->DispatchTP_noCB(std::move(f));
      }
      else{
        proto::Write write(readReply->write());
        SignMessage(&write, keyManager->GetPrivateKey(id), id,
            readReply->mutable_signed_write());
        sendCB();
      }

    } else {

      if(params.multiThreading){

        std::vector<::google::protobuf::Message *> msgs;
        proto::Write* write = new proto::Write(readReply->write()); //TODO might want to re-use
        msgs.push_back(write);
        std::vector<proto::SignedMessage *> smsgs;
        smsgs.push_back(readReply->mutable_signed_write());

        auto f = [this, msgs, smsgs, sendCB = std::move(sendCB), write]()
        {
          SignMessages(msgs, keyManager->GetPrivateKey(id), id, smsgs, params.merkleBranchFactor);
          sendCB();
          delete write;
          return (void*) true;
        };
        transport->DispatchTP_noCB(std::move(f));
      }
      else{
        proto::Write write(readReply->write());
        std::vector<::google::protobuf::Message *> msgs;
        msgs.push_back(&write);
        std::vector<proto::SignedMessage *> smsgs;
        smsgs.push_back(readReply->mutable_signed_write());
        SignMessages(msgs, keyManager->GetPrivateKey(id), id, smsgs, params.merkleBranchFactor);
        sendCB();
      }
    }
  } else {
    sendCB();
  }
  if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeReadmessage(&msg);
}

//////////////////////

void Server::HandlePhase1(const TransportAddress &remote,
    proto::Phase1 &msg) {

  std::string txnDigest = TransactionDigest(msg.txn(), params.hashDigest);
  Debug("PHASE1[%lu:%lu][%s] with ts %lu.", msg.txn().client_id(),
      msg.txn().client_seq_num(), BytesToHex(txnDigest, 16).c_str(),
      msg.txn().timestamp().timestamp());
  proto::ConcurrencyControl::Result result;
  const proto::CommittedProof *committedProof;

  //KEEP track of interested client //TODO: keep track of original client
  // interestedClientsMap::accessor i;
  // bool interestedClientsItr = interestedClients.insert(i, txnDigest);
  // i->second.insert(remote.clone());
  // i.release();
  // //interestedClients[txnDigest].insert(remote.clone());

  // no-replays property, i.e. recover existing decision/result from storage
  //Ignore duplicate requests that are already committed, aborted, or ongoing
  ongoingMap::accessor b;
  if(ongoing.find(b, txnDigest)){
    b.release();
    return;
  }
  if(committed.find(txnDigest) != committed.end() || aborted.find(txnDigest) != aborted.end()) return;

  proto::Transaction *txn = msg.release_txn();
  ongoing.insert(b, std::make_pair(txnDigest, txn));
  b.release();

  p1DecisionsMap::const_accessor c;
  bool p1DecItr = p1Decisions.find(c, txnDigest);
  if(p1DecItr){
    result = c->second;
    c.release();
    //TODO: need to check if result is WAIT: if so, need to add to waitingDeps original client..
    // use original client list and store pairs <txnDigest, <reqID, remote>>
    if (result == proto::ConcurrencyControl::ABORT) {
      p1ConflictsMap::const_accessor d;
      auto p1ConflictsItr = p1Conflicts.find(d, txnDigest);
      //committedProof = p1Conflicts[txnDigest];
      committedProof = d->second;
      d.release();
    }
  } else{
    c.release();
    if (params.validateProofs && params.signedMessages && params.verifyDeps) {
      //for (const auto &dep : msg.txn().deps()) {
      for (const auto &dep : txn->deps()) {
        if (!dep.has_write_sigs()) {
          Debug("Dep for txn %s missing signatures.",
              BytesToHex(txnDigest, 16).c_str());
          return;
        }
        if (!ValidateDependency(dep, &config, params.readDepSize, keyManager,
              verifier)) {
          Debug("VALIDATE Dependency failed for txn %s.",
              BytesToHex(txnDigest, 16).c_str());
          // safe to ignore Byzantine client
          return;
        }
      }
    }
     if(params.mainThreadDispatching) current_viewsMutex.lock();
    current_views[txnDigest] = 0;
     if(params.mainThreadDispatching) current_viewsMutex.unlock();

    //proto::Transaction *txn = msg.release_txn();

     // //if(params.mainThreadDispatching) ongoingMutex.lock();
     // ongoingMap::accessor b;
     // ongoing.insert(b, std::make_pair(txnDigest, txn));
     // b.release();
     // //ongoing[txnDigest] = txn;
     // //if(params.mainThreadDispatching) ongoingMutex.unlock();

    Timestamp retryTs;

    if(!params.parallel_CCC || !params.mainThreadDispatching){
      result = DoOCCCheck(msg.req_id(), remote, txnDigest, *txn, retryTs,
          committedProof);
    }
    else{
      auto f = [this, msg_ptr = &msg, remote_ptr = &remote, txnDigest, txn, committedProof]() mutable {
        Timestamp retryTs;
          //check if concurrently committed/aborted already, and if so return
          ongoingMap::const_accessor b;
          if(!ongoing.find(b, txnDigest)){
            Debug("Already concurrently Committed/Aborted txn[%s]", BytesToHex(txnDigest, 16).c_str());
            return (void*) false;
          }
          b.release();

        proto::ConcurrencyControl::Result *result = new proto::ConcurrencyControl::Result(this->DoOCCCheck(msg_ptr->req_id(),
        *remote_ptr, txnDigest, *txn, retryTs, committedProof));

        HandlePhase1CB(msg_ptr, *result, committedProof, txnDigest, *remote_ptr);
        delete result;
        return (void*) true;
      };
      transport->DispatchTP_noCB(std::move(f));
      return;
    }
  }

  HandlePhase1CB(&msg, result, committedProof, txnDigest, remote);
}

//TODO: move p1Decision into this function (not sendp1: Then, can unlock here.)
void Server::HandlePhase1CB(proto::Phase1 *msg, proto::ConcurrencyControl::Result result,
  const proto::CommittedProof* &committedProof, std::string &txnDigest, const TransportAddress &remote){
  if (result != proto::ConcurrencyControl::WAIT) {
    //XXX setting client time outs for Fallback
    // if(client_starttime.find(txnDigest) == client_starttime.end()){
    //   struct timeval tv;
    //   gettimeofday(&tv, NULL);
    //   uint64_t start_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
    //   client_starttime[txnDigest] = start_time;
    // }//time(NULL); //TECHNICALLY THIS SHOULD ONLY START FOR THE ORIGINAL CLIENT, i.e. if another client manages to do it first it shouldnt count... Then again, that client must have gotten it somewhere, so the timer technically started.
    SendPhase1Reply(msg->req_id(), result, committedProof, txnDigest, &remote);
  }
  if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) FreePhase1message(msg);
}


void Server::HandlePhase2CB(proto::Phase2 *msg, const std::string* txnDigest,
  signedCallback sendCB, proto::Phase2Reply* phase2Reply, cleanCallback cleanCB, void* valid){

  Debug("HandlePhase2CB invoked");
  if(!valid){
    Debug("VALIDATE P1Replies for TX %s failed.", BytesToHex(*txnDigest, 16).c_str());
    cleanCB(); //deletes SendCB resources should SendCB not be called.
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreePhase2message(msg); //const_cast<proto::Phase2&>(msg));
    }
    return;
  }

  auto f = [this, msg, txnDigest, sendCB = std::move(sendCB), phase2Reply, cleanCB = std::move(cleanCB), valid ](){

     if(params.mainThreadDispatching) p2DecisionsMutex.lock();
     if(params.mainThreadDispatching) current_viewsMutex.lock();
     if(params.mainThreadDispatching) decision_viewsMutex.lock();

    if(p2Decisions.find(*txnDigest) != p2Decisions.end()){
      proto::CommitDecision &decision = p2Decisions[*txnDigest];
      phase2Reply->mutable_p2_decision()->set_decision(decision);
    }
    else{
      p2Decisions[*txnDigest] = msg->decision();
      current_views[*txnDigest] = 0;
      decision_views[*txnDigest] = 0;
      phase2Reply->mutable_p2_decision()->set_decision(msg->decision());
    }

   if(params.mainThreadDispatching) p2DecisionsMutex.unlock();
   if(params.mainThreadDispatching) current_viewsMutex.unlock();

    //XXX start client timeout for Fallback relay
    // if(client_starttime.find(*txnDigest) == client_starttime.end()){
    //   struct timeval tv;
    //   gettimeofday(&tv, NULL);
    //   uint64_t start_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
    //   client_starttime[*txnDigest] = start_time;
    // }

    if (params.validateProofs) {
      // TODO: need a way for a process to know the decision view when verifying the signed p2_decision
      phase2Reply->mutable_p2_decision()->set_view(decision_views[*txnDigest]);
    }

   if(params.mainThreadDispatching) decision_viewsMutex.unlock();

  //Free allocated memory
  if(params.multiThreading || params.mainThreadDispatching){
    FreePhase2message(msg); // const_cast<proto::Phase2&>(msg));
  }

  if (params.validateProofs && params.signedMessages) {
    proto::Phase2Decision* p2Decision = new proto::Phase2Decision(phase2Reply->p2_decision());

    MessageToSign(p2Decision, phase2Reply->mutable_signed_p2_decision(),
        [sendCB, p2Decision]() {
        sendCB();
        delete p2Decision;
        });
    return (void*) true;
  }
  sendCB();
  return (void*) true;
 };

 if(params.multiThreading && params.mainThreadDispatching && params.dispatchCallbacks){
   transport->DispatchTP_main(std::move(f));
 }
 else{
   f();
 }
}


//TODO: ADD AUTHENTICATION IN ORDER TO ENFORCE FB TIMEOUTS. ANYBODY THAT IS NOT THE ORIGINAL CLIENT SHOULD ONLY BE ABLE TO SEND P2FB messages and NOT normal P2!!!!!!
// //TODO: client signatures need to be implemented. keymanager needs to associate with client ids.
// client ids can just start after all replica ids

//     if(params.clientAuthenticated){ //TODO: this branch needs to go around the validate Proofs.
//       if(!msg.has_auth_p2dec()){
//         Debug("PHASE2 message not authenticated");
//         return;
//       }
//       proto::Phase2ClientDecision p2cd;
//       p2cd.ParseFromString(msg.auth_p2dec().data());
//       //TODO: the parsing of txn and txn_digest
//
//       if(msg.auth_p2dec().process_id() != txn.client_id()){
//         Debug("PHASE2 message from unauthenticated client");
//         return;
//       }
//       //Todo: Get client key... Currently unimplemented. Clients not authenticated.
//       crypto::Verify(clientPublicKey, &msg.auth_p2dec().data()[0], msg.auth_p2dec().data().length(), &msg.auth_p2dec().signature()[0]);
//
//
//       //TODO: 1) check that message is signed
//       //TODO: 2) check if id matches txn id. Access
//       //TODO: 3) check whether signature validates.
//     }
//     else{
//
//     }

void Server::HandlePhase2(const TransportAddress &remote,
       proto::Phase2 &msg) {

  const proto::Transaction *txn;
  std::string computedTxnDigest;
  const std::string *txnDigest = &computedTxnDigest;
  if (params.validateProofs) {
    if (!msg.has_txn() && !msg.has_txn_digest()) {
      Debug("PHASE2 message contains neither txn nor txn_digest.");
      return;
    }

    if (msg.has_txn_digest() ) {
      txnDigest = &msg.txn_digest();
      ongoingMap::const_accessor b;
      auto txnItr = ongoing.find(b, msg.txn_digest());
      if(txnItr){
        txn = b->second;
        b.release();
      }
      else{
        b.release();
        if(msg.has_txn()){
          txn = &msg.txn();
          // check that digest and txn match..
           if(*txnDigest !=TransactionDigest(*txn, params.hashDigest)) return;
        }
        else{
          Debug("PHASE2[%s] message does not contain txn, but have not seen"
              " txn_digest previously.", BytesToHex(msg.txn_digest(), 16).c_str());
          return;
        }
      }

    } else {
      txn = &msg.txn();
      computedTxnDigest = TransactionDigest(msg.txn(), params.hashDigest);
      txnDigest = &computedTxnDigest;
    }
  }
  else{
    txnDigest = &dummyString;  //just so I can run with validate turned off while using params.mainThreadDispatching
  }

  proto::Phase2Reply* phase2Reply = GetUnusedPhase2Reply();
  TransportAddress *remoteCopy = remote.clone();

  auto sendCB = [this, remoteCopy, phase2Reply, txnDigest]() {
    this->transport->SendMessage(this, *remoteCopy, *phase2Reply);
    Debug("PHASE2[%s] Sent Phase2Reply.", BytesToHex(*txnDigest, 16).c_str());
    FreePhase2Reply(phase2Reply);
    delete remoteCopy;
  };
  auto cleanCB = [this, remoteCopy, phase2Reply]() {
    FreePhase2Reply(phase2Reply);
    delete remoteCopy;
  };

  phase2Reply->set_req_id(msg.req_id());
  *phase2Reply->mutable_p2_decision()->mutable_txn_digest() = *txnDigest;
  phase2Reply->mutable_p2_decision()->set_involved_group(groupIdx);

  // no-replays property, i.e. recover existing decision/result from storage (do this for HandlePhase1 as well.)
  if(params.mainThreadDispatching) p2DecisionsMutex.lock();

  if(p2Decisions.find(msg.txn_digest()) != p2Decisions.end()){
    if(params.mainThreadDispatching) p2DecisionsMutex.unlock();
     //XXX Handler logic was moved to HandlePhase2CB
  }
  //first time receiving p2 message:
  else{
      if(params.mainThreadDispatching) p2DecisionsMutex.unlock();
    Debug("PHASE2[%s].", BytesToHex(*txnDigest, 16).c_str());

    int64_t myProcessId;
    proto::ConcurrencyControl::Result myResult;
    LookupP1Decision(*txnDigest, myProcessId, myResult);


    if (params.validateProofs && params.signedMessages) {
        if(params.multiThreading){

          mainThreadCallback mcb(std::bind(&Server::HandlePhase2CB, this,
             &msg, txnDigest, sendCB, phase2Reply, cleanCB, std::placeholders::_1));

          //OPTION 1: Validation itself is synchronous, i.e. is one job (Would need to be extended with thread safety)
              //std::function<void*()> f (std::bind(&ValidateP1RepliesWrapper, msg_copy.decision(), false, txn, txnDigest, msg.grouped_sigs(), keyManager, &config, myProcessId, myResult, verifier));
              //transport->DispatchTP(f, mcb);

          //OPTION2: Validation itself is asynchronous (each verification = 1 job)
          if(params.batchVerification){
            asyncBatchValidateP1Replies(msg.decision(),
                  false, txn, txnDigest, msg.grouped_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier, std::move(mcb), transport, true);
            return;

          }
          else{
            asyncValidateP1Replies(msg.decision(),
                  false, txn, txnDigest, msg.grouped_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier, std::move(mcb), transport, true);
            return;
          }
        }
        else{
          if(params.batchVerification){
            mainThreadCallback mcb(std::bind(&Server::HandlePhase2CB, this,
              &msg, txnDigest, sendCB, phase2Reply, cleanCB, std::placeholders::_1));

            asyncBatchValidateP1Replies(msg.decision(),
                  false, txn, txnDigest, msg.grouped_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier, std::move(mcb), transport, false);
            return;
          }
          else{
            if(!ValidateP1Replies(msg.decision(),
                  false, txn, txnDigest, msg.grouped_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier)) {
              Debug("VALIDATE P1Replies failed.");
              FreePhase2Reply(phase2Reply);
              delete remoteCopy;
              return;
            }
          }
        }
      }

  }
  HandlePhase2CB(&msg, txnDigest, sendCB, phase2Reply, cleanCB, (void*) true);

}

void Server::WritebackCallback(proto::Writeback *msg, const std::string* txnDigest,
  proto::Transaction* txn, void* valid){

  if(! valid){
    Debug("VALIDATE Writeback for TX %s failed.", BytesToHex(*txnDigest, 16).c_str());
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreeWBmessage(msg);
    }
    return;
  }

  auto f = [this, msg, txnDigest, txn, valid]() mutable {
      Debug("WRITEBACK Callback[%s] being called", BytesToHex(*txnDigest, 16).c_str());

      ///////////////////////////// Below: Only executed by MainThread
      // tbb::concurrent_hash_map<std::string, std::mutex>::accessor z;
      // completing.insert(z, *txnDigest);
      // //z->second.lock();
      // z.release();

      if (msg->decision() == proto::COMMIT) {
        Debug("WRITEBACK[%s] successfully committing.", BytesToHex(*txnDigest, 16).c_str());
        bool p1Sigs = msg->has_p1_sigs();
        uint64_t view = -1;
        if(!p1Sigs){
          if(msg->has_p2_sigs() && msg->has_p2_view()){
            view = msg->p2_view();
          }
          else{
            Debug("Writeback for P2 does not have view or sigs");
            //return; //XXX comment back
            view = 0;
            //return (void*) false;
          }
        }
        Debug("COMMIT ONLY RUN BY MAINTHREAD: %d", sched_getcpu());
        Commit(*txnDigest, txn, p1Sigs ? msg->release_p1_sigs() : msg->release_p2_sigs(), p1Sigs, view);
      } else {
        Debug("WRITEBACK[%s] successfully aborting.", BytesToHex(*txnDigest, 16).c_str());
        //msg->set_allocated_txn(txn); //dont need to set since client will?
        writebackMessages[*txnDigest] = *msg;  //Only necessary for fallback... (could avoid storing these, if one just replied with a p2 vote instea - but that is not as responsive)

        ///TODO: msg might no longer hold txn; could have been released in HanldeWriteback
        Abort(*txnDigest);
      }

      if(params.multiThreading || params.mainThreadDispatching){
        FreeWBmessage(msg);
      }
      return (void*) true;
  };

 if(params.multiThreading && params.mainThreadDispatching && params.dispatchCallbacks){
   transport->DispatchTP_main(std::move(f));
 }
 else{
   f();
 }

}


void Server::HandleWriteback(const TransportAddress &remote,
    proto::Writeback &msg) {
  stats.Increment("total_transactions", 1);

  proto::Transaction *txn;
  const std::string *txnDigest;
  std::string computedTxnDigest;
  if (!msg.has_txn() && !msg.has_txn_digest()) {
    Debug("WRITEBACK message contains neither txn nor txn_digest.");
    return;
  }

  if (msg.has_txn_digest() ) {
    txnDigest = &msg.txn_digest();
    ongoingMap::const_accessor b;
    auto txnItr = ongoing.find(b, msg.txn_digest());
    if(txnItr){
      txn = b->second;
      b.release();
    }
    else{
      b.release();
      if(msg.has_txn()){
        txn = msg.release_txn();
        // check that digest and txn match..
         if(*txnDigest !=TransactionDigest(*txn, params.hashDigest)) return;
      }
      else{
        Debug("PHASE2[%s] message does not contain txn, but have not seen"
            " txn_digest previously.", BytesToHex(msg.txn_digest(), 16).c_str());
        return;
      }
    }
  } else {
    txn = msg.release_txn();
    computedTxnDigest = TransactionDigest(msg.txn(), params.hashDigest);
    txnDigest = &computedTxnDigest;
  }


  if(committed.find(*txnDigest) != committed.end() || aborted.find(*txnDigest) != aborted.end()){
    return; //duplicate TODO: Forward to all interested clients and empty it?
  }

  Debug("WRITEBACK[%s] with decision %d.",
      BytesToHex(*txnDigest, 16).c_str(), msg.decision());

  //Verifying signatures
  //XXX batchVerification branches are currently deprecated
  if (params.validateProofs ) {
      if(params.multiThreading){

          Debug("1: TAKING MULTITHREADING BRANCH, generating MCB");
          mainThreadCallback mcb(std::bind(&Server::WritebackCallback, this, &msg,
            txnDigest, txn, std::placeholders::_1));

          if(params.signedMessages && msg.decision() == proto::COMMIT && msg.has_p1_sigs()){
            stats.Increment("total_transactions_fast_commit", 1);
            int64_t myProcessId;
            proto::ConcurrencyControl::Result myResult;
            LookupP1Decision(*txnDigest, myProcessId, myResult);

            if(params.batchVerification){
              Debug("2: Taking batch branch p1 commit");
              asyncBatchValidateP1Replies(msg.decision(),
                    true, txn, txnDigest, msg.p1_sigs(), keyManager, &config, myProcessId,
                    myResult, verifier, std::move(mcb), transport, true);
            }
            else{
                  Debug("2: Taking non-batch branch p1 commit");
            asyncValidateP1Replies(msg.decision(),
                  true, txn, txnDigest, msg.p1_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier, std::move(mcb), transport, true);
            }
            return;


          }
          else if(params.signedMessages && msg.decision() == proto::ABORT && msg.has_p1_sigs()){
            stats.Increment("total_transactions_fast_Abort_sigs", 1);
            int64_t myProcessId;
            proto::ConcurrencyControl::Result myResult;
            LookupP1Decision(*txnDigest, myProcessId, myResult);

            if(params.batchVerification){
              Debug("2: Taking batch branch p1 abort");
              asyncBatchValidateP1Replies(msg.decision(),
                    true, txn, txnDigest, msg.p1_sigs(), keyManager, &config, myProcessId,
                    myResult, verifier, std::move(mcb), transport, true);
            }
            else{
              Debug("2: Taking non-batch branch p1 abort");
            asyncValidateP1Replies(msg.decision(),
                  true, txn, txnDigest, msg.p1_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier, std::move(mcb), transport, true);
            }
            return;
          }

          else if (params.signedMessages && msg.has_p2_sigs()) {
             stats.Increment("total_transactions_slow", 1);
              // require clients to include view for easier matching
              if(!msg.has_p2_view()) return;
              int64_t myProcessId;
              proto::CommitDecision myDecision;
              LookupP2Decision(*txnDigest, myProcessId, myDecision);

              if(params.batchVerification){
                Debug("2: Taking batch branch p2");
                asyncBatchValidateP2Replies(msg.decision(), msg.p2_view(),
                      txn, txnDigest, msg.p2_sigs(), keyManager, &config, myProcessId,
                      myDecision, verifier, std::move(mcb), transport, true);
              }
              else{
                Debug("2: Taking non-batch branch p2");
                asyncValidateP2Replies(msg.decision(), msg.p2_view(),
                      txn, txnDigest, msg.p2_sigs(), keyManager, &config, myProcessId,
                      myDecision, verifier, std::move(mcb), transport, true);
              }
              return;
          }

          else if (msg.decision() == proto::ABORT && msg.has_conflict()) {
             stats.Increment("total_transactions_fast_Abort_conflict", 1);

              std::string committedTxnDigest = TransactionDigest(msg.conflict().txn(),
                  params.hashDigest);
              asyncValidateCommittedConflict(msg.conflict(), &committedTxnDigest, txn,
                    txnDigest, params.signedMessages, keyManager, &config, verifier,
                    mcb, transport, true, params.batchVerification);
              return;
          }
          else if (params.signedMessages) {

             Debug("WRITEBACK[%s] decision %d, has_p1_sigs %d, has_p2_sigs %d, and"
                 " has_conflict %d.", BytesToHex(*txnDigest, 16).c_str(),
                 msg.decision(), msg.has_p1_sigs(), msg.has_p2_sigs(), msg.has_conflict());
             return;
          }

      }
      //If I make the else case use the async function too, then I can collapse the duplicate code here
      //and just pass params.multiThreading as argument...
      //Currently NOT doing that because the async version does additional copies (binds) that are avoided in the single threaded code.
      else{

          if (params.signedMessages && msg.decision() == proto::COMMIT && msg.has_p1_sigs()) {
            int64_t myProcessId;
            proto::ConcurrencyControl::Result myResult;
            LookupP1Decision(*txnDigest, myProcessId, myResult);

            if(params.batchVerification){
              mainThreadCallback mcb(std::bind(&Server::WritebackCallback, this, &msg, txnDigest, txn, std::placeholders::_1));
              asyncBatchValidateP1Replies(proto::COMMIT,
                    true, txn, txnDigest,msg.p1_sigs(), keyManager, &config, myProcessId,
                    myResult, verifier, std::move(mcb), transport, false);
              return;
            }
            else{
              if (!ValidateP1Replies(proto::COMMIT, true, txn, txnDigest, msg.p1_sigs(),
                    keyManager, &config, myProcessId, myResult, verifyLat, verifier)) {
                Debug("WRITEBACK[%s] Failed to validate P1 replies for fast commit.",
                    BytesToHex(*txnDigest, 16).c_str());
                return;
              }
            }
          }
          else if (params.signedMessages && msg.decision() == proto::ABORT && msg.has_p1_sigs()) {
            int64_t myProcessId;
            proto::ConcurrencyControl::Result myResult;
            LookupP1Decision(*txnDigest, myProcessId, myResult);

            if(params.batchVerification){
              mainThreadCallback mcb(std::bind(&Server::WritebackCallback, this, &msg, txnDigest, txn, std::placeholders::_1));
              asyncBatchValidateP1Replies(proto::ABORT,
                    true, txn, txnDigest,msg.p1_sigs(), keyManager, &config, myProcessId,
                    myResult, verifier, std::move(mcb), transport, false);
              return;
            }
            else{
              if (!ValidateP1Replies(proto::ABORT, true, txn, txnDigest, msg.p1_sigs(),
                    keyManager, &config, myProcessId, myResult, verifyLat, verifier)) {
                Debug("WRITEBACK[%s] Failed to validate P1 replies for fast abort.",
                    BytesToHex(*txnDigest, 16).c_str());
                return;
              }
            }
          }

          else if (params.signedMessages && msg.has_p2_sigs()) {
            if(!msg.has_p2_view()) return;
            int64_t myProcessId;
            proto::CommitDecision myDecision;
            LookupP2Decision(*txnDigest, myProcessId, myDecision);


            if(params.batchVerification){
              mainThreadCallback mcb(std::bind(&Server::WritebackCallback, this, &msg, txnDigest, txn, std::placeholders::_1));
              asyncBatchValidateP2Replies(msg.decision(), msg.p2_view(),
                    txn, txnDigest, msg.p2_sigs(), keyManager, &config, myProcessId,
                    myDecision, verifier, std::move(mcb), transport, false);
              return;
            }
            else{
              if (!ValidateP2Replies(msg.decision(), msg.p2_view(), txn, txnDigest, msg.p2_sigs(),
                    keyManager, &config, myProcessId, myDecision, verifyLat, verifier)) {
                Debug("WRITEBACK[%s] Failed to validate P2 replies for decision %d.",
                    BytesToHex(*txnDigest, 16).c_str(), msg.decision());
                return;
              }
            }

          } else if (msg.decision() == proto::ABORT && msg.has_conflict()) {
            std::string committedTxnDigest = TransactionDigest(msg.conflict().txn(),
                params.hashDigest);

                if(params.batchVerification){
                  mainThreadCallback mcb(std::bind(&Server::WritebackCallback, this, &msg, txnDigest, txn, std::placeholders::_1));
                  asyncValidateCommittedConflict(msg.conflict(), &committedTxnDigest, txn,
                        txnDigest, params.signedMessages, keyManager, &config, verifier,
                        std::move(mcb), transport, false, params.batchVerification);
                  return;
                }
                else{
                  if (!ValidateCommittedConflict(msg.conflict(), &committedTxnDigest, txn,
                        txnDigest, params.signedMessages, keyManager, &config, verifier)) {
                    Debug("WRITEBACK[%s] Failed to validate committed conflict for fast abort.",
                        BytesToHex(*txnDigest, 16).c_str());
                    return;
                }

            }
          } else if (params.signedMessages) {
            Debug("WRITEBACK[%s] decision %d, has_p1_sigs %d, has_p2_sigs %d, and"
                " has_conflict %d.", BytesToHex(*txnDigest, 16).c_str(),
                msg.decision(), msg.has_p1_sigs(), msg.has_p2_sigs(), msg.has_conflict());
            return;
          }
        }

  }

  WritebackCallback(&msg, txnDigest, txn, (void*) true);
}


void Server::HandleAbort(const TransportAddress &remote,
    const proto::Abort &msg) {
  const proto::AbortInternal *abort;
  if (params.validateProofs && params.signedMessages) {
    if (!msg.has_signed_internal()) {
      return;
    }

    //Latency_Start(&verifyLat);
    if (!verifier->Verify(keyManager->GetPublicKey(msg.signed_internal().process_id()),
          msg.signed_internal().data(),
          msg.signed_internal().signature())) {
      //Latency_End(&verifyLat);
      return;
    }
    //Latency_End(&verifyLat);

    if (!abortInternal.ParseFromString(msg.signed_internal().data())) {
      return;
    }

    if (abortInternal.ts().id() != msg.signed_internal().process_id()) {
      return;
    }

    abort = &abortInternal;
  } else {
    UW_ASSERT(msg.has_internal());
    abort = &msg.internal();
  }

  //RECOMMENT XXX currently displaced by RTS version with no set, but only a single replacing RTS
  //  if(params.mainThreadDispatching) rtsMutex.lock();
  // for (const auto &read : abort->read_set()) {
  //   rts[read].erase(abort->ts());
  // }
  //  if(params.mainThreadDispatching) rtsMutex.unlock();
}

proto::ConcurrencyControl::Result Server::DoOCCCheck(
    uint64_t reqId, const TransportAddress &remote,
    const std::string &txnDigest, const proto::Transaction &txn,
    Timestamp &retryTs, const proto::CommittedProof* &conflict,
    bool fallback_flow) {

  locks_t locks;
  //lock keys to perform an atomic OCC check when parallelizing OCC checks.
  if(params.parallel_CCC){
    locks = LockTxnKeys_scoped(txn);
  }

  switch (occType) {
    case TAPIR:
      return DoTAPIROCCCheck(txnDigest, txn, retryTs);
    case MVTSO:
      return DoMVTSOOCCCheck(reqId, remote, txnDigest, txn, conflict, fallback_flow);
    default:
      Panic("Unknown OCC type: %d.", occType);
      return proto::ConcurrencyControl::ABORT;
  }
}

locks_t Server::LockTxnKeys_scoped(const proto::Transaction &txn) {
    // timeval tv1;
    // gettimeofday(&tv1, 0);

    locks_t locks;
    auto itr_r = txn.read_set().begin();
    auto itr_w = txn.write_set().begin();

    //for(int i = 0; i < txn.read_set().size() + txn.write_set().size(); ++i){
    while(itr_r != txn.read_set().end() || itr_w != txn.write_set().end()){
      //skip duplicate keys (since the list is sorted they should be next)
      if(itr_r != txn.read_set().end() && std::next(itr_r) != txn.read_set().end()){
        if(itr_r->key() == std::next(itr_r)->key()){
          itr_r++;
          continue;
        }
      }
      if(itr_w != txn.write_set().end() && std::next(itr_w) != txn.write_set().end()){
        if(itr_w->key() == std::next(itr_w)->key()){
          itr_w++;
          continue;
        }
      }
      //lock and advance read/write respectively if the other set is done
      if(itr_r == txn.read_set().end()){
        //std::cerr<< "Locking Write [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_w->key(), 16).c_str() << "]" << std::endl;
        locks.emplace_back(mutex_map[itr_w->key()]);
        itr_w++;
      }
      else if(itr_w == txn.write_set().end()){
        //std::cerr<< "Locking Read [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_r->key(), 16).c_str() << "]" << std::endl;
        locks.emplace_back(mutex_map[itr_r->key()]);
        itr_r++;
      }
      //lock and advance read/write iterators in order
      else{
        if(itr_r->key() <= itr_w->key()){
          //std::cerr<< "Locking Read/Write [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_r->key(), 16).c_str() << "]" << std::endl;
          locks.emplace_back(mutex_map[itr_r->key()]);
          //If read set and write set share keys, must not acquire lock twice.
          if(itr_r->key() == itr_w->key()) {
            itr_w++;
          }
          itr_r++;
        }
        else{
          //std::cerr<< "Locking Write [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_w->key(), 16).c_str() << "]" << std::endl;
          locks.emplace_back(mutex_map[itr_w->key()]);
          itr_w++;
        }
      }
    }
    // timeval tv2;
    // gettimeofday(&tv2, 0);
    // total_lock_time_ms += (tv2.tv_sec * 1000  + tv2.tv_usec / 1000) - (tv1.tv_sec * 1000  + tv1.tv_usec / 1000);
    return locks;
}

//XXX DEPRECATED
void Server::LockTxnKeys(proto::Transaction &txn){
  // Lock all (read/write) keys in order for atomicity if using parallel OCC

    auto itr_r = txn.read_set().begin();
    auto itr_w = txn.write_set().begin();
    //for(int i = 0; i < txn.read_set().size() + txn.write_set().size(); ++i){
    while(itr_r != txn.read_set().end() || itr_w != txn.write_set().end()){
      if(itr_r == txn.read_set().end()){
        lock_keys[itr_w->key()].lock();
        itr_w++;
      }
      else if(itr_w == txn.write_set().end()){
        lock_keys[itr_r->key()].lock();
        itr_r++;
      }
      else{
        if(itr_r->key() <= itr_w->key()){
          lock_keys[itr_r->key()].lock();
          //If read set and write set share keys, must not acquire lock twice.
          if(itr_r->key() == itr_w->key()) { itr_w++;}
          itr_r++;
        }
        else{
          lock_keys[itr_w->key()].lock();
          itr_w++;
        }
      }
    }
}
//XXX DEPRECATED
void Server::UnlockTxnKeys(proto::Transaction &txn){
  // Lock all (read/write) keys in order for atomicity if using parallel OCC
    auto itr_r = txn.read_set().rbegin();
    auto itr_w = txn.write_set().rbegin();
    //for(int i = 0; i < txn.read_set().size() + txn.write_set().size(); ++i){
    while(itr_r != txn.read_set().rend() || itr_w != txn.write_set().rend()){
      if(itr_r == txn.read_set().rend()){
        lock_keys[itr_w->key()].unlock();
        itr_w++;
      }
      else if(itr_w == txn.write_set().rend()){
        lock_keys[itr_r->key()].unlock();
        itr_r++;
      }
      else{
        if(itr_r->key() > itr_w->key()){
          lock_keys[itr_r->key()].unlock();
          itr_r++;
        }
        else{
          lock_keys[itr_w->key()].unlock();
          if(itr_r->key() == itr_w->key()) { itr_r++;}
          itr_w++;
        }
      }
    }
}

proto::ConcurrencyControl::Result Server::DoTAPIROCCCheck(
    const std::string &txnDigest, const proto::Transaction &txn,
    Timestamp &retryTs) {
  Debug("[%s] START PREPARE", txnDigest.c_str());


  if(params.mainThreadDispatching) preparedMutex.lock_shared();
  preparedMap::const_accessor a;
  auto preparedItr = prepared.find(a, txnDigest);
  if(preparedItr){
      if(a->second.first == txn.timestamp()){
  //if (preparedItr != prepared.end()) {
  //  if (preparedItr->second.first == txn.timestamp()) {
      Warning("[%s] Already Prepared!", txnDigest.c_str());
      if(params.mainThreadDispatching) preparedMutex.unlock_shared();
      return proto::ConcurrencyControl::COMMIT;
    } else {
      // run the checks again for a new timestamp
      if(params.mainThreadDispatching) preparedMutex.unlock_shared();
      Clean(txnDigest);
    }
  }
  a.release();
  // do OCC checks
  std::unordered_map<std::string, std::set<Timestamp>> pReads;
  GetPreparedReadTimestamps(pReads);

  // check for conflicts with the read set
  for (const auto &read : txn.read_set()) {
    std::pair<Timestamp, Timestamp> range;
     //if(params.mainThreadDispatching) storeMutex.lock();
    bool ret = store.getRange(read.key(), read.readtime(), range);
     //if(params.mainThreadDispatching) storeMutex.unlock();

    Debug("Range %lu %lu %lu", Timestamp(read.readtime()).getTimestamp(),
        range.first.getTimestamp(), range.second.getTimestamp());

    // if we don't have this key then no conflicts for read
    if (!ret) {
      continue;
    }

    // if we don't have this version then no conflicts for read
    if (range.first != read.readtime()) {
      continue;
    }

    // if the value is still valid
    if (!range.second.isValid()) {
      // check pending writes.
      //auto preparedWritesMutexScope = params.mainThreadDispatching ? std::shared_lock<std::shared_mutex>(preparedWritesMutex) : std::shared_lock<std::shared_mutex>();

      if (preparedWrites.find(read.key()) != preparedWrites.end()) {
        Debug("[%lu,%lu] ABSTAIN rw conflict w/ prepared key %s.",
            txn.client_id(),
            txn.client_seq_num(),
            BytesToHex(read.key(), 16).c_str());
        stats.Increment("cc_abstains", 1);
        stats.Increment("cc_abstains_rw_conflict", 1);
        return proto::ConcurrencyControl::ABSTAIN;
      }
    } else {
      // if value is not still valtxnDigest, then abort.
      /*if (Timestamp(txn.timestamp()) <= range.first) {
        Warning("timestamp %lu <= range.first %lu (range.second %lu)",
            txn.timestamp().timestamp(), range.first.getTimestamp(),
            range.second.getTimestamp());
      }*/
      //UW_ASSERT(timestamp > range.first);
      Debug("[%s] ABORT rw conflict: %lu > %lu", txnDigest.c_str(),
          txn.timestamp().timestamp(), range.second.getTimestamp());
      stats.Increment("cc_aborts", 1);
      stats.Increment("cc_aborts_rw_conflict", 1);
      return proto::ConcurrencyControl::ABORT;
    }
  }

  // check for conflicts with the write set
  for (const auto &write : txn.write_set()) {
    std::pair<Timestamp, Server::Value> val;
    // if this key is in the store
     //if(params.mainThreadDispatching) storeMutex.lock();
    if (store.get(write.key(), val)) {
      Timestamp lastRead;
      bool ret;

      // if the last committed write is bigger than the timestamp,
      // then can't accept
      if (val.first > Timestamp(txn.timestamp())) {
        Debug("[%s] RETRY ww conflict w/ prepared key:%s", txnDigest.c_str(),
            write.key().c_str());
        retryTs = val.first;
        stats.Increment("cc_retries_committed_write", 1);
         //if(params.mainThreadDispatching) storeMutex.unlock();
        return proto::ConcurrencyControl::ABSTAIN;
      }

      // if last committed read is bigger than the timestamp, can't
      // accept this transaction, but can propose a retry timestamp

      // we get the timestamp of the last read ever on this object
      ret = store.getLastRead(write.key(), lastRead);

      // if this key is in the store and has been read before
      if (ret && lastRead > Timestamp(txn.timestamp())) {
        Debug("[%s] RETRY wr conflict w/ prepared key:%s", txnDigest.c_str(),
            write.key().c_str());
        retryTs = lastRead;
         //if(params.mainThreadDispatching) storeMutex.unlock();
        return proto::ConcurrencyControl::ABSTAIN;
      }
    }
     //if(params.mainThreadDispatching) storeMutex.unlock();

    // if there is a pending write for this key, greater than the
    // proposed timestamp, retry

     //if(params.mainThreadDispatching) preparedWritesMutex.lock_shared();

     auto itr = preparedWrites.find(write.key());

    if (itr != preparedWrites.end()) {
      itr->second.first.lock_shared();
      std::map<Timestamp, const proto::Transaction *>::iterator it =
          itr->second.second.upper_bound(txn.timestamp());
      if (it != itr->second.second.end() ) {
        Debug("[%s] RETRY ww conflict w/ prepared key:%s", txnDigest.c_str(),
            write.key().c_str());
        retryTs = it->first;
        stats.Increment("cc_retries_prepared_write", 1);
         //if(params.mainThreadDispatching) preparedWritesMutex.unlock_shared();
         itr->second.first.unlock_shared();
        return proto::ConcurrencyControl::ABSTAIN;
      }
      itr->second.first.unlock_shared();
    }
     //if(params.mainThreadDispatching) preparedWritesMutex.unlock_shared();


    //if there is a pending read for this key, greater than the
    //propsed timestamp, abstain
    if (pReads.find(write.key()) != pReads.end() &&
        pReads[write.key()].upper_bound(txn.timestamp()) !=
        pReads[write.key()].end()) {
      Debug("[%s] ABSTAIN wr conflict w/ prepared key: %s",
            txnDigest.c_str(), write.key().c_str());
      stats.Increment("cc_abstains", 1);
      return proto::ConcurrencyControl::ABSTAIN;
    }
  }

  // Otherwise, prepare this transaction for commit
  Prepare(txnDigest, txn);

  Debug("[%s] PREPARED TO COMMIT", txnDigest.c_str());

  return proto::ConcurrencyControl::COMMIT;
}

proto::ConcurrencyControl::Result Server::DoMVTSOOCCCheck(
    uint64_t reqId, const TransportAddress &remote,
    const std::string &txnDigest, const proto::Transaction &txn,
    const proto::CommittedProof* &conflict, bool fallback_flow) {
  Debug("PREPARE[%lu:%lu][%s] with ts %lu.%lu.",
      txn.client_id(), txn.client_seq_num(),
      BytesToHex(txnDigest, 16).c_str(),
      txn.timestamp().timestamp(), txn.timestamp().id());
  Timestamp ts(txn.timestamp());


  preparedMap::const_accessor a;
  bool preparedItr = prepared.find(a, txnDigest);
  //if (preparedItr == prepared.end()) {
  if(!preparedItr){
    a.release();
    if (CheckHighWatermark(ts)) {
      Debug("[%lu:%lu][%s] ABSTAIN ts %lu beyond high watermark.",
          txn.client_id(), txn.client_seq_num(),
          BytesToHex(txnDigest, 16).c_str(),
          ts.getTimestamp());
      stats.Increment("cc_abstains", 1);
      stats.Increment("cc_abstains_watermark", 1);
      return proto::ConcurrencyControl::ABSTAIN;
    }
    for (const auto &read : txn.read_set()) {
      // TODO: remove this check when txns only contain read set/write set for the
      //   shards stored at this replica
      if (!IsKeyOwned(read.key())) {
        continue;
      }

      std::vector<std::pair<Timestamp, Server::Value>> committedWrites;
      GetCommittedWrites(read.key(), read.readtime(), committedWrites);
      for (const auto &committedWrite : committedWrites) {
        // readVersion < committedTs < ts
        //     GetCommittedWrites only returns writes larger than readVersion
        if (committedWrite.first < ts) {
          if (params.validateProofs) {
            conflict = committedWrite.second.proof;
          }
          Debug("[%lu:%lu][%s] ABORT wr conflict committed write for key %s:"
              " this txn's read ts %lu.%lu < committed ts %lu.%lu < this txn's ts %lu.%lu.",
              txn.client_id(),
              txn.client_seq_num(),
              BytesToHex(txnDigest, 16).c_str(),
              BytesToHex(read.key(), 16).c_str(),
              read.readtime().timestamp(),
              read.readtime().id(), committedWrite.first.getTimestamp(),
              committedWrite.first.getID(), ts.getTimestamp(), ts.getID());
          stats.Increment("cc_aborts", 1);
          stats.Increment("cc_aborts_wr_conflict", 1);
          return proto::ConcurrencyControl::ABORT;
        }
      }

      const auto preparedWritesItr = preparedWrites.find(read.key());
      if (preparedWritesItr != preparedWrites.end()) {

        std::shared_lock lock(preparedWritesItr->second.first);
        for (const auto &preparedTs : preparedWritesItr->second.second) {
          if (Timestamp(read.readtime()) < preparedTs.first && preparedTs.first < ts) {
            Debug("[%lu:%lu][%s] ABSTAIN wr conflict prepared write for key %s:"
              " this txn's read ts %lu.%lu < prepared ts %lu.%lu < this txn's ts %lu.%lu.",
                txn.client_id(),
                txn.client_seq_num(),
                BytesToHex(txnDigest, 16).c_str(),
                BytesToHex(read.key(), 16).c_str(),
                read.readtime().timestamp(),
                read.readtime().id(), preparedTs.first.getTimestamp(),
                preparedTs.first.getID(), ts.getTimestamp(), ts.getID());
            stats.Increment("cc_abstains", 1);
            stats.Increment("cc_abstains_wr_conflict", 1);
            return proto::ConcurrencyControl::ABSTAIN;
          }
        }
      }
    }

    for (const auto &write : txn.write_set()) {
      if (!IsKeyOwned(write.key())) {
        continue;
      }

      auto committedReadsItr = committedReads.find(write.key());

      if (committedReadsItr != committedReads.end()){
         std::shared_lock lock(committedReadsItr->second.first);
         if(committedReadsItr->second.second.size() > 0) {
          for (auto ritr = committedReadsItr->second.second.rbegin();
              ritr != committedReadsItr->second.second.rend(); ++ritr) {
            if (ts >= std::get<0>(*ritr)) {
              // iterating over committed reads from largest to smallest committed txn ts
              //    if ts is larger than current itr, it is also larger than all subsequent itrs
              break;
            } else if (std::get<1>(*ritr) < ts) {
              if (params.validateProofs) {
                conflict = std::get<2>(*ritr);
              }
              Debug("[%lu:%lu][%s] ABORT rw conflict committed read for key %s: committed"
                  " read ts %lu.%lu < this txn's ts %lu.%lu < committed ts %lu.%lu.",
                  txn.client_id(),
                  txn.client_seq_num(),
                  BytesToHex(txnDigest, 16).c_str(),
                  BytesToHex(write.key(), 16).c_str(),
                  std::get<1>(*ritr).getTimestamp(),
                  std::get<1>(*ritr).getID(), ts.getTimestamp(),
                  ts.getID(), std::get<0>(*ritr).getTimestamp(),
                  std::get<0>(*ritr).getID());
              stats.Increment("cc_aborts", 1);
              stats.Increment("cc_aborts_rw_conflict", 1);
              return proto::ConcurrencyControl::ABORT;
            }
          }
        }
      }

      const auto preparedReadsItr = preparedReads.find(write.key());
      if (preparedReadsItr != preparedReads.end()) {

        std::shared_lock lock(preparedReadsItr->second.first);

        for (const auto preparedReadTxn : preparedReadsItr->second.second) {
          bool isDep = false;
          for (const auto &dep : preparedReadTxn->deps()) {
            if (txnDigest == dep.write().prepared_txn_digest()) {
              isDep = true;
              break;
            }
          }

          bool isReadVersionEarlier = false;
          Timestamp readTs;
          for (const auto &read : preparedReadTxn->read_set()) {
            if (read.key() == write.key()) {
              readTs = Timestamp(read.readtime());
              isReadVersionEarlier = readTs < ts;
              break;
            }
          }
          if (!isDep && isReadVersionEarlier &&
              ts < Timestamp(preparedReadTxn->timestamp())) {
            Debug("[%lu:%lu][%s] ABSTAIN rw conflict prepared read for key %s: prepared"
                " read ts %lu.%lu < this txn's ts %lu.%lu < committed ts %lu.%lu.",
                txn.client_id(),
                txn.client_seq_num(),
                BytesToHex(txnDigest, 16).c_str(),
                BytesToHex(write.key(), 16).c_str(),
                readTs.getTimestamp(),
                readTs.getID(), ts.getTimestamp(),
                ts.getID(), preparedReadTxn->timestamp().timestamp(),
                preparedReadTxn->timestamp().id());
            stats.Increment("cc_abstains", 1);
            stats.Increment("cc_abstains_rw_conflict", 1);
            return proto::ConcurrencyControl::ABSTAIN;
          }
        }
      }

       //RECOMMENT XXX Set version of RTS implementation; currently using single replacing RTS
      //  //Latency_Start(&waitingOnLocks);
      //  if(params.mainThreadDispatching) rtsMutex.lock_shared();
      //  //Latency_End(&waitingOnLocks);
      // auto rtsItr = rts.find(write.key());
      // if (rtsItr != rts.end()) {
      //   auto rtsRBegin = rtsItr->second.rbegin();
      //   if (rtsRBegin != rtsItr->second.rend()) {
      //     Debug("Largest rts for write to key %s: %lu.%lu.",
      //       BytesToHex(write.key(), 16).c_str(), rtsRBegin->getTimestamp(),
      //       rtsRBegin->getID());
      //   }
      //   auto rtsLB = rtsItr->second.lower_bound(ts);
      //   if (rtsLB != rtsItr->second.end()) {
      //     Debug("Lower bound rts for write to key %s: %lu.%lu.",
      //       BytesToHex(write.key(), 16).c_str(), rtsLB->getTimestamp(),
      //       rtsLB->getID());
      //     if (*rtsLB == ts) {
      //       rtsLB++;
      //     }
      //     if (rtsLB != rtsItr->second.end()) {
      //       if (*rtsLB > ts) {
      //         Debug("[%lu:%lu][%s] ABSTAIN larger rts acquired for key %s: rts %lu.%lu >"
      //             " this txn's ts %lu.%lu.",
      //             txn.client_id(),
      //             txn.client_seq_num(),
      //             BytesToHex(txnDigest, 16).c_str(),
      //             BytesToHex(write.key(), 16).c_str(),
      //             rtsLB->getTimestamp(),
      //             rtsLB->getID(), ts.getTimestamp(), ts.getID());
      //         stats.Increment("cc_abstains", 1);
      //         stats.Increment("cc_abstains_rts", 1);
      //          if(params.mainThreadDispatching) rtsMutex.unlock_shared();
      //         return proto::ConcurrencyControl::ABSTAIN;
      //       }
      //     }
      //   }
      // }
      //  if(params.mainThreadDispatching) rtsMutex.unlock_shared();
      auto rtsItr = rts.find(write.key());
      if(rtsItr != rts.end()){
        if(rtsItr->second > ts.getTimestamp()){
          ///TODO XXX Re-introduce ID also, for finer ordering. This is safe, since the
          //RTS check is just an additional heuristic; The prepare/commit checks guarantee serializability on their own
          stats.Increment("cc_abstains", 1);
          stats.Increment("cc_abstains_rts", 1);
          return proto::ConcurrencyControl::ABSTAIN;
        }
      }

      // TODO: add additional rts dep check to shrink abort window  (aka Exceptions)
      //    Is this still a thing?  -->> No currently not
    }

    if (params.validateProofs && params.signedMessages && !params.verifyDeps) {

      Debug("Exec MessageToSign by CPU: %d", sched_getcpu());
      for (const auto &dep : txn.deps()) {
        if (dep.involved_group() != groupIdx) {
          continue;
        }
        if (committed.find(dep.write().prepared_txn_digest()) == committed.end() &&
            aborted.find(dep.write().prepared_txn_digest()) == aborted.end()) {
          //check whether we (i.e. the server) have prepared it ourselves: This alleviates having to verify dep proofs

          preparedMap::const_accessor a2;
          auto preparedItr = prepared.find(a2, dep.write().prepared_txn_digest());
          if(!preparedItr){
          //if (preparedItr == prepared.end()) {
            return proto::ConcurrencyControl::ABSTAIN;
          }
          a2.release();
        }
      }
    }
    Prepare(txnDigest, txn);
  }
  else{
     a.release();
  }

  bool allFinished = true;

  if(params.maxDepDepth > -2){

     //if(params.mainThreadDispatching) dependentsMutex.lock();
     if(params.mainThreadDispatching) waitingDependenciesMutex.lock();

    for (const auto &dep : txn.deps()) {
      if (dep.involved_group() != groupIdx) {
        continue;
      }

      // tbb::concurrent_hash_map<std::string, std::mutex>::const_accessor z;
      // bool currently_completing = completing.find(z, dep.write().prepared_txn_digest());
      // if(currently_completing) //z->second.lock();

      if (committed.find(dep.write().prepared_txn_digest()) == committed.end() &&
          aborted.find(dep.write().prepared_txn_digest()) == aborted.end()) {
        Debug("[%lu:%lu][%s] WAIT for dependency %s to finish.",
            txn.client_id(), txn.client_seq_num(),
            BytesToHex(txnDigest, 16).c_str(),
            BytesToHex(dep.write().prepared_txn_digest(), 16).c_str());

        //XXX start RelayP1 to initiate Fallback handling
        //TODO can remove this redundant lookup since it will be checked again...
        ongoingMap::const_accessor b;
        bool inOngoing = ongoing.find(b, dep.write().prepared_txn_digest());
        if (inOngoing && !fallback_flow && false) {
          std::string txnDig = dep.write().prepared_txn_digest();
          //schedule Relay for client timeout only..
          TransportAddress *remoteCopy = remote.clone();
          transport->Timer((CLIENTTIMEOUT), [this, remoteCopy, txnDig, reqId](){
            this->RelayP1(*remoteCopy, txnDig, reqId);
            delete remoteCopy;
          });
          //proto::Transaction *tx = b->second; //ongoing[txnDig];
          //RelayP1(remote, *tx, reqId);
        }
        b.release();

        allFinished = false;
        //dependents[dep.write().prepared_txn_digest()].insert(txnDigest);

        // auto dependenciesItr = waitingDependencies.find(txnDigest);
        // if (dependenciesItr == waitingDependencies.end()) {
        //   auto inserted = waitingDependencies.insert(std::make_pair(txnDigest,
        //         WaitingDependency()));
        //   UW_ASSERT(inserted.second);
        //   dependenciesItr = inserted.first;
        // }
        // dependenciesItr->second.reqId = reqId;
        // dependenciesItr->second.remote = remote.clone();  //&remote;
        // dependenciesItr->second.deps.insert(dep.write().prepared_txn_digest());

        dependentsMap::accessor e;
        dependents.insert(e, dep.write().prepared_txn_digest());
        e->second.insert(txnDigest);
        e.release();

        waitingDependenciesMap::accessor f;
        bool dependenciesItr = waitingDependencies_new.find(f, txnDigest);
        if (!dependenciesItr) {
          waitingDependencies_new.insert(f, txnDigest);
          //f->second = WaitingDependency();
        }
        if(!fallback_flow){
          f->second.original_client = true;
          f->second.reqId = reqId;
          f->second.remote = remote.clone();  //&remote;
        }
        f->second.deps.insert(dep.write().prepared_txn_digest());
        f.release();
      }
     // if(currently_completing) //z->second.unlock();
     // z.release();
    }

     //if(params.mainThreadDispatching) dependentsMutex.unlock();
     if(params.mainThreadDispatching) waitingDependenciesMutex.unlock();

 }

  if (!allFinished) {
    stats.Increment("cc_waits", 1);
    return proto::ConcurrencyControl::WAIT;
  } else {
    return CheckDependencies(txn);
  }
}

void Server::GetPreparedReadTimestamps(
    std::unordered_map<std::string, std::set<Timestamp>> &reads) {
  // gather up the set of all writes that are currently prepared
   if(params.mainThreadDispatching) preparedMutex.lock_shared();
  for (const auto &t : prepared) {
    for (const auto &read : t.second.second->read_set()) {
      if (IsKeyOwned(read.key())) {
        reads[read.key()].insert(t.second.first);
      }
    }
  }
   if(params.mainThreadDispatching) preparedMutex.unlock_shared();
}

void Server::GetPreparedReads(
    std::unordered_map<std::string, std::vector<const proto::Transaction*>> &reads) {
  // gather up the set of all writes that are currently prepared
   if(params.mainThreadDispatching) preparedMutex.lock_shared();
  for (const auto &t : prepared) {
    for (const auto &read : t.second.second->read_set()) {
      if (IsKeyOwned(read.key())) {
        reads[read.key()].push_back(t.second.second);
      }
    }
  }
   if(params.mainThreadDispatching) preparedMutex.unlock_shared();
}

void Server::Prepare(const std::string &txnDigest,
    const proto::Transaction &txn) {
  Debug("PREPARE[%s] agreed to commit with ts %lu.%lu.",
      BytesToHex(txnDigest, 16).c_str(), txn.timestamp().timestamp(), txn.timestamp().id());


  ongoingMap::const_accessor b;
  auto ongoingItr = ongoing.find(b, txnDigest);
  if(!ongoingItr){
  //if(ongoingItr == ongoing.end()){
    Debug("Already concurrently Committed/Aborted txn[%s]", BytesToHex(txnDigest, 16).c_str());
    return;
  }
  const proto::Transaction *ongoingTxn = b->second;
  //const proto::Transaction *ongoingTxn = ongoing.at(txnDigest);

  preparedMap::accessor a;
  auto p = prepared.insert(a, std::make_pair(txnDigest, std::make_pair(
          Timestamp(txn.timestamp()), ongoingTxn)));

  for (const auto &read : txn.read_set()) {
    if (IsKeyOwned(read.key())) {
      //preparedReads[read.key()].insert(p.first->second.second);
      //preparedReads[read.key()].insert(a->second.second);

      std::pair<std::shared_mutex, std::set<const proto::Transaction *>> &y = preparedReads[read.key()];
      std::unique_lock lock(y.first);
      y.second.insert(a->second.second);
    }
  }

  std::pair<Timestamp, const proto::Transaction *> pWrite =
    std::make_pair(a->second.first, a->second.second);
  a.release();
    //std::make_pair(p.first->second.first, p.first->second.second);
  for (const auto &write : txn.write_set()) {
    if (IsKeyOwned(write.key())) {
      std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[write.key()];
      std::unique_lock lock(x.first);
      x.second.insert(pWrite);
      // std::unique_lock lock(preparedWrites[write.key()].first);
      // preparedWrites[write.key()].second.insert(pWrite);
    }
  }
  b.release(); //Relase only at the end, so that Prepare and Clean in parallel for the same TX are atomic.
}

void Server::GetCommittedWrites(const std::string &key, const Timestamp &ts,
    std::vector<std::pair<Timestamp, Server::Value>> &writes) {

  std::vector<std::pair<Timestamp, Server::Value>> values;
  if (store.getCommittedAfter(key, ts, values)) {
    for (const auto &p : values) {
      writes.push_back(p);
    }
  }
}

void Server::Commit(const std::string &txnDigest, proto::Transaction *txn,
      proto::GroupedSignatures *groupedSigs, bool p1Sigs, uint64_t view) {

  Timestamp ts(txn->timestamp());

  Value val;
  proto::CommittedProof *proof = nullptr;
  if (params.validateProofs) {
    Debug("Access only by CPU: %d", sched_getcpu());
    proof = new proto::CommittedProof();
    //proof = testing_committed_proof.back();
    //testing_committed_proof.pop_back();
  }
  val.proof = proof;

  auto committedItr = committed.insert(std::make_pair(txnDigest, proof));
  //auto committedItr =committed.emplace(txnDigest, proof);

  if (params.validateProofs) {
    // CAUTION: we no longer own txn pointer (which we allocated during Phase1
    //    and stored in ongoing)
    proof->set_allocated_txn(txn);
    if (params.signedMessages) {
      if (p1Sigs) {
        proof->set_allocated_p1_sigs(groupedSigs);
      } else {
        proof->set_allocated_p2_sigs(groupedSigs);
        proof->set_p2_view(view);
        //view = groupedSigs.begin()->second.sigs().begin()
      }
    }
  }

  for (const auto &read : txn->read_set()) {
    if (!IsKeyOwned(read.key())) {
      continue;
    }


    //store.commitGet(read.key(), read.readtime(), ts);   //SEEMINGLY NEVER USED XXX
    //commitGet_count++;

    //Latency_Start(&committedReadInsertLat);

     std::pair<std::shared_mutex, std::set<committedRead>> &z = committedReads[read.key()];
     std::unique_lock lock(z.first);
     z.second.insert(std::make_tuple(ts, read.readtime(),
           committedItr.first->second));
    // committedReads[read.key()].insert(std::make_tuple(ts, read.readtime(),
    //       committedItr.first->second));

    //uint64_t ns = Latency_End(&committedReadInsertLat);
    //stats.Add("committed_read_insert_lat_" + BytesToHex(read.key(), 18), ns);
  }


  for (const auto &write : txn->write_set()) {
    if (!IsKeyOwned(write.key())) {
      continue;
    }

    Debug("COMMIT[%lu,%lu] Committing write for key %s.",
        txn->client_id(), txn->client_seq_num(),
        BytesToHex(write.key(), 16).c_str());
    val.val = write.value();

    store.put(write.key(), val, ts);



     //RECOMMENT XXX RTS version with set of timestamps
    //  //Latency_Start(&waitingOnLocks);
    //  if(params.mainThreadDispatching) rtsMutex.lock();
    //  //Latency_End(&waitingOnLocks);
    // auto rtsItr = rts.find(write.key());
    // if (rtsItr != rts.end()) {
    //   auto itr = rtsItr->second.begin();
    //   auto endItr = rtsItr->second.upper_bound(ts);
    //   while (itr != endItr) {
    //     itr = rtsItr->second.erase(itr);
    //   }
    // }
    //  if(params.mainThreadDispatching) rtsMutex.unlock();
  }

  Clean(txnDigest);
  CheckDependents(txnDigest);
  CleanDependencies(txnDigest);
}

void Server::Abort(const std::string &txnDigest) {
   //if(params.mainThreadDispatching) abortedMutex.lock();
  aborted.insert(txnDigest);
   //if(params.mainThreadDispatching) abortedMutex.unlock();
  Clean(txnDigest);
  CheckDependents(txnDigest);
  CleanDependencies(txnDigest);
}

void Server::Clean(const std::string &txnDigest) {

  //Latency_Start(&waitingOnLocks);
  //auto ongoingMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(ongoingMutex) : std::unique_lock<std::shared_mutex>();
  //auto preparedMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(preparedMutex) : std::unique_lock<std::shared_mutex>();
  //auto preparedReadsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(preparedReadsMutex) : std::unique_lock<std::shared_mutex>();
  //auto preparedWritesMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(preparedWritesMutex) : std::unique_lock<std::shared_mutex>();

  //auto p1ConflictsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(p1ConflictsMutex) : std::unique_lock<std::mutex>();
  //p1Decisions..
  auto p2DecisionsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(p2DecisionsMutex) : std::unique_lock<std::mutex>();
  //auto interestedClientsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(interestedClientsMutex) : std::unique_lock<std::mutex>();
  auto current_viewsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(current_viewsMutex) : std::unique_lock<std::mutex>();
  auto decision_viewsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(decision_viewsMutex) : std::unique_lock<std::mutex>();
  auto ElectQuorumMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(ElectQuorumMutex) : std::unique_lock<std::mutex>();
  //Latency_End(&waitingOnLocks);

  ongoingMap::accessor b;
  if(ongoing.find(b, txnDigest)){
      ongoing.erase(b);
  }
  //ongoing.erase(txnDigest);

  preparedMap::accessor a;
  auto preparedItr = prepared.find(a, txnDigest);
  if(preparedItr){
  //if (itr != prepared.end()) {
    for (const auto &read : a->second.second->read_set()) {
    //for (const auto &read : itr->second.second->read_set()) {
      if (IsKeyOwned(read.key())) {
        //preparedReads[read.key()].erase(a->second.second);
        //preparedReads[read.key()].erase(itr->second.second);
        std::pair<std::shared_mutex, std::set<const proto::Transaction *>> &y = preparedReads[read.key()];
        std::unique_lock lock(y.first);
        y.second.erase(a->second.second);
      }
    }
    for (const auto &write : a->second.second->write_set()) {
    //for (const auto &write : itr->second.second->write_set()) {
      if (IsKeyOwned(write.key())) {
        //preparedWrites[write.key()].erase(itr->second.first);
        std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[write.key()];
        std::unique_lock lock(x.first);
        x.second.erase(a->second.first);
        //x.second.erase(itr->second.first);
      }
    }
    prepared.erase(a);
  }
  a.release();
  b.release(); //Release only at the end, so that Prepare and Clean in parallel for the same TX are atomic.
  //TODO: might want to move it all the way to the end.

  //XXX: Fallback related cleans

  //interestedClients[txnDigest].insert(remote.clone());
  interestedClientsMap::accessor i;
  auto jtr = interestedClients.find(i, txnDigest);
  if(jtr){
  //if (jtr != interestedClients.end()) {
    for (const auto addr : i->second) {
      delete addr;
    }
    interestedClients.erase(i);
  }
  i.release();

  current_views.erase(txnDigest);
  decision_views.erase(txnDigest);
  auto ktr = ElectQuorum.find(txnDigest);
  if (ktr != ElectQuorum.end()) {
    for (const auto signed_m : ktr->second) {
      delete signed_m;
    }
    ElectQuorum.erase(ktr);
    ElectQuorum_meta.erase(txnDigest);
  }
  p1DecisionsMap::accessor c;
  auto p1DecItr = p1Decisions.find(c, txnDigest);
  //p1Decisions.erase(txnDigest);
  if(p1DecItr) p1Decisions.erase(c);
  c.release();

  p1ConflictsMap::accessor d;
  auto p1ConfItr = p1Conflicts.find(d, txnDigest);
  //p1Conflicts.erase(txnDigest);
  if(p1ConfItr) p1Conflicts.erase(d);
  d.release();

  p2Decisions.erase(txnDigest);
  //TODO: erase all timers if we end up using them again

  // tbb::concurrent_hash_map<std::string, std::mutex>::accessor z;
  // if(completing.find(z, txnDigest)){
  //   //z->second.unlock();
  //   completing.erase(z);
  // }
  // z.release();
}

void Server::CheckDependents(const std::string &txnDigest) {
  //Latency_Start(&waitingOnLocks);
   //if(params.mainThreadDispatching) dependentsMutex.lock(); //read lock
   if(params.mainThreadDispatching) waitingDependenciesMutex.lock();
  //Latency_End(&waitingOnLocks);

  dependentsMap::const_accessor e;
  bool dependentsItr = dependents.find(e, txnDigest);
  //auto dependentsItr = dependents.find(txnDigest);
  if(dependentsItr){
  //if (dependentsItr != dependents.end()) {
    for (const auto &dependent : e->second) {
    //for (const auto &dependent : dependentsItr->second) {

      waitingDependenciesMap::accessor f;
      bool dependenciesItr = waitingDependencies_new.find(f, dependent);
      //if(!dependenciesItr){   std::cerr << "waitingdeps empty" << std::endl; e.release(); return;}
      UW_ASSERT(dependenciesItr);  //technically this should never fail, since if it were not
      // in the waitingDep struct anymore, it wouldve also removed itself from the
      //dependents set of txnDigest. XXX Need to reason carefully whether this is still true
      // with parallel OCC --> or rather parallel Commit (this is only affected by parallel commit)

      f->second.deps.erase(txnDigest);
      if (f->second.deps.size() == 0) {
        Debug("Dependencies of %s have all committed or aborted.",
            BytesToHex(dependent, 16).c_str());
        proto::ConcurrencyControl::Result result = CheckDependencies(
            dependent);
        UW_ASSERT(result != proto::ConcurrencyControl::ABORT);
        Debug("print remote: %p", f->second.remote);
        //waitingDependencies.erase(dependent);
        const proto::CommittedProof *conflict = nullptr;
        if(f->second.original_client){
          SendPhase1Reply(f->second.reqId, result, conflict, dependent,
              f->second.remote);
          delete f->second.remote;
        }
        //Send it to all interested FB clients too:

          interestedClientsMap::accessor i;
          auto jtr = interestedClients.find(i, txnDigest);
          if(jtr){
            if(ForwardWritebackMulti(txnDigest, i)){i.release(); return;}
            i.release();
            P1FBorganizer *p1fb_organizer = new P1FBorganizer(0, txnDigest, this);
            SetP1(0, p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, conflict);
            //TODO: If need reqId, can store it as pairs with the interested client.
            SendPhase1FBReply(p1fb_organizer, txnDigest, true);

          }
          else{
            i.release();
          }


        /////

        waitingDependencies_new.erase(f);
      }
      f.release();
    }
  }
  e.release();
   //if(params.mainThreadDispatching) dependentsMutex.unlock();
   if(params.mainThreadDispatching) waitingDependenciesMutex.unlock();
}

proto::ConcurrencyControl::Result Server::CheckDependencies(
    const std::string &txnDigest) {
      //Latency_Start(&waitingOnLocks);
   //if(params.mainThreadDispatching) ongoingMutex.lock_shared();
   //Latency_End(&waitingOnLocks);
  ongoingMap::const_accessor b;
  auto txnItr = ongoing.find(b, txnDigest);
  if(!txnItr){

  //if(txnItr == ongoing.end()){
    Debug("Tx with txn digest [%s] has already committed/aborted", BytesToHex(txnDigest, 16).c_str());
    {
      //std::shared_lock lock(committedMutex);
      //std::shared_lock lock2(abortedMutex);
      if(committed.find(txnDigest) != committed.end()){
        //if(params.mainThreadDispatching) ongoingMutex.unlock_shared();
        return proto::ConcurrencyControl::COMMIT;
      }
      else if(aborted.find(txnDigest) != aborted.end()){
        //if(params.mainThreadDispatching) ongoingMutex.unlock_shared();
        return proto::ConcurrencyControl::ABSTAIN;
      }
      else{
        Panic("has to be either committed or aborted");
      }
    }
  }

  //UW_ASSERT(txnItr != ongoing.end());
   //if(params.mainThreadDispatching) ongoingMutex.unlock_shared();
  //return CheckDependencies(*txnItr->second);
  return CheckDependencies(*b->second);
}

proto::ConcurrencyControl::Result Server::CheckDependencies(
    const proto::Transaction &txn) {

   //if(params.mainThreadDispatching) committedMutex.lock_shared();
  for (const auto &dep : txn.deps()) {
    if (dep.involved_group() != groupIdx) {
      continue;
    }
    if (committed.find(dep.write().prepared_txn_digest()) != committed.end()) {
      if (Timestamp(dep.write().prepared_timestamp()) > Timestamp(txn.timestamp())) {
        stats.Increment("cc_aborts", 1);
        stats.Increment("cc_aborts_dep_ts", 1);
         //if(params.mainThreadDispatching) committedMutex.unlock_shared();
        return proto::ConcurrencyControl::ABSTAIN;
      }
    } else {
      stats.Increment("cc_aborts", 1);
      stats.Increment("cc_aborts_dep_aborted", 1);
       //if(params.mainThreadDispatching) committedMutex.unlock_shared();
      return proto::ConcurrencyControl::ABSTAIN;
    }
  }
   //if(params.mainThreadDispatching) committedMutex.unlock_shared();
  return proto::ConcurrencyControl::COMMIT;
}

bool Server::CheckHighWatermark(const Timestamp &ts) {
  Timestamp highWatermark(timeServer.GetTime());
  // add delta to current local time
  highWatermark.setTimestamp(highWatermark.getTimestamp() + timeDelta);
  Debug("High watermark: %lu.", highWatermark.getTimestamp());
  return ts > highWatermark;
}

void Server::SendPhase1Reply(uint64_t reqId,
    proto::ConcurrencyControl::Result result,
    const proto::CommittedProof *conflict, const std::string &txnDigest,
    const TransportAddress *remote) {

   p1DecisionsMap::accessor c;
   p1Decisions.insert(c, std::make_pair(txnDigest, result));
   c.release();
 //  p1Decisions[txnDigest] = result;

  //add abort proof so we can use it for fallbacks easily.
  if(result == proto::ConcurrencyControl::ABORT){
     p1ConflictsMap::accessor d;
     p1Conflicts.insert(d, std::make_pair(txnDigest, conflict));
    //p1Conflicts[txnDigest] = conflict;  //does this work this way for CbR
    d.release();
  }

  proto::Phase1Reply* phase1Reply = GetUnusedPhase1Reply();
  phase1Reply->set_req_id(reqId);
  TransportAddress *remoteCopy = remote->clone();

  auto sendCB = [remoteCopy, this, phase1Reply]() {
    this->transport->SendMessage(this, *remoteCopy, *phase1Reply);
    FreePhase1Reply(phase1Reply);
    delete remoteCopy;
  };

  phase1Reply->mutable_cc()->set_ccr(result);
  if (params.validateProofs) {
    *phase1Reply->mutable_cc()->mutable_txn_digest() = txnDigest;
    phase1Reply->mutable_cc()->set_involved_group(groupIdx);
    if (result == proto::ConcurrencyControl::ABORT) {
      *phase1Reply->mutable_cc()->mutable_committed_conflict() = *conflict;
    } else if (params.signedMessages) {
      proto::ConcurrencyControl* cc = new proto::ConcurrencyControl(phase1Reply->cc());
      //Latency_Start(&signLat);
      Debug("PHASE1[%s] Batching Phase1Reply.",
            BytesToHex(txnDigest, 16).c_str());

      MessageToSign(cc, phase1Reply->mutable_signed_cc(),
        [sendCB, cc, txnDigest, this, phase1Reply]() {
          Debug("PHASE1[%s] Sending Phase1Reply with signature %s from priv key %lu.",
            BytesToHex(txnDigest, 16).c_str(),
            BytesToHex(phase1Reply->signed_cc().signature(), 100).c_str(),
            phase1Reply->signed_cc().process_id());

          sendCB();
          delete cc;
        });
      //Latency_End(&signLat);
      return;
    }
  }
  sendCB();
}

void Server::CleanDependencies(const std::string &txnDigest) {
   //if(params.mainThreadDispatching) dependentsMutex.lock();
   if(params.mainThreadDispatching) waitingDependenciesMutex.lock();

  waitingDependenciesMap::accessor f;
  bool dependenciesItr = waitingDependencies_new.find(f, txnDigest);
  if (dependenciesItr ) {
  //auto dependenciesItr = waitingDependencies.find(txnDigest);
  //if (dependenciesItr != waitingDependencies.end()) {
    //for (const auto &dependency : dependenciesItr->second.deps) {
    for (const auto &dependency : f->second.deps) {
      //std::cerr << "ABORTING AT 3" << std::endl;
      dependentsMap::accessor e;
      auto dependentItr = dependents.find(e, dependency);
      if (dependentItr) {
        e->second.erase(txnDigest);
      }
      e.release();
      // auto dependentItr = dependents.find(dependency);
      // if (dependentItr != dependents.end()) {
      //   dependentItr->second.erase(txnDigest);
      // }

    }
    waitingDependencies_new.erase(f);
    //waitingDependencies.erase(dependenciesItr);
  }
  f.release();
  dependentsMap::accessor e;
  if(dependents.find(e, txnDigest)){
    dependents.erase(e);
  }
  e.release();
  //dependents.erase(txnDigest);
   //if(params.mainThreadDispatching) dependentsMutex.unlock();
   if(params.mainThreadDispatching) waitingDependenciesMutex.unlock();
}

void Server::LookupP1Decision(const std::string &txnDigest, int64_t &myProcessId,
    proto::ConcurrencyControl::Result &myResult) const {
  myProcessId = -1;
  // see if we participated in this decision
   //if(params.mainThreadDispatching) p1DecisionsMutex.lock();
   p1DecisionsMap::const_accessor c;

  auto p1DecisionItr = p1Decisions.find(c, txnDigest);
  if(p1DecisionItr){
  //if (p1DecisionItr != p1Decisions.end()) {
    myProcessId = id;
    myResult = c->second;
  }
  c.release();
   //if(params.mainThreadDispatching) p1DecisionsMutex.unlock();
}

void Server::LookupP2Decision(const std::string &txnDigest, int64_t &myProcessId,
    proto::CommitDecision &myDecision) const {
  myProcessId = -1;
  // see if we participated in this decision
   if(params.mainThreadDispatching) p2DecisionsMutex.lock();
  auto p2DecisionItr = p2Decisions.find(txnDigest);
  if (p2DecisionItr != p2Decisions.end()) {
    myProcessId = id;
    myDecision = p2DecisionItr->second;
  }
   if(params.mainThreadDispatching) p2DecisionsMutex.unlock();
}

void Server::LookupCurrentView(const std::string &txnDigest, int64_t &myProcessId,
    uint64_t &myCurrentView) const {
  myProcessId = -1;
  // see if we participated in this decision
   if(params.mainThreadDispatching) current_viewsMutex.lock();
  auto current_viewsItr = current_views.find(txnDigest);
  if (current_viewsItr != current_views.end()) {
    myProcessId = id;
    myDecision = current_viewsItr->second;
  }
   if(params.mainThreadDispatching) current_viewsMutex.unlock();
}

uint64_t Server::DependencyDepth(const proto::Transaction *txn) const {
  uint64_t maxDepth = 0;
  std::queue<std::pair<const proto::Transaction *, uint64_t>> q;
  q.push(std::make_pair(txn, 0UL));

  //auto ongoingMutexScope = params.mainThreadDispatching ? std::shared_lock<std::shared_mutex>(ongoingMutex) : std::shared_lock<std::shared_mutex>();

  while (!q.empty()) {
    std::pair<const proto::Transaction *, uint64_t> curr = q.front();
    q.pop();
    maxDepth = std::max(maxDepth, curr.second);
    for (const auto &dep : curr.first->deps()) {
      ongoingMap::const_accessor b;
      auto oitr = ongoing.find(b, dep.write().prepared_txn_digest());
      if(oitr){
      //if (oitr != ongoing.end()) {
        //q.push(std::make_pair(oitr->second, curr.second + 1));
        q.push(std::make_pair(b->second, curr.second + 1));
      }
      b.release();
    }
  }
  return maxDepth;
}

void Server::MessageToSign(::google::protobuf::Message* msg,
      proto::SignedMessage *signedMessage, signedCallback cb) {


  ////auto lockScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(mainThreadMutex) : std::unique_lock<std::mutex>();
  Debug("Exec MessageToSign by CPU: %d", sched_getcpu());

  if(params.multiThreading){
      if (params.signatureBatchSize == 1) {

          Debug("(multithreading) dispatching signing");
          auto f = [this, msg, signedMessage, cb = std::move(cb)](){
            SignMessage(msg, keyManager->GetPrivateKey(id), id, signedMessage);
            cb();
            return (void*) true;
          };
          transport->DispatchTP_noCB(std::move(f));
      }

      else {
        Debug("(multithreading) adding sig request to localbatchSigner");

        batchSigner->asyncMessageToSign(msg, signedMessage, std::move(cb));
      }
  }
  else{
    if (params.signatureBatchSize == 1) {

      SignMessage(msg, keyManager->GetPrivateKey(id), id, signedMessage);
      cb();
      //if multithread: Dispatch f: SignMessage and cb.
    } else {
      batchSigner->MessageToSign(msg, signedMessage, std::move(cb));
    }
  }
}

//TODO: replace all of these with moodycamel queue (just check that try_dequeue is successful)
proto::ReadReply *Server::GetUnusedReadReply() {
  return new proto::ReadReply();

  // std::unique_lock<std::mutex> lock(readReplyProtoMutex);
  // proto::ReadReply *reply;
  // if (readReplies.size() > 0) {
  //   reply = readReplies.back();
  //   reply->Clear();
  //   readReplies.pop_back();
  // } else {
  //   reply = new proto::ReadReply();
  // }
  // return reply;
}

proto::Phase1Reply *Server::GetUnusedPhase1Reply() {
  return new proto::Phase1Reply();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p1ReplyProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase1Reply *reply;
  // if (p1Replies.size() > 0) {
  //   reply = p1Replies.back();
  //   //reply->Clear(); //can move this to Free if want more work at threads
  //   p1Replies.pop_back();
  // } else {
  //   reply = new proto::Phase1Reply();
  // }
  // return reply;
}

proto::Phase2Reply *Server::GetUnusedPhase2Reply() {
  return new proto::Phase2Reply();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p2ReplyProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase2Reply *reply;
  // if (p2Replies.size() > 0) {
  //   reply = p2Replies.back();
  //   //reply->Clear();
  //   p2Replies.pop_back();
  // } else {
  //   reply = new proto::Phase2Reply();
  // }
  // return reply;
}

proto::Read *Server::GetUnusedReadmessage() {
  return new proto::Read();

  // std::unique_lock<std::mutex> lock(readProtoMutex);
  // proto::Read *msg;
  // if (readMessages.size() > 0) {
  //   msg = readMessages.back();
  //   msg->Clear();
  //   readMessages.pop_back();
  // } else {
  //   msg = new proto::Read();
  // }
  // return msg;
}

proto::Phase1 *Server::GetUnusedPhase1message() {
  return new proto::Phase1();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p1ProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase1 *msg;
  // if (p1messages.size() > 0) {
  //   msg = p1messages.back();
  //   msg->Clear();
  //   p1messages.pop_back();
  // } else {
  //   msg = new proto::Phase1();
  // }
  // return msg;
}

proto::Phase2 *Server::GetUnusedPhase2message() {
  return new proto::Phase2();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p2ProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase2 *msg;
  // if (p2messages.size() > 0) {
  //   msg = p2messages.back();
  //   msg->Clear();
  //   p2messages.pop_back();
  // } else {
  //   msg = new proto::Phase2();
  // }
  // return msg;
}

proto::Writeback *Server::GetUnusedWBmessage() {
  return new proto::Writeback();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(WBProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Writeback *msg;
  // if (WBmessages.size() > 0) {
  //   msg = WBmessages.back();
  //   msg->Clear();
  //   WBmessages.pop_back();
  // } else {
  //   msg = new proto::Writeback();
  // }
  // return msg;
}

void Server::FreeReadReply(proto::ReadReply *reply) {
  delete reply;
  // std::unique_lock<std::mutex> lock(readReplyProtoMutex);
  // //reply->Clear();
  // readReplies.push_back(reply);
}

void Server::FreePhase1Reply(proto::Phase1Reply *reply) {
  delete reply;
  // std::unique_lock<std::mutex> lock(p1ReplyProtoMutex);
  //
  // reply->Clear();
  // p1Replies.push_back(reply);
}

void Server::FreePhase2Reply(proto::Phase2Reply *reply) {
  delete reply;
  // std::unique_lock<std::mutex> lock(p2ReplyProtoMutex);
  // reply->Clear();
  // p2Replies.push_back(reply);
}

void Server::FreeReadmessage(proto::Read *msg) {
  delete msg;
  // std::unique_lock<std::mutex> lock(readProtoMutex);
  // readMessages.push_back(msg);
}

void Server::FreePhase1message(proto::Phase1 *msg) {
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p1ProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // p1messages.push_back(msg);
}

void Server::FreePhase2message(proto::Phase2 *msg) {
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p2ProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // p2messages.push_back(msg);
}

void Server::FreeWBmessage(proto::Writeback *msg) {
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(WBProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // WBmessages.push_back(msg);
}

proto::Phase1FBReply *Server::GetUnusedPhase1FBReply(){
  return new proto::Phase1FBReply();
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(P1FBRProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase1FBReply *msg;
  // if (P1FBReplies.size() > 0) {
  //   msg = P1FBReplies.back();
  //   msg->Clear();
  //   P1FBReplies.pop_back();
  // } else {
  //   msg = new proto::proto::Phase1FBReply();
  // }
  // return msg;
}

void Server::FreePhase1FBReply(proto::Phase1FBReply *msg){
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(P1FBRProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // P1FBReplies.push_back(msg);
}

proto::Phase2FBReply *Server::GetUnusedPhase2FBReply(){
  return new proto::Phase2FBReply();
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(P2FBRProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase2FBReply *msg;
  // if (P2FBReplies.size() > 0) {
  //   msg = P2FBReplies.back();
  //   msg->Clear();
  //   P2FBReplies.pop_back();
  // } else {
  //   msg = new proto::proto::Phase2FBReply();
  // }
  // return msg;
}

void Server::FreePhase2FBReply(proto::Phase2FBReply *msg){
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(P2FBRProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // P2FBReplies.push_back(msg);
}

proto::Phase2FB *Server::GetUnusedPhase2FBmessage(){
  return new proto::Phase2FB();
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(P2FBProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase2FB *msg;
  // if (P2FBmessages.size() > 0) {
  //   msg = P2FBmessages.back();
  //   msg->Clear();
  //   P2FBmessages.pop_back();
  // } else {
  //   msg = new proto::proto::Phase2FB();
  // }
  // return msg;
}

void Server::FreePhase2FBmessage(proto::Phase2FB *msg){
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(P2FBProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // P2FBmessages.push_back(msg);
}




//XXX Simulated HMAC code
std::unordered_map<uint64_t, std::string> sessionKeys;
void CreateSessionKeys();
bool ValidateHMACedMessage(const proto::SignedMessage &signedMessage);
void CreateHMACedMessage(const ::google::protobuf::Message &msg, proto::SignedMessage& signedMessage);
// assume these are somehow secretly shared before hand

//TODO: If one wants to use Macs for Clients, need to add it to keymanager (in advance or dynamically based off id)
//can use client id to replica id (group * n + idx)
void Server::CreateSessionKeys(){
  for (uint64_t i = 0; i < config.n; i++) {
    if (i > idx) {
      sessionKeys[i] = std::string(8, (char) idx + 0x30) + std::string(8, (char) i + 0x30);
    } else {
      sessionKeys[i] = std::string(8, (char) i + 0x30) + std::string(8, (char) idx + 0x30);
    }
  }
}

// create MAC messages and verify them: Used for all to all leader election.
bool Server::ValidateHMACedMessage(const proto::SignedMessage &signedMessage) {

proto::HMACs hmacs;
hmacs.ParseFromString(signedMessage.signature());
return crypto::verifyHMAC(signedMessage.data(), (*hmacs.mutable_hmacs())[idx], sessionKeys[signedMessage.process_id() % config.n]);
}

void Server::CreateHMACedMessage(const ::google::protobuf::Message &msg, proto::SignedMessage& signedMessage) {

std::string msgData = msg.SerializeAsString();
signedMessage.set_data(msgData);
signedMessage.set_process_id(id);

proto::HMACs hmacs;
for (uint64_t i = 0; i < config.n; i++) {
  (*hmacs.mutable_hmacs())[i] = crypto::HMAC(msgData, sessionKeys[i]);
}
signedMessage.set_signature(hmacs.SerializeAsString());
}



////////////////////// XXX Fallback realm beings here...

//RELAY DEPENDENCY IN ORDER FOR CLIENT TO START FALLBACK
//params: reqID: client tx identifier for blocked tx; txnDigest = tx that is stalling
void Server::RelayP1(const TransportAddress &remote, const std::string &txnDigest, uint64_t conflict_id){

  Debug("RelayP1[%s] timed out. Sending now!", BytesToHex(txnDigest, 256).c_str());
  proto::Transaction *tx;

  ongoingMap::const_accessor b;
  bool ongoingItr = ongoing.find(b, txnDigest);
  if(!ongoingItr) return;  //If txnDigest no longer ongoing, then no FB necessary as it has completed already

  tx = b->second;
  b.release();
  proto::Phase1 p1;
  p1.set_req_id(0); //doesnt matter, its not used for fallback requests really.
  *p1.mutable_txn() = *tx;
  proto::RelayP1 relayP1;
  relayP1.set_conflict_id(conflict_id);
  *relayP1.mutable_p1() = p1;

  Debug("Sending RelayP1[%s].", BytesToHex(txnDigest, 256).c_str());

  transport->SendMessage(this, remote, relayP1);
}

bool Server::ForwardWriteback(const TransportAddress &remote, uint64_t ReqId, const std::string &txnDigest){
  //1) COMMIT CASE
  if(committed.find(txnDigest) != committed.end()){
      phase1FBReply.Clear();   //XXX CAUTION: USING GLOBAL OBJECT CURRENTLY
      phase1FBReply.set_req_id(ReqId);
      phase1FBReply.set_txn_digest(txnDigest);

      proto::Writeback *wb = phase1FBReply.mutable_wb();
      wb->Clear();
      wb->set_decision(proto::COMMIT);
      wb->set_txn_digest(txnDigest);
      proto::CommittedProof* proof = committed[txnDigest];

      if(proof->has_p1_sigs()){
        *wb->mutable_p1_sigs() = proof->p1_sigs();
      }
      else if(proof->has_p2_sigs()){
        *wb->mutable_p2_sigs() = proof->p2_sigs();
        wb->set_p2_view(proof->p2_view());
      }
      else{
        Panic("Commit proof has no signatures"); //error, should not happen
        // A Commit proof
        return false;
      }

      transport->SendMessage(this, remote, phase1FBReply);

      //TODO: delete interested client addres too, should there be an interested one. (or always use ForwardMulti.)
      // interestedClientsMap::accessor i;
      // bool interestedClientsItr = interestedClients.find(i, txnDigest);
      // i->second.erase(remote.clone());
      // i.release();
      // delete addr;
      // interestedClients.erase(i);
      // i.release();
      return true;
  }

  //2) ABORT CASE
  //currently for simplicity just forward writeback message that we received and stored.
  //writebackMessages only contains Abort copies. (when can one delete these?)
  //(A blockchain stores all request too, whether commit/abort)
  if(writebackMessages.find(txnDigest) != writebackMessages.end()){
      phase1FBReply.Clear();   //XXX CAUTION: USING GLOBAL OBJECT CURRENTLY
      phase1FBReply.set_req_id(ReqId);
      phase1FBReply.set_txn_digest(txnDigest);
      *phase1FBReply.mutable_wb() = writebackMessages[txnDigest];
      transport->SendMessage(this, remote, phase1FBReply);
      return true;
  }
}

bool Server::ForwardWritebackMulti(const std::string &txnDigest, interestedClientsMap::accessor &i){

  //interestedClientsMap::accessor i;
  //auto jtr = interestedClients.find(i, txnDigest);
  //if(!jtr) return true; //no interested clients, return

  if(committed.find(txnDigest) != committed.end()){
      phase1FBReply.Clear();   //XXX CAUTION: USING GLOBAL OBJECT CURRENTLY
      phase1FBReply.set_req_id(0);
      phase1FBReply.set_txn_digest(txnDigest);

      proto::Writeback *wb = phase1FBReply.mutable_wb();
      wb->Clear();
      wb->set_decision(proto::COMMIT);
      wb->set_txn_digest(txnDigest);
      proto::CommittedProof* proof = committed[txnDigest];

      if(proof->has_p1_sigs()){
        *wb->mutable_p1_sigs() = proof->p1_sigs();
      }
      else if(proof->has_p2_sigs()){
        *wb->mutable_p2_sigs() = proof->p2_sigs();
        wb->set_p2_view(proof->p2_view());
      }
      else{
        Panic("Commit proof has no signatures"); //error, should not happen
        // A Commit proof
        return false;
      }
  }
  //2) ABORT CASE
  else if(writebackMessages.find(txnDigest) != writebackMessages.end()){
      phase1FBReply.Clear();   //XXX CAUTION: USING GLOBAL OBJECT CURRENTLY
      phase1FBReply.set_req_id(0);
      phase1FBReply.set_txn_digest(txnDigest);
      *phase1FBReply.mutable_wb() = writebackMessages[txnDigest];
  }
  else{
    return false;
  }

  for (const auto addr : i->second) {
    transport->SendMessage(this, *addr, phase1FBReply);
    delete addr;
  }
  interestedClients.erase(i);
  i.release();
  return true;
}

//TODO: all requestID entries can be deleted.. currently unused for FB
//TODO: CAUTION: USING GLOBABL MESSAGE OBJECT CURRENTLY (must change)
//TODO:: CURRENTLY IF CASES ARE NOT ATOMIC (only matters if one intends to parallelize):
//For example, 1) case for committed could fail, but all consecutive fail too because it was committed inbetween.
//Could just put abort cases last; but makes for redundant work if it should occur inbetween.
void Server::HandlePhase1FB(const TransportAddress &remote, proto::Phase1FB &msg) {

  std::string txnDigest = TransactionDigest(msg.txn(), params.hashDigest);
  Debug("PHASE1FB[%lu:%lu][%s] with ts %lu.", msg.txn().client_id(),
      msg.txn().client_seq_num(), BytesToHex(txnDigest, 16).c_str(),
      msg.txn().timestamp().timestamp());

  //check if already committed. reply with whole proof so client can forward that.
  //1) COMMIT CASE, 2) ABORT CASE
  if(ForwardWriteback(remote, msg.req_id(), txnDigest)) return;

  //Otherwise, keep track of interested clients to message in the future
  interestedClientsMap::accessor i;
  bool interestedClientsItr = interestedClients.insert(i, txnDigest);
  i->second.insert(remote.clone());
  i.release();
  //interestedClients[txnDigest].insert(remote.clone());

  //3) BOTH P2 AND P1 CASE
  //might want to include the p1 too in order for there to exist a quorum for p1r (if not enough p2r). if you dont have a p1, then execute it yourself.
  //Alternatively, keep around the decision proof and send it. For now/simplicity, p2 suffices
  //TODO: could store p2 and p1 signatures (until writeback) in order to avoid re-computation
  p1DecisionsMap::const_accessor c;
  auto p1DecItr = p1Decisions.find(c, txnDigest);
  if(params.mainThreadDispatching) p2DecisionsMutex.lock();

  if(p2Decisions.end() != p2Decisions.find(txnDigest) && p1DecItr){
       proto::CommitDecision decision = p2Decisions[txnDigest];
       if(params.mainThreadDispatching) p2DecisionsMutex.unlock();

       proto::ConcurrencyControl::Result result = c->second; //p1Decisions[txnDigest];
       c.release();

       P1FBorganizer *p1fb_organizer = new P1FBorganizer(msg.req_id(), txnDigest, remote, this);
       //recover stored commit proof.
       if (result != proto::ConcurrencyControl::WAIT) { //if the result is WAIT, then the p1 is not necessary..
         const proto::CommittedProof *conflict;
         if(result == proto::ConcurrencyControl::ABORT){
            p1ConflictsMap::const_accessor d;
            auto p1ConflictsItr = p1Conflicts.find(d, txnDigest);
            conflict = d->second;
            d.release();
            //conflict = p1Conflicts[txnDigest];
         }
         SetP1(msg.req_id(), p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, conflict);
         //SendPhase1FBReply(msg.req_id(), phase1Reply, phase2Reply, writeback, remote,  txnDigest, 4);
       }
       SetP2(msg.req_id(), p1fb_organizer->p1fbr->mutable_p2r(), txnDigest, decision);
       SendPhase1FBReply(p1fb_organizer, txnDigest);
  }

  //4) ONLY P1 CASE: (already did p1 but no p2)
  else if(p1DecItr){
        if(params.mainThreadDispatching) p2DecisionsMutex.unlock();
        proto::ConcurrencyControl::Result result = c->second; //p1Decisions[txnDigest];
        c.release();

        if (result != proto::ConcurrencyControl::WAIT) {
          const proto::CommittedProof *conflict;
          //recover stored commit proof.
          if(result == proto::ConcurrencyControl::ABORT){
             p1ConflictsMap::const_accessor d;
             auto p1ConflictsItr = p1Conflicts.find(d, txnDigest);
             //conflict = p1Conflicts[txnDigest];
             conflict = d->second;
             d.release();
          }
          P1FBorganizer *p1fb_organizer = new P1FBorganizer(msg.req_id(), txnDigest, remote, this);
          SetP1(msg.req_id(), p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, conflict);
          SendPhase1FBReply(p1fb_organizer, txnDigest);
        }

  }
  //5) ONLY P2 CASE  (received p2, but was not part of p1)
  //TODO: if you dont have a p1, then execute it yourself. (see case 3) discussion)
  else if(p2Decisions.end() != p2Decisions.find(txnDigest)){
      c.release();
      proto::CommitDecision decision = p2Decisions[txnDigest];
      if(params.mainThreadDispatching) p2DecisionsMutex.unlock();

      P1FBorganizer *p1fb_organizer = new P1FBorganizer(msg.req_id(), txnDigest, remote, this);
      SetP2(msg.req_id(), p1fb_organizer->p1fbr->mutable_p2r(), txnDigest, decision);
      SendPhase1FBReply(p1fb_organizer, txnDigest);
  }

  //6) NO STATE STORED: Do p1 normally. copied logic from HandlePhase1(remote, msg)
  else{
      //TODO: ENTIRE CASE NEEDS TO BE ADJUSTED TO NEW HANDLEPHASE1
      //TODO: refactor into function to be called from case 5)
      c.release();
       if(params.mainThreadDispatching) p2DecisionsMutex.unlock();

      std::string txnDigest = TransactionDigest(msg.txn(), params.hashDigest);
      Debug("FB exec PHASE1[%lu:%lu][%s] with ts %lu.", msg.txn().client_id(),
          msg.txn().client_seq_num(), BytesToHex(txnDigest, 16).c_str(),
          msg.txn().timestamp().timestamp());

      if (params.validateProofs && params.signedMessages && params.verifyDeps) {
        for (const auto &dep : msg.txn().deps()) {
          if (!dep.has_write_sigs()) {
            Debug("Dep for txn %s missing signatures.",
              BytesToHex(txnDigest, 16).c_str());
            return;
          }
          if (!ValidateDependency(dep, &config, params.readDepSize, keyManager,
                verifier)) {
            Debug("VALIDATE Dependency failed for txn %s.",
                BytesToHex(txnDigest, 16).c_str());
            // safe to ignore Byzantine client
            return;
          }
        }
      }

      //start new current view
       if(params.mainThreadDispatching) current_viewsMutex.lock();
      current_views[txnDigest] = 0;
      if(params.mainThreadDispatching) current_viewsMutex.unlock();

      proto::Transaction *txn = msg.release_txn();
      ongoingMap::accessor b;
      ongoing.insert(b, std::make_pair(txnDigest, txn));
      b.release();

      Timestamp retryTs;
      const proto::CommittedProof *committedProof;

      //TODO: add parallel OCC check logic here:
      proto::ConcurrencyControl::Result result = DoOCCCheck(msg.req_id(),
          remote, txnDigest, *txn, retryTs, committedProof, true);
          // TODO: Call Relay with a txn digest.. Client receives Relay with no conflict id --> start another FB?


      p1DecisionsMap::accessor c2;
      p1Decisions.insert(c, std::make_pair(txnDigest, result));
      c2.release();
      if(result == proto::ConcurrencyControl::ABORT){
         p1ConflictsMap::accessor d;
         p1Conflicts.insert(d, std::make_pair(txnDigest, committedProof));
         d.release();
      }

      //What happens in the FB case if the result is WAIT?
      //Since we limit to depth 1, we expect this to not be possible.
      //But if it happens, the CheckDependents call will send a P1FB reply to all interested clients.
      if (result != proto::ConcurrencyControl::WAIT) {
          // if(client_starttime.find(txnDigest) == client_starttime.end()){
          //   struct timeval tv;
          //   gettimeofday(&tv, NULL);
          //   uint64_t start_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
          //   client_starttime[txnDigest] = start_time;
          // }
          P1FBorganizer *p1fb_organizer = new P1FBorganizer(msg.req_id(), txnDigest, remote, this);
          SetP1(msg.req_id(), p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, committedProof);
          SendPhase1FBReply(p1fb_organizer, txnDigest);
      }
  }
}


void Server::SetP1(uint64_t reqId, proto::Phase1Reply *p1Reply, const std::string &txnDigest, proto::ConcurrencyControl::Result &result,
  const proto::CommittedProof *conflict){
  //proto::Phase1Reply *p1Reply = p1fb_organizer->p1fbr->mutable_p1r();

  p1Reply->set_req_id(reqId);
  p1Reply->mutable_cc()->set_ccr(result);
  if (params.validateProofs) {
    *p1Reply->mutable_cc()->mutable_txn_digest() = txnDigest;
    if (result == proto::ConcurrencyControl::ABORT) {
      *phase1Reply.mutable_cc()->mutable_committed_conflict() = *conflict;
    }
  }
}

void Server::SetP2(uint64_t reqId, proto::Phase2Reply *p2Reply, const std::string &txnDigest, proto::CommitDecision &decision){
  //proto::Phase2Reply *p2Reply = p1fb_organizer->p1fbr->mutable_p2r();
  p2Reply->set_req_id(reqId);
  p2Reply->mutable_p2_decision()->set_decision(decision);

  //add decision view
  if(decision_views.find(txnDigest) == decision_views.end()) decision_views[txnDigest] = 0;
  p2Reply->mutable_p2_decision()->set_view(decision_views[txnDigest]);

  if (params.validateProofs) {
    *p2Reply->mutable_p2_decision()->mutable_txn_digest() = txnDigest;
  }
}

//TODO: add a way to buffer this message/organizer until commit/abort
//So that only the first interested client ever creates the object.
//XXX need to keep changing p2 and current views though...
void Server::SendPhase1FBReply(P1FBorganizer *p1fb_organizer, const std::string &txnDigest, bool multi) {

    proto::Phase1FBReply *p1FBReply = p1fb_organizer->p1fbr;
    if(p1FBReply->has_wb()){
      transport->SendMessage(this, *p1fb_organizer->remote, *p1FBReply);
      delete p1fb_organizer;
    }

    proto::AttachedView *attachedView = p1FBReply->mutable_attached_view();

    attachedView->mutable_current_view()->set_current_view(current_views[txnDigest]);
    attachedView->mutable_current_view()->set_txn_digest(txnDigest);
    attachedView->mutable_current_view()->set_replica_id(id);

//SIGN IT:  //TODO: Want to add this to p2 also, in case fallback is faulty - for simplicity assume only correct fallbacks for now.


    auto sendCB = [this, p1fb_organizer, multi](){
      if(p1fb_organizer->c_view_sig_outstanding || p1fb_organizer->p1_sig_outstanding || p1fb_organizer->p2_sig_outstanding) return;
      if(!multi){
          transport->SendMessage(this, *p1fb_organizer->remote, *p1fb_organizer->p1fbr);
      }
      else{
        interestedClientsMap::accessor i;
        auto jtr = interestedClients.find(i, p1fb_organizer->p1fbr->txn_digest());
        for (const auto addr : i->second) {
          transport->SendMessage(this, *addr, *p1fb_organizer->p1fbr);
        }
      }

      delete p1fb_organizer;
    };

    if (params.signedMessages) {
      p1fb_organizer->c_view_sig_outstanding = true;

      //1) sign current view
      proto::CurrentView *cView = new proto::CurrentView(attachedView->current_view());
      MessageToSign(cView, attachedView->mutable_signed_current_view(),
      [sendCB, p1fb_organizer, cView](){
          p1fb_organizer->c_view_sig_outstanding = false;
          sendCB();
          delete cView;
        });
      //2) sign p1
      if(p1FBReply->has_p1r() && p1FBReply->p1r().cc().ccr() != proto::ConcurrencyControl::ABORT){
        proto::ConcurrencyControl* cc = new proto::ConcurrencyControl(p1FBReply->p1r().cc());
        MessageToSign(cc, p1FBReply->mutable_p1r()->mutable_signed_cc(), [sendCB, p1fb_organizer, cc](){
            p1fb_organizer->c_view_sig_outstanding = false;
            sendCB();
            delete cc;
          });
      }
      //3) sign p2
      if(p1FBReply->has_p2r()){
        proto::Phase2Decision* p2Decision = new proto::Phase2Decision(p1FBReply->p2r().p2_decision());
        MessageToSign(p2Decision, p1FBReply->mutable_p2r()->mutable_signed_p2_decision(),
        [sendCB, p1fb_organizer, p2Decision](){
            p1fb_organizer->c_view_sig_outstanding = false;
            sendCB();
            delete p2Decision;
          });
      }
    }

    else{
      sendCB();
    }
}

/////////////////////////TODO: clean up from below here.

void Server::HandlePhase2FB(const TransportAddress &remote,
    proto::Phase2FB &msg) {
  //std::string txnDigest = TransactionDigest(msg.txn(), params.hashDigest);
  const std::string &txnDigest = msg.txn_digest();
  //Debug("PHASE2FB[%lu:%lu][%s] with ts %lu.", msg.txn().client_id(),
  //    msg.txn().client_seq_num(), BytesToHex(txnDigest, 16).c_str(),
  //    msg.txn().timestamp().timestamp());

  //TODO: change to multi and delete all interested clients?
  if(ForwardWriteback(remote, msg.req_id(), txnDigest)) return;

  // HandePhase2 just returns an existing decision.
  if(p2Decisions.find(msg.txn_digest()) != p2Decisions.end()){
      proto::CommitDecision decision =p2Decisions[txnDigest];

      P2FBorganizer *p2fb_organizer = new P2FBorganizer(msg.req_id(), txnDigest, remote, this);
      SetP2(msg.req_id(), p2fb_organizer->p2fbr->mutable_p2r() ,txnDigest, decision);
      SendPhase2FBReply(p2fb_organizer, txnDigest);
      Debug("PHASE2FB[%s] Sent Phase2Reply.", BytesToHex(txnDigest, 16).c_str());
  }
  //just do normal handle p2 otherwise after timeout
  else{
      //TODO: start a timer after mvtso check returns with != WAIT. That timer sets a bool,
      //When the bool is set then Handle all P2 requests. (assuming relay only starts after the timeout anyways
      // then this is not necessary to run a simulation - but it would not be robust to byz clients)

      //The timer should start running AFTER the Mvtso check returns.
      // I could make the timeout window 0 if I dont expect byz clients. An honest client will likely only ever start this on conflict.
      //std::chrono::high_resolution_clock::time_point current_time = high_resolution_clock::now();

      //TODO: call HandleP2FB again instead
      ProcessP2FB(remote, txnDigest, msg);
      //transport->Timer((CLIENTTIMEOUT), [this, &remote, &txnDigest, &msg](){ProcessP2FB(remote, txnDigest, msg);});

//TODO: for time being dont do this smarter scheduling.
// if(false){
//       struct timeval tv;
//       gettimeofday(&tv, NULL);
//       uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
//
//
//       //std::time_t current_time;
//       //std::chrono::high_resolution_clock::time_point
//       uint64_t elapsed;
//       if(client_starttime.find(txnDigest) != client_starttime.end())
//           elapsed = current_time - client_starttime[txnDigest];
//       else{
//         //PANIC, have never seen the tx that is mentioned. Start timer ourselves.
//         client_starttime[txnDigest] = current_time;
//         transport->Timer((CLIENTTIMEOUT), [this, &remote, &txnDigest, &msg](){VerifyP2FB(remote, txnDigest, msg);});
//         return;
//
//       }
//
// 	    //current_time = time(NULL);
//       //std::time_t elapsed = current_time - FBclient_timeouts[txnDigest];
//       //TODO: Replay this toy time logic with proper MS timer.
//       if (elapsed >= CLIENTTIMEOUT){
//         VerifyP2FB(remote, txnDigest, msg);
//       }
//       else{
//         //schedule for once original client has timed out.
//         transport->Timer((CLIENTTIMEOUT-elapsed), [this, &remote, &txnDigest, &msg](){VerifyP2FB(remote, txnDigest, msg);});
//         return;
//       }
//  }
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//TODO: Refactor both P1Reply and P2Reply into a single message and remove all redundant functions.
void Server::SendPhase2FBReply(P2FBorganizer *p2fb_organizer, const std::string &txnDigest, bool multi) {

    proto::Phase2FBReply *p2FBReply = p2fb_organizer->p2fbr;

    proto::AttachedView *attachedView = p2FBReply->mutable_attached_view();

    attachedView->mutable_current_view()->set_current_view(current_views[txnDigest]);
    attachedView->mutable_current_view()->set_txn_digest(txnDigest);
    attachedView->mutable_current_view()->set_replica_id(id);

//SIGN IT:  //TODO: Want to add this to p2 also, in case fallback is faulty - for simplicity assume only correct fallbacks for now.


    auto sendCB = [this, p2fb_organizer, multi](){
      if(p2fb_organizer->c_view_sig_outstanding || p2fb_organizer->p2_sig_outstanding) return;
      if(!multi){
          transport->SendMessage(this, *p2fb_organizer->remote, *p2fb_organizer->p2fbr);
      }
      else{
        interestedClientsMap::accessor i;
        auto jtr = interestedClients.find(i, p2fb_organizer->p2fbr->txn_digest());
        for (const auto addr : i->second) {
          transport->SendMessage(this, *addr, *p2fb_organizer->p2fbr);
        }
      }

      delete p2fb_organizer;
    };

    if (params.signedMessages) {
      p2fb_organizer->c_view_sig_outstanding = true;

      //1) sign current view
      proto::CurrentView *cView = new proto::CurrentView(attachedView->current_view());
      MessageToSign(cView, attachedView->mutable_signed_current_view(),
      [sendCB, p2fb_organizer, cView](){
          p2fb_organizer->c_view_sig_outstanding = false;
          sendCB();
          delete cView;
        });
      //2) sign p2
      if(p2FBReply->has_p2r()){
        proto::Phase2Decision* p2Decision = new proto::Phase2Decision(p2FBReply->p2r().p2_decision());
        MessageToSign(p2Decision, p2FBReply->mutable_p2r()->mutable_signed_p2_decision(),
        [sendCB, p2fb_organizer, p2Decision](){
            p2fb_organizer->c_view_sig_outstanding = false;
            sendCB();
            delete p2Decision;
          });
      }
    }

    else{
      sendCB();
    }
}

//TODO: p2fb needs to be on the Heap if using multithreading!!!!
//TODO: create Callback and asyncVerification
void Server::ProcessP2FB(const TransportAddress &remote, const std::string &txnDigest, proto::Phase2FB &p2fb){
  //Shotcircuit if request already processed once.
  if(ForwardWriteback(remote, 0, txnDigest)) return;
  // returns an existing decision.
  if(p2Decisions.find(txnDigest) != p2Decisions.end()){
      proto::CommitDecision decision =p2Decisions[txnDigest];
      P2FBorganizer *p2fb_organizer = new P2FBorganizer(0, txnDigest, remote, this);
      SetP2(0, p2fb_organizer->p2fbr->mutable_p2r() ,txnDigest, decision);
      SendPhase2FBReply(p2fb_organizer, txnDigest);
      Debug("PHASE2FB[%s] Sent Phase2Reply.", BytesToHex(txnDigest, 16).c_str());
      return;
  }

  uint8_t groupIndex = txnDigest[0];
  //int64_t logGroup; //probably do not need it
  const proto::Transaction *txn;
  // find txn. that maps to txnDigest. if not stored locally, and not part of the msg, reject msg.
  ongoingMap::const_accessor b;
  bool isOngoing = ongoing.find(b, txnDigest);
  if(isOngoing){
    txn = b->second;
    b.release();
    // groupIndex = groupIndex % txn->involved_groups_size();
    // UW_ASSERT(groupIndex < txn->involved_groups_size());
    // logGroup = txn->involved_groups(groupIndex);
  }
  else{
    b.release();
    if(p2fb.has_txn()){
        txn = &p2fb.txn();
        // groupIndex = groupIndex % txn->involved_groups_size();
        // UW_ASSERT(groupIndex < txn->involved_groups_size());
        // logGroup = txn->involved_groups(groupIndex);
      }
      else{
         Debug("Txn[%s] neither in ongoing nor in FallbackP2 message.", BytesToHex(txnDigest, 64).c_str());
        return;
      }
  }

  //P2FB either contains P1 GroupedSignatures, OR it just contains forwarded P2Replies.
  //TODO: Case A: The FbP2 message has f+1 matching P2replies from logShard replicas
  if(p2fb.has_p2_replies()){
    if(params.signedMessages){
      mainThreadCallback mcb(std::bind(&Server::ProcessP2FBCallback, this,
         &p2fb, txnDigest, remote.clone(), std::placeholders::_1));
      int64_t myProcessId;
      proto::CommitDecision myDecision;
      LookupP2Decision(txnDigest, myProcessId, myDecision);
      asyncValidateFBP2Replies(p2fb.decision(), txn, &txnDigest, p2fb.p2_replies(),
         keyManager, &config, myProcessId, myDecision, verifier, std::move(mcb), transport, params.multiThreading);
    }
    else{
      proto::P2Replies p2Reps = p2fb.p2_replies();
      uint32_t counter = config.f + 1;
      for(auto & p2_reply : p2Reps.p2replies()){
        if(p2_reply.has_p2_decision()){
          if(p2_reply.p2_decision().decision() == p2fb.decision() && p2_reply.p2_decision().txn_digest() == p2fb.txn_digest()){
            counter--;
          }
        }
        if(counter == 0){
          ProcessP2FBCallback(&p2fb, txnDigest, &remote, (void*) true);
          return;
        }
      }
      ProcessP2FBCallback(&p2fb, txnDigest, &remote, (void*) false);
    }

    // proto::P2Replies p2Reps = p2fb.p2_replies();
    // uint32_t counter = config.f + 1;
    // for(auto & p2_reply : p2Reps.p2replies()){
    //     proto::Phase2Decision p2dec;
    //
    //       if(!p2_reply.has_signed_p2_decision()){ return;}
    //       proto::SignedMessage sig_msg = p2_reply.signed_p2_decision();
    //
    //       if(!IsReplicaInGroup(sig_msg.process_id(), logGroup, &config)){ return;}
    //
    //       p2dec.ParseFromString(sig_msg.data());
    //       if(p2dec.decision() == p2fb.decision() && p2dec.txn_digest() == p2fb.txn_digest()){
    //         if(crypto::Verify(keyManager->GetPublicKey(sig_msg.process_id()),
    //               &sig_msg.data()[0], sig_msg.data().length(), &sig_msg.signature()[0])){ counter--;} else{return;}
    //       }
    //
    // }
  }
  //TODO: Case B: The FbP2 message has standard P1 Quorums that match the decision
  else if(p2fb.has_p1_sigs()){

      const proto::GroupedSignatures &grpSigs = p2fb.p1_sigs();
      int64_t myProcessId;
      proto::ConcurrencyControl::Result myResult;
      LookupP1Decision(txnDigest, myProcessId, myResult);

      if(params.multiThreading){
        mainThreadCallback mcb(std::bind(&Server::ProcessP2FBCallback, this,
           &p2fb, txnDigest, remote.clone(), std::placeholders::_1));
           asyncValidateP1Replies(p2fb.decision(),
                 false, txn, &txnDigest, grpSigs, keyManager, &config, myProcessId,
                 myResult, verifier, std::move(mcb), transport, true);
           return;

      }
      else{
        if(!ValidateP1Replies(p2fb.decision(), false, txn, &txnDigest, grpSigs, keyManager, &config, myProcessId, myResult, verifier)){
          return; //INVALID SIGS
        }
        ProcessP2FBCallback(&p2fb, txnDigest, &remote, (void*) true);
        return;
      }
  }
  else{
    Debug("FallbackP2 message for Txn[%s] has no proofs.", BytesToHex(txnDigest, 64).c_str());
    return;
  }

  ProcessP2FBCallback(&p2fb, txnDigest, &remote, (void*) true);
}

void Server::ProcessP2FBCallback(proto::Phase2FB *p2fb, const std::string &txnDigest,
  const TransportAddress *remote, void* valid){

    if(!valid){
      return;
    }
    proto::CommitDecision decision;
    if(params.mainThreadDispatching) p2DecisionsMutex.lock();
    if(params.mainThreadDispatching) decision_viewsMutex.lock();
    if(p2Decisions.find(txnDigest) != p2Decisions.end()){
      decision = p2Decisions[txnDigest];
    }
    else{
      p2Decisions[txnDigest] = p2fb->decision();
      decision_views[txnDigest] = 0;
      decision = p2fb->decision();
    }
    if(params.mainThreadDispatching) p2DecisionsMutex.unlock();
    if(params.mainThreadDispatching) decision_viewsMutex.unlock();


    P2FBorganizer *p2fb_organizer = new P2FBorganizer(0, txnDigest, *remote, this);
    SetP2(0, p2fb_organizer->p2fbr->mutable_p2r() ,txnDigest, decision);
    SendPhase2FBReply(p2fb_organizer, txnDigest);

    //TODO: FreeOriginal p2fb message + delete remote. (could also instantiate the p2fb_org object earlier, and delete if false)
    if(params.multiThreading || params.mainThreadDispatching){
      FreePhase2FBmessage(p2fb);
    }
    delete remote;
    Debug("PHASE2FB[%s] Sent Phase2Reply.", BytesToHex(txnDigest, 16).c_str());

}

//TODO replace with common functions.
//TODO: create callback, check in callback whether higher view exists too (if so ignore this one) - only necessary for async version
bool Server::VerifyViews(proto::InvokeFB &msg, uint32_t logGrp){

  auto txnDigest = msg.txn_digest();

  //Assuming Invoke Message contains SignedMessages for view instead of Signatures.
      proto::SignedMessages signed_messages = msg.view_signed();
      if(msg.catchup()){
        uint64_t counter = config.f +1;
        for(const auto &signed_m : signed_messages.sig_msgs()){

          proto::CurrentView view_s;
          view_s.ParseFromString(signed_m.data());

          if(IsReplicaInGroup(signed_m.process_id(), logGrp, &config)){
              if(view_s.current_view() < msg.proposed_view()){ return false;}
              if(view_s.txn_digest() != txnDigest) { return false;}
              if(crypto::Verify(keyManager->GetPublicKey(signed_m.process_id()),
                    &signed_m.data()[0], signed_m.data().length(), &signed_m.signature()[0])) { counter--;} else{return false;}
          }
          if(counter == 0) return true;
        }
      }
      else{
        uint64_t counter = 3* config.f +1;
        for(const auto &signed_m : signed_messages.sig_msgs()){

          proto::CurrentView view_s;
          view_s.ParseFromString(signed_m.data());

          if(IsReplicaInGroup(signed_m.process_id(), logGrp, &config)){
              if(view_s.current_view() < msg.proposed_view()-1){ return false;}
              if(view_s.txn_digest() != txnDigest) { return false;}
              if(crypto::Verify(keyManager->GetPublicKey(signed_m.process_id()),
                    &signed_m.data()[0], signed_m.data().length(),
                    &signed_m.signature()[0])) { counter--;} else{return false;}
          }
          if(counter == 0) return true;
        }

      }
      return false;


  // //USE THIS CODE IF ASSUMING VIEWS ARE JUST SIGNATURES - NEED TO DISTINGUISH THOUGH IN CASE VIEWS WHERE >= proposed view, not just =
  // std::string viewMsg;
  // proto::CurrentView curr_view;
  //curr_view.mutable_txn_digest(msg.txn_digest());

  //*curr_view.mutable_txn_digest() = txnDigest;

  //   proto::Signatures sigs = msg.view_sigs();
  //   if(msg.catchup()){
  //     curr_view.set_current_view(msg.proposed_view());
  //     curr_view.SerializeToString(&viewMsg);
  //     uint64_t counter = config.f +1;
  //     for(const auto &sig : sigs.sigs()){
  //       //TODO: check that this id was from the loggin shard. sig.process_id() in lG
  //       if(IsReplicaInGroup(sig.process_id(), lG, &config)){
  //           if(crypto::Verify(keyManager->GetPublicKey(sig.process_id()), viewMsg, sig.signature())) { counter--;} else{return false;}
  //       }
  //       if(counter == 0) return true;
  //     }
  //   }
  //   else{
  //     curr_view.set_current_view(msg.proposed_view()-1);
  //     curr_view.SerializeToString(&viewMsg);
  //     uint64_t counter = 3* config.f +1;
  //     for(const auto &sig : sigs.sigs()){
  //       if(IsReplicaInGroup(sig.process_id(), lG, &config)){
  //           if(crypto::Verify(keyManager->GetPublicKey(sig.process_id()), viewMsg, sig.signature() )) { counter--;} else{return false;}
  //       }
  //       if(counter == 0) return true;
  //     }
  //
  //   }
  //   return false;

}


//TODO remove remote argument, it is useless here. Instead add and keep track of INTERESTED CLIENTS REMOTE MAP
void Server::HandleInvokeFB(const TransportAddress &remote, proto::InvokeFB &msg) {
//Expect the invokeFB message to contain a P2 message if not seen a decision ourselves.
//If that is not the case: CALL HAndle Phase2B. This in turn should check if there are f+1 matching p2, and otherwise call HandlePhase2 by passing the args.

proto::Phase2FB p2fb;
// CHECK if part of logging shard. (this needs to be done at all p2s, reject if its not ourselves)
auto txnDigest = msg.txn_digest();

if(msg.proposed_view() <= current_views[txnDigest]) return; //Obsolete Invoke Message. //TODO: return new view.

  //TODO  Schedule request for when current leader timeout is complete --> check exp timeouts; then set new one.
//   struct timeval tv;
//   gettimeofday(&tv, NULL);
//   uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
//
//   uint64_t elapsed;
//   if(client_starttime.find(txnDigest) != client_starttime.end())
//       elapsed = current_time - client_starttime[txnDigest];
//   else{
//     //PANIC, have never seen the tx that is mentioned. Start timer ourselves.
//     client_starttime[txnDigest] = current_time;
//     transport->Timer((CLIENTTIMEOUT), [this, &remote, &msg](){HandleInvokeFB(remote, msg);});
//     return;
//   }
//   if (elapsed < CLIENTTIMEOUT ){
//     transport->Timer((CLIENTTIMEOUT-elapsed), [this, &remote, &msg](){HandleInvokeFB(remote, msg);});
//     return;
//   }
// //check for current FB reign
//   uint64_t FB_elapsed;
//   if(exp_timeouts.find(txnDigest) != exp_timeouts.end()){
//       FB_elapsed = current_time - FBtimeouts_start[txnDigest];
//       if(FB_elapsed < exp_timeouts[txnDigest]){
//           transport->Timer((exp_timeouts[txnDigest]-FB_elapsed), [this, &remote, &msg](){HandleInvokeFB(remote, msg);});
//           return;
//       }
//
//   }
  //otherwise pass and invoke for the first time!


      uint8_t groupIndex = txnDigest[0];
      int64_t logGroup;
      // find txn. that maps to txnDigest. if not stored locally, and not part of the msg, reject msg.
      //TODO: refactor this into common function

       ////if(params.mainThreadDispatching) ongoingMutex.lock();

      ongoingMap::const_accessor b;
      bool ongoingItr = ongoing.find(b, txnDigest);
      if(ongoingItr){
      //if(ongoing.find(txnDigest) != ongoing.end()){
      //  proto::Transaction txn = *ongoing[txnDigest];
      proto::Transaction txn = *b->second;
        groupIndex = groupIndex % txn.involved_groups_size();
        UW_ASSERT(groupIndex < txn.involved_groups_size());
        logGroup = txn.involved_groups(groupIndex);
      }
      else if(msg.has_p2fb()){
        p2fb = msg.p2fb();
        if(p2fb.has_txn()){
            proto::Transaction txn = p2fb.txn();
            groupIndex = groupIndex % txn.involved_groups_size();
            UW_ASSERT(groupIndex < txn.involved_groups_size());
            logGroup = txn.involved_groups(groupIndex);
          }
        else{
           ////if(params.mainThreadDispatching) ongoingMutex.unlock();
          return; //REPLICA HAS NEVER SEEN THIS TXN
        }
      }
      else{
         ////if(params.mainThreadDispatching) ongoingMutex.unlock();
        return; //REPLICA HAS NEVER SEEN THIS TXN
      }
      b.release();
       ////if(params.mainThreadDispatching) ongoingMutex.unlock();

      if(groupIdx != logGroup){
          return;  //This replica is not part of the shard responsible for Fallback.
      }

      //int64_t logGrp = GetLogGroup(*txn, *txnDigest);

//process decision if one does not have any yet.
//This is safe even if current_view > 0 because this replica could not have taken part in any elections yet (can only elect once you have decision), nor has yet received a dec from a larger view which it would adopt.
      if(p2Decisions.find(txnDigest) == p2Decisions.end()){
        p2fb = msg.p2fb();
        HandlePhase2FB(remote, p2fb);
        //either no need for fallback, or still no decision learned so one cannot contribute to election.
        if(writebackMessages.find(txnDigest) != writebackMessages.end() || p2Decisions.find(txnDigest) == p2Decisions.end()){
          return;
        }
      }

    if(msg.proposed_view() <= current_views[txnDigest]) return; //already voted or passed this view. //TODO: reply with signed current current_view..



//verify views
    if(!VerifyViews(msg, logGroup)) return; //invalid view signatures.
    //TODO: Adopt view if it is bigger than our current.
    if(current_views[txnDigest] >= msg.proposed_view()) return; //
    current_views[txnDigest] = msg.proposed_view();
    size_t replicaID = (msg.proposed_view() + txnDigest[0]) % config.n;

    //TODO 3) Form and send ElectFB message to all replicas within logging shard.
    proto::ElectMessage electMessage;
    electMessage.set_req_id(msg.req_id());
    electMessage.set_txn_digest(txnDigest);
    electMessage.set_decision(p2Decisions[txnDigest]);
    electMessage.set_view(msg.proposed_view());

    proto::ElectFB electFB;
    *electFB.mutable_elect_fb() = electMessage;

    if (params.signedMessages) {
      SignMessage(&electMessage, keyManager->GetPrivateKey(id), id, electFB.mutable_signed_elect_fb());
      transport->SendMessageToReplica(this, logGroup, replicaID, electFB);
    }
    else{
      transport->SendMessageToReplica(this, logGroup, replicaID, electFB);
    }

    //Set timeouts new.
    // gettimeofday(&tv, NULL);
    // current_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;
    // if(exp_timeouts.find(txnDigest) == exp_timeouts.end()){
    //    exp_timeouts[txnDigest] = CLIENTTIMEOUT; //start first timeout. //TODO: Make this a config parameter.
    //    FBtimeouts_start[txnDigest] = current_time;
    // }
    // else{
    //    exp_timeouts[txnDigest] = exp_timeouts[txnDigest] * 2; //TODO: increase timeouts exponentially. SET INCREASE RATIO IN CONFIG
    //    FBtimeouts_start[txnDigest] = current_time;
    // }
    //(TODO 4) Send MoveView message for new view to all other replicas)
}

//TODO remove remote argument
void Server::HandleElectFB(const TransportAddress &remote, proto::ElectFB &msg){

  //assume all is signed for now. TODO: make that general so no exceptions.
  if (!params.signedMessages) {Debug("ERROR HANDLE ELECT FB: NON SIGNED VERSION NOT IMPLEMENTED");}
  if(!msg.has_signed_elect_fb()) return;

  proto::SignedMessage signed_msg = msg.signed_elect_fb();
  proto::ElectMessage electMessage;
  electMessage.ParseFromString(signed_msg.data());
  std::string Msg;
  electMessage.SerializeToString(&Msg);
  std::string txnDigest = electMessage.txn_digest();

  if(ElectQuorum.find(txnDigest) == ElectQuorum.end()){
    //ElectQuorum[txnDigest] = std::unordered_set<proto::SignedMessage*>();
    ElectQuorum_meta[txnDigest] = std::make_pair(0, 0);
  }

  if(uint64_t(idx) != (electMessage.view() + txnDigest[0]) % config.n) return;
  if(IsReplicaInGroup(signed_msg.process_id(), groupIdx, &config)){
    if(ElectQuorum_meta[txnDigest].first > electMessage.view()) return;

    if(!crypto::Verify(keyManager->GetPublicKey(signed_msg.process_id()),
          &Msg[0], Msg.length(), &signed_msg.signature()[0])) return;
    // check whether all views in the elect messages match (the replica)
    if(ElectQuorum_meta[txnDigest].first == electMessage.view() && ElectQuorum[txnDigest].find(&signed_msg) == ElectQuorum[txnDigest].end()){
        // update state, keep counter of received ElectFbs  --> last one executes the function
      ElectQuorum[txnDigest].insert(&signed_msg); //no clone?
      if(electMessage.decision() == proto::COMMIT) ElectQuorum_meta[txnDigest].second++;
    }
    else if(ElectQuorum_meta[txnDigest].first < electMessage.view()){
      ElectQuorum_meta[txnDigest].first = electMessage.view();
      ElectQuorum[txnDigest].clear(); //TODO:free all.
      ElectQuorum[txnDigest].insert(&signed_msg); //no clone?
    }
  }

//form new decision, and send DecisionFB message to all replicas
  if(ElectQuorum[txnDigest].size() == uint64_t(config.n - config.f)){
    proto::CommitDecision decision;
    if(ElectQuorum_meta[txnDigest].second > uint64_t(2* config.f +1)){
      decision = proto::COMMIT;
    }
    else{
      decision = proto::ABORT;
    }
    proto::DecisionFB decisionFB;
    decisionFB.set_req_id(electMessage.req_id());
    decisionFB.set_txn_digest(txnDigest);
    decisionFB.set_dec(decision);
    decisionFB.set_view(electMessage.view());
    for (const proto::SignedMessage *iter : ElectQuorum[txnDigest] ) {
      //response->add_elect_sigs(iter);
      proto::SignedMessage* sm = decisionFB.add_elect_sigs();
      *sm = *iter;
    }
    transport->SendMessageToGroup(this, groupIdx, decisionFB);

  }
}

void Server::HandleDecisionFB(const TransportAddress &remote,
     proto::DecisionFB &msg){

    std::string txnDigest = msg.txn_digest();
    //outdated request
    if(current_views[txnDigest] > msg.view()) return;


    std::string Msg;
  //  proto::ElectFB electfb;

    proto::ElectMessage elect_msg;

//TODO: verify signatures, Ignore if from view < current
    uint64_t counter = 2* config.f +1;
    for(auto & iter :msg.elect_sigs()){  //iter of type SignedMessage
    //  electfb.ParseFromString(iter.data());
    //  electfb.SerializeToString(&Msg);
      elect_msg.ParseFromString(iter.data());
      //TODO: need to add that these signatures come from different replicas..
      //TODO: check that there are 4f+1 elect messages in total? is this necessary?
      if(crypto::Verify(keyManager->GetPublicKey(iter.process_id()),
            &iter.data()[0], iter.data().length(), &iter.signature()[0])){
        //check that replicas were electing this FB and voting for this decision
        //if(electfb.elect_fb().decision() == msg.dec() && electfb.elect_fb().view() == msg.view()){
        if(elect_msg.decision() == msg.dec() && elect_msg.view() == msg.view()){
          counter--;
        }
      }
      if(counter==0) break;
    }
    if(counter !=0) return; //sigs dont match decision

    if(decision_views[txnDigest] < msg.view()){
      decision_views[txnDigest] = msg.view();
      p2Decisions[txnDigest] = msg.dec();
    }
    if(current_views[txnDigest] < msg.view()){
      current_views[txnDigest] = msg.view();
    }
    //TODO: XXX XXX XXX Make compatible with new code
    //SetP2(msg.req_id(), txnDigest, p2Decisions[txnDigest]);  //TODO: problematic that client cannot distinguish? Wrap it inside a P2FBreply?
    phase2FBReply.Clear();
    phase2FBReply.set_txn_digest(txnDigest);
    *phase2FBReply.mutable_p2r() = phase2Reply;
    proto::AttachedView attachedView;
    attachedView.mutable_current_view()->set_current_view(current_views[txnDigest]);
    attachedView.mutable_current_view()->set_replica_id(id);

//SIGN IT:  //TODO: Want to add this to p2 also, in case fallback is faulty - for simplicity assume only correct fallbacks for now.
      *attachedView.mutable_current_view()->mutable_txn_digest() = txnDigest; //redundant line if I always include txn digest

      if (params.signedMessages) {
        proto::CurrentView cView(attachedView.current_view());
        //Latency_Start(&signLat);
        SignMessage(&cView, keyManager->GetPrivateKey(id), id, attachedView.mutable_signed_current_view());
        //Latency_End(&signLat);
      }
    *phase2FBReply.mutable_attached_view() = attachedView;

    interestedClientsMap::const_accessor i;
    bool interestedClientsItr = interestedClients.find(i, txnDigest);

    for(const TransportAddress * target :   i->second ){
      transport->SendMessage(this, *target, phase2FBReply);
    }
    i.release();
  //TODO: 2) Send Phase2Reply message to all interested clients (check list interestedClients for all remote addresses)
  //TODO: 3) send current views to client (in case there was no consensus, so the client can start a new round.)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

} // namespace indicusstore
