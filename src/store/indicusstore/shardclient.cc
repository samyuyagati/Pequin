// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/indicusstore/groupclient.cc:
 *   Single group indicus transactional client.
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
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

#include "store/indicusstore/shardclient.h"

#include <google/protobuf/util/message_differencer.h>

#include "store/indicusstore/common.h"

namespace indicusstore {

ShardClient::ShardClient(transport::Configuration *config, Transport *transport,
    uint64_t client_id, int group, const std::vector<int> &closestReplicas_,
    bool pingReplicas,
    Parameters params, KeyManager *keyManager, Verifier *verifier,
    TrueTime &timeServer, uint64_t phase1DecisionTimeout, uint64_t consecutiveMax) :
    PingInitiator(this, transport, config->n),
    client_id(client_id), transport(transport), config(config), group(group),
    timeServer(timeServer), pingReplicas(pingReplicas), params(params),
    keyManager(keyManager), verifier(verifier), phase1DecisionTimeout(phase1DecisionTimeout),
    lastReqId(0UL), failureActive(false), consecutiveMax(consecutiveMax) {
  transport->Register(this, *config, -1, -1); //phase1DecisionTimeout(1000UL)

  if (closestReplicas_.size() == 0) {
    for  (int i = 0; i < config->n; ++i) {
      closestReplicas.push_back((i + client_id) % config->n);
      // Debug("i: %d; client_id: %d", i, client_id);
      // Debug("Calculations: %d", (i + client_id) % config->n);
    }
  } else {
    closestReplicas = closestReplicas_;
  }
}

ShardClient::~ShardClient() {
}

void ShardClient::ReceiveMessage(const TransportAddress &remote,
      const std::string &type, const std::string &data, void *meta_data) {
  if (type == readReply.GetTypeName()) {
    if(params.multiThreading){
      proto::ReadReply *curr_read = GetUnusedReadReply();
      curr_read->ParseFromString(data);
      HandleReadReplyMulti(curr_read);
    }
    else{
      readReply.ParseFromString(data);
      HandleReadReply(readReply);
    }
  } else if (type == queryReply.GetTypeName()) {
    if(params.multiThreading) {
      proto::QueryReply *curr_query = GetUnusedQueryReply();
      curr_query->ParseFromString(data);
    }
    else {
      queryReply.ParseFromString(data);
      HandleQueryReply(queryReply);
    }
  } else if (type == phase1Reply.GetTypeName()) {
    phase1Reply.ParseFromString(data);
    HandlePhase1Reply(phase1Reply);
    // if (params.injectFailure.enabled && params.injectFailure.type == InjectFailureType::CLIENT_EQUIVOCATE) {
    //   HandleP1REquivocate(phase1Reply);
    // } else {
    //   HandlePhase1Reply(phase1Reply);
    // }
  } else if (type == phase2Reply.GetTypeName()) {
    phase2Reply.ParseFromString(data);
    //Use old handle Read only when proofs/signatures disabled
    if(!(params.validateProofs && params.signedMessages)){
      HandlePhase2Reply(phase2Reply);
    }
    else{ //If validateProofs and signMessages are true, then use multi view
      HandlePhase2Reply_MultiView(phase2Reply);
    }
    //HandlePhase2Reply_MultiView(phase2Reply);
    //HandlePhase2Reply(phase2Reply);
  } else if (type == ping.GetTypeName()) {
    ping.ParseFromString(data);
    HandlePingResponse(ping);

    //FALLBACK readMessages
  } else if(type == relayP1.GetTypeName()){ //receive full TX info for a dependency
    relayP1.ParseFromString(data);
    HandlePhase1Relay(relayP1); //Call into client to see if still waiting.
  }
  else if(type == phase1FBReply.GetTypeName()){
    //wait for quorum and relay to client
    phase1FBReply.ParseFromString(data);
    HandlePhase1FBReply(phase1FBReply); // update pendingFB state -- if complete, upcall to client
  }
  else if(type == phase2FBReply.GetTypeName()){
    //wait for quorum and relay to client
    phase2FBReply.ParseFromString(data);
    HandlePhase2FBReply(phase2FBReply); //update pendingFB state -- if complete, upcall to client
  }
  else if(type == forwardWB.GetTypeName()){
    forwardWB.ParseFromString(data);
    HandleForwardWB(forwardWB);
  }
  else if(type == sendView.GetTypeName()){
    sendView.ParseFromString(data);
    HandleSendViewMessage(sendView);
  }
  else {
    Panic("Received unexpected message type: %s", type.c_str());
  }
}

void ShardClient::Begin(uint64_t id) {
  Debug("[group %i] BEGIN: %lu", group, id);

  txn.Clear();
  readValues.clear();
}

void ShardClient::Query(uint64_t id, const std::string &key,
    const TimestampMessage &ts, uint64_t readMessages, uint64_t rqs,
    uint64_t rds, read_callback gcb, read_timeout_callback gtcb,
    uint32_t timeout) {
  if (BufferGet(key, gcb)) {
    Debug("[group %i] read from buffer.", group);
    return;
  }

  uint64_t reqId = lastReqId++;
  PendingQuorumQuery *pendingQuery = new PendingQuorumQuery(reqId);
  pendingQuerys[reqId] = pendingQuery;
  pendingQuery->key = key;
  pendingQuery->rqs = rqs;
  pendingQuery->rds = rds;
  pendingQuery->gcb = gcb;
  pendingQuery->gtcb = gtcb;

  query.Clear();
  query.set_req_id(reqId);
  query.set_key(key);
  *query.mutable_timestamp() = ts;

  UW_ASSERT(readMessages <= closestReplicas.size());
  for (size_t i = 0; i < readMessages; ++i) {
    Debug("[group %i] Sending QUERY to replica %lu", group, GetNthClosestReplica(i));
    transport->SendMessageToReplica(this, group, GetNthClosestReplica(i), query);
  }

}


void ShardClient::Get(uint64_t id, const std::string &key,
    const TimestampMessage &ts, uint64_t readMessages, uint64_t rqs,
    uint64_t rds, read_callback gcb, read_timeout_callback gtcb,
    uint32_t timeout) {
  if (BufferGet(key, gcb)) {
    Debug("[group %i] read from buffer.", group);
    return;
  }

  uint64_t reqId = lastReqId++;
  PendingQuorumGet *pendingGet = new PendingQuorumGet(reqId);
  pendingGets[reqId] = pendingGet;
  pendingGet->key = key;
  pendingGet->rqs = rqs;
  pendingGet->rds = rds;
  pendingGet->gcb = gcb;
  pendingGet->gtcb = gtcb;

  read.Clear();
  read.set_req_id(reqId);
  read.set_key(key);
  *read.mutable_timestamp() = ts;

  UW_ASSERT(readMessages <= closestReplicas.size());
  for (size_t i = 0; i < readMessages; ++i) {
    Debug("[group %i] Sending GET to replica %lu", group, GetNthClosestReplica(i));
    transport->SendMessageToReplica(this, group, GetNthClosestReplica(i), read);
  }

  Debug("[group %i] Sent GET [%lu : %lu]", group, id, reqId);
}

void ShardClient::Put(uint64_t id, const std::string &key,
      const std::string &value, put_callback pcb, put_timeout_callback ptcb,
      uint32_t timeout) {
  WriteMessage *writeMsg = txn.add_write_set();
  writeMsg->set_key(key);
  writeMsg->set_value(value);
  pcb(REPLY_OK, key, value);
}



void ShardClient::Phase1(uint64_t id, const proto::Transaction &transaction, const std::string &txnDigest,
  phase1_callback pcb, phase1_timeout_callback ptcb, relayP1_callback rcb, finishConflictCB fcb, uint32_t timeout) {
  Debug("[group %i] Sending PHASE1 [%lu]", group, id);
  uint64_t reqId = lastReqId++;
  client_seq_num_mapping[id].pendingP1_id = reqId;
  PendingPhase1 *pendingPhase1 = new PendingPhase1(reqId, group, transaction,
      txnDigest, config, keyManager, params, verifier, id);
  pendingPhase1s[reqId] = pendingPhase1;
  pendingPhase1->pcb = std::move(pcb);
  pendingPhase1->ptcb = std::move(ptcb);
  pendingPhase1->rcb = std::move(rcb);
  pendingPhase1->ConflictCB = std::move(fcb); //TODO: change all to std::move()
  pendingPhase1->requestTimeout = new Timeout(transport, timeout, [this, pendingPhase1]() {
      phase1_timeout_callback ptcb = pendingPhase1->ptcb;
      auto itr = this->pendingPhase1s.find(pendingPhase1->reqId);
      //uint64_t reqId = pendingPhase1->reqId;
      if (itr != this->pendingPhase1s.end()) {
        PendingPhase1 *pendingPhase1 = itr->second;
        this->pendingPhase1s.erase(itr);
        delete pendingPhase1;
      }
      ptcb(REPLY_TIMEOUT);
  });

  // create prepare request
  phase1.Clear();
  phase1.set_req_id(reqId);
  *phase1.mutable_txn() = transaction;
  phase1.set_replica_gossip(false);


  if(failureActive && params.injectFailure.type == InjectFailureType::CLIENT_SEND_PARTIAL_P1){
       phase1.set_crash_failure(true);
       for (size_t i = 0; i < config->n; ++i) {
         size_t rindex = GetNthClosestReplica(i);
         //if(rindex <= 2){
         if (rindex % 2 == 1) {
           Debug("[group %i] Sending P1 to odd-numbered replica %lu", group, rindex);
           transport->SendMessageToReplica(this, group, rindex, phase1);
         }
       }
  }
  else if(failureActive && params.injectFailure.type == InjectFailureType::CLIENT_CRASH) {
    transport->SendMessageToGroup(this, group, phase1);
  }
  else{
    transport->SendMessageToGroup(this, group, phase1);
  }
  //transport->SendMessageToGroup(this, group, phase1);

  pendingPhase1->requestTimeout->Reset();

    // struct timeval tv;
    // gettimeofday(&tv, NULL);
    // start_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds
}

void ShardClient::Phase2(uint64_t id,
    const proto::Transaction &txn, const std::string &txnDigest,
    proto::CommitDecision decision,
    const proto::GroupedSignatures &groupedSigs, phase2_callback pcb,
    phase2_timeout_callback ptcb, uint32_t timeout) {
  Debug("[group %i] Sending PHASE2 [%lu]", group, id);
  uint64_t reqId = lastReqId++;
  client_seq_num_mapping[id].pendingP2_id = reqId;

  // uint64_t special_id = reqId << 8 + client_id;
  // std::cerr << "special ID: " << special_id << std::endl;
  // std::cerr << "Trying to " << (decision? "ABORT" : "COMMIT") << "txn on slowpath" << std::endl;
  // reqId = special_id;
  // test_mapping[reqId] = id;
  // std::cerr <<"Sending Phase2 for txnId: " << id << " to group: " << group << std::endl;
  // std::cerr << "TxnId " << id << " maps to reqId: " << reqId << " sent by client" << client_id <<  std::endl;;


  PendingPhase2 *pendingPhase2 = new PendingPhase2(reqId, decision);  //TODO: add view that this decision is from (default = 0).
  //TODO: When sending an InvokeFB message, this view = the view you propose ; but unclear what decision you are waiting for?
  //Create many mappings for potential views/decisions instead.
  //XXX currently not necessary as code is written under the assumption that Invoke only happens for equivocation, and client issuing equivocation moves on to next tx.
  pendingPhase2s[reqId] = pendingPhase2;
  pendingPhase2->pcb = std::move(pcb);
  pendingPhase2->ptcb = std::move(ptcb);
  pendingPhase2->requestTimeout = new Timeout(transport, timeout, [this, pendingPhase2]() {
      phase2_timeout_callback ptcb = pendingPhase2->ptcb;
      auto itr = this->pendingPhase2s.find(pendingPhase2->reqId);
      if (itr != this->pendingPhase2s.end()) {
        PendingPhase2 *pendingPhase2 = itr->second;
        this->pendingPhase2s.erase(itr);
        delete pendingPhase2;
      }

      ptcb(REPLY_TIMEOUT);
  });

  phase2.Clear();
  phase2.set_req_id(reqId);
  phase2.set_decision(decision);
  *phase2.mutable_txn_digest() = txnDigest;
  if (params.validateProofs && params.signedMessages) {
    *phase2.mutable_grouped_sigs() = groupedSigs;
  }
  transport->SendMessageToGroup(this, group, phase2);

  pendingPhase2->requestTimeout->Reset();
}

void ShardClient::Phase2Equivocate_Simulate(uint64_t id,
    const proto::Transaction &txn, const std::string &txnDigest,
    proto::GroupedSignatures &groupedCommitSigs) {

      //std::cerr<<"SendingPhase2_Equiv for txnId: " << id << "to group: " << group << std::endl;
      uint64_t reqId = lastReqId++;

      phase2.Clear();
      phase2.set_req_id(reqId);
      phase2.set_decision(proto::COMMIT);
      *phase2.mutable_txn_digest() = txnDigest;
      if (params.validateProofs && params.signedMessages) {
        *phase2.mutable_grouped_sigs() = groupedCommitSigs;
      }
      phase2.set_simulated_equiv(true);

      for (size_t i = 0; i < config->n; ++i) {
        size_t rindex = GetNthClosestReplica(i);
        if (rindex % 2 == 0) {
          Debug("[group %i] Sending COMMIT to even-numbered replica %lu", group, rindex);
          transport->SendMessageToReplica(this, group, rindex, phase2);
        }
      }

      proto::GroupedSignatures &groupedAbortSigs = groupedCommitSigs;
      //Remove additional groups. Only need one group of f+1 sigs to simulate Abort
      //XXX NOTE! these are not "valid" signatures since they signed Commit.
      while (groupedAbortSigs.grouped_sigs().size() > 1) {
        auto itr = groupedAbortSigs.mutable_grouped_sigs()->begin();
        groupedAbortSigs.mutable_grouped_sigs()->erase(itr);
      }
      for (auto &groupSigs : *groupedAbortSigs.mutable_grouped_sigs()) {
        while (static_cast<uint64_t>(groupSigs.second.sigs_size()) > SlowAbortQuorumSize(config)) {
          groupSigs.second.mutable_sigs()->RemoveLast();
        }
      }

      phase2.Clear();
      phase2.set_req_id(reqId);
      phase2.set_decision(proto::ABORT);
      *phase2.mutable_txn_digest() = txnDigest;
      if (params.validateProofs && params.signedMessages) {
        *phase2.mutable_grouped_sigs() = groupedAbortSigs;
      }
      phase2.set_simulated_equiv(true);

      for (size_t i = 0; i < config->n; ++i) {
        size_t rindex = GetNthClosestReplica(i);
        if (rindex % 2 == 1) {
          Debug("[group %i] Sending ABORT to odd-numbered replica %lu", group, rindex);
          transport->SendMessageToReplica(this, group, rindex, phase2);
        }
      }
}

void ShardClient::Phase2Equivocate(uint64_t id,
    const proto::Transaction &txn, const std::string &txnDigest,
    const proto::GroupedSignatures &groupedCommitSigs,
    const proto::GroupedSignatures &groupedAbortSigs) {

      uint64_t reqId = lastReqId++;

      phase2.Clear();
      phase2.set_req_id(reqId);
      phase2.set_decision(proto::COMMIT);
      *phase2.mutable_txn_digest() = txnDigest;
      if (params.validateProofs && params.signedMessages) {
        *phase2.mutable_grouped_sigs() = groupedCommitSigs;
      }
      phase2.set_real_equiv(true);

      for (size_t i = 0; i < config->n; ++i) {
        size_t rindex = GetNthClosestReplica(i);
        if (rindex % 2 == 0) {
          Debug("[group %i] Sending COMMIT to even-numbered replica %lu", group, rindex);
          transport->SendMessageToReplica(this, group, rindex, phase2);
        }
      }

      phase2.Clear();
      phase2.set_req_id(reqId);
      phase2.set_decision(proto::ABORT);
      *phase2.mutable_txn_digest() = txnDigest;
      if (params.validateProofs && params.signedMessages) {
        *phase2.mutable_grouped_sigs() = groupedAbortSigs;
      }
      phase2.set_real_equiv(true);

      for (size_t i = 0; i < config->n; ++i) {
        size_t rindex = GetNthClosestReplica(i);
        if (rindex % 2 == 1) {
          Debug("[group %i] Sending ABORT to odd-numbered replica %lu", group, rindex);
          transport->SendMessageToReplica(this, group, rindex, phase2);
        }
      }
}


//TODO: remove the PendingPhase2 creation and map insertion if this client simply returns. If it participates fully, then keep.
void ShardClient::Phase2Equivocate(uint64_t id,
    const proto::Transaction &txn, const std::string &txnDigest,
    const proto::GroupedSignatures &groupedCommitSigs,
    const proto::GroupedSignatures &groupedAbortSigs, phase2_callback pcb,
    phase2_timeout_callback ptcb, uint32_t timeout) {
  Debug("[group %i] Sending PHASE2 EQUIVOCATION [%lu]", group, id);
  // first send COMMIT to half of the replicas
  uint64_t reqId = lastReqId++;
  PendingPhase2 *pendingP2Commit = new PendingPhase2(reqId, proto::COMMIT);
  pendingPhase2s[reqId] = pendingP2Commit;
  pendingP2Commit->pcb = pcb;
  pendingP2Commit->ptcb = ptcb;
  pendingP2Commit->requestTimeout = new Timeout(transport, timeout, [this, pendingP2Commit]() {
      phase2_timeout_callback ptcb = pendingP2Commit->ptcb;
      auto itr = this->pendingPhase2s.find(pendingP2Commit->reqId);
      if (itr != this->pendingPhase2s.end()) {
        PendingPhase2 *pendingPhase2 = itr->second;
        this->pendingPhase2s.erase(itr);
        delete pendingPhase2;
      }

      ptcb(REPLY_TIMEOUT);
  });

  phase2.Clear();
  phase2.set_req_id(reqId);
  phase2.set_decision(proto::COMMIT);
  *phase2.mutable_txn_digest() = txnDigest;
  if (params.validateProofs && params.signedMessages) {
    *phase2.mutable_grouped_sigs() = groupedCommitSigs;
  }

  for (size_t i = 0; i < config->n; ++i) {
    size_t rindex = GetNthClosestReplica(i);
    if (rindex % 2 == 0) {
      Debug("[group %i] Sending COMMIT to even-numbered replica %lu", group, rindex);
      transport->SendMessageToReplica(this, group, rindex, phase2);
    }
  }

  pendingP2Commit->requestTimeout->Reset();

  // then send ABORT to the other half
  reqId = lastReqId++;
  PendingPhase2 *pendingP2Abort = new PendingPhase2(reqId, proto::ABORT);
  pendingPhase2s[reqId] = pendingP2Abort;
  pendingP2Abort->pcb = pcb;
  pendingP2Abort->ptcb = ptcb;
  pendingP2Abort->requestTimeout = new Timeout(transport, timeout, [this, pendingP2Abort]() {
      phase2_timeout_callback ptcb = pendingP2Abort->ptcb;
      auto itr = this->pendingPhase2s.find(pendingP2Abort->reqId);
      if (itr != this->pendingPhase2s.end()) {
        PendingPhase2 *pendingPhase2 = itr->second;
        this->pendingPhase2s.erase(itr);
        delete pendingPhase2;
      }

      ptcb(REPLY_TIMEOUT);
  });

  phase2.Clear();
  phase2.set_req_id(reqId);
  phase2.set_decision(proto::ABORT);
  *phase2.mutable_txn_digest() = txnDigest;
  if (params.validateProofs && params.signedMessages) {
    *phase2.mutable_grouped_sigs() = groupedAbortSigs;
  }

  for (size_t i = 0; i < config->n; ++i) {
    size_t rindex = GetNthClosestReplica(i);
    if (rindex % 2 == 1) {
      Debug("[group %i] Sending ABORT to odd-numbered replica %lu", group, rindex);
      transport->SendMessageToReplica(this, group, rindex, phase2);
    }
  }

  pendingP2Abort->requestTimeout->Reset();

  // equivocation cleanup code for ShardClient
  // equivocation sent. since shardclient attempting to equivocate wont
  // handle p2 replies anyways, delete the dangling PendingPhase2 objects from pendingPhase2s
  auto itrc = this->pendingPhase2s.find(pendingP2Commit->reqId);
  if (itrc != this->pendingPhase2s.end()) {
    PendingPhase2 *pendingP2 = itrc->second;
    this->pendingPhase2s.erase(itrc);
    delete pendingP2;
  }

  auto itra = this->pendingPhase2s.find(pendingP2Abort->reqId);
  if (itra != this->pendingPhase2s.end()) {
    PendingPhase2 *pendingP2 = itra->second;
    this->pendingPhase2s.erase(itra);
    delete pendingP2;
  }
}

//TODO: make more efficient by swapping sigs instead of copying.
void ShardClient::Writeback(uint64_t id, const proto::Transaction &transaction, const std::string &txnDigest,
  proto::CommitDecision decision, bool fast, bool conflict_flag, const proto::CommittedProof &conflict,
  const proto::GroupedSignatures &p1Sigs, const proto::GroupedSignatures &p2Sigs, uint64_t decision_view) {

  writeback.Clear();
  // create commit request
  writeback.set_decision(decision);
  if (params.validateProofs && params.signedMessages) {
    if (fast && decision == proto::COMMIT) {
      *writeback.mutable_p1_sigs() = p1Sigs;
    }
    else if (fast && !conflict_flag && decision == proto::ABORT) {
      *writeback.mutable_p1_sigs() = p1Sigs;
    }
    else if (fast && conflict_flag && decision == proto::ABORT) {
      *writeback.mutable_conflict() = conflict;
      if(conflict.has_p2_view()){
        writeback.set_p2_view(conflict.p2_view()); //XXX not really necessary, we never check it
      }
      else{
        writeback.set_p2_view(0); //implies that this was a p1 proof for the conflict, attaching a view anyway..
      }

    } else {
      *writeback.mutable_p2_sigs() = p2Sigs;
      writeback.set_p2_view(decision_view); //TODO: extend this to process other views too? Bookkeeping should only be needed
      // for fallback though. Either combine the logic, or change it so that the orignial client issues FB function too

    }
  }
  writeback.set_txn_digest(txnDigest);
  // if(id == 0) { //in FB a replica may not have seen the txn... not necessary since all failed clients wouldve sent to everyone first...
  //   *writeback.mutable_txn() = transaction;
  // }

  transport->SendMessageToGroup(this, group, writeback);
  if(id > 0) {
    Debug("[group %i] Sent WRITEBACK[%lu]", group, id);
    auto itr = client_seq_num_mapping.find(id);
    if(itr != client_seq_num_mapping.end()){
        // PendingReqIds &pRids = itr->second;
        // auto itrP1 = pendingPhase1s.find(pRids.pendingP1_id);
        // if(itrP1 != pendingPhase1s.end()){
        //   delete itrP1->second;
        //   pendingPhase1s.erase(itrP1);
        // }
        // auto itrP2 = pendingPhase2s.find(pRids.pendingP2_id);
        // if(itrP2 != pendingPhase2s.end()){
        //   pendingPhase2s.erase(itrP2);
        //   delete itrP2->second;
        // }
        // pendingPhase1s.erase(pRids.pendingP1_id);
        // pendingPhase2s.erase(pRids.pendingP2_id);
        client_seq_num_mapping.erase(itr);
     }
  }
  else{
    Debug("[group %i] Sent Fallback WRITEBACK[%s]", group, BytesToHex(txnDigest, 16).c_str());
    //pendingFallbacks.erase(txnDigest);
  }
}

//Overloaded Wb function to not include ID, this is purely for debug purpose to distinguish whether a message came from FB instance.
void ShardClient::WritebackFB(const proto::Transaction &transaction,
    const std::string &txnDigest,
    proto::CommitDecision decision, bool fast, const proto::CommittedProof &conflict,
    const proto::GroupedSignatures &p1Sigs, const proto::GroupedSignatures &p2Sigs) {

  writeback.Clear();
  // create commit request
  writeback.set_decision(decision);
  if (params.validateProofs && params.signedMessages) {
    if (fast && decision == proto::COMMIT) {
      *writeback.mutable_p1_sigs() = p1Sigs;
    } else if (fast && decision == proto::ABORT) {
      *writeback.mutable_conflict() = conflict;
    } else {
      *writeback.mutable_p2_sigs() = p2Sigs;
    }
  }
  //TODO:: add view.
  writeback.set_txn_digest(txnDigest);

  transport->SendMessageToGroup(this, group, writeback);
  Debug("[group %i] Sent Fallback WRITEBACK[%s]", group, txnDigest.c_str());

  //TODO:delete pendingFB instance? And all pending phase2 that are inside.
}


void ShardClient::Abort(uint64_t id, const TimestampMessage &ts) {
  abort.Clear();
  *abort.mutable_internal()->mutable_ts() = ts;
  for (const auto &read : txn.read_set()) {
    *abort.mutable_internal()->add_read_set() = read.key();
  }

  if (params.validateProofs && params.signedMessages) {
    proto::AbortInternal internal(abort.internal());
    if (params.signatureBatchSize == 1) {
      SignMessage(&internal, keyManager->GetPrivateKey(client_id % 1024), client_id % 1024,
          abort.mutable_signed_internal());
    } else {
      std::vector<::google::protobuf::Message*> messages = {&internal};
      std::vector<proto::SignedMessage*> signedMessages = {abort.mutable_signed_internal()};
      SignMessages(messages, keyManager->GetPrivateKey(client_id % 1024), client_id % 1024,
          signedMessages, params.merkleBranchFactor);
    }
  }

  transport->SendMessageToGroup(this, group, abort);

  Debug("[group %i] Sent ABORT[%lu]", group, id);
}

bool ShardClient::SendPing(size_t replica, const PingMessage &ping) {
  transport->SendMessageToReplica(this, group, replica, ping);
  return true;
}


bool ShardClient::BufferGet(const std::string &key, read_callback rcb) {
  for (const auto &write : txn.write_set()) {
    if (write.key() == key) {
      Debug("[group %i] Key %s was written with val %s.", group,
          BytesToHex(key, 16).c_str(), BytesToHex(write.value(), 16).c_str());
      rcb(REPLY_OK, key, write.value(), Timestamp(), proto::Dependency(),
          false, false);
      return true;
    }
  }

  for (const auto &read : txn.read_set()) {
    if (read.key() == key) {
      Debug("[group %i] Key %s was already read with ts %lu.%lu.", group,
          BytesToHex(key, 16).c_str(), read.readtime().timestamp(),
          read.readtime().id());
      rcb(REPLY_OK, key, readValues[key], read.readtime(), proto::Dependency(),
          false, false);
      return true;
    }
  }

  return false;
}

void ShardClient::GetTimeout(uint64_t reqId) {
  auto itr = this->pendingGets.find(reqId);
  if (itr != this->pendingGets.end()) {
    PendingQuorumGet *pendingGet = itr->second;
    get_timeout_callback gtcb = pendingGet->gtcb;
    std::string key = pendingGet->key;
    this->pendingGets.erase(itr);
    delete pendingGet;
    gtcb(REPLY_TIMEOUT, key);
  }
}





//TODO: pass in reply as GetUnused. Free it at the end.
void ShardClient::HandleReadReplyMulti(proto::ReadReply* reply) {
  auto itr = this->pendingGets.find(reply->req_id());
  if (itr == this->pendingGets.end()) {
    return; // this is a stale request
  }
  PendingQuorumGet *req = itr->second;
  Debug("[group %i] ReadReply for %lu.", group, reply->req_id());
  //dispatch first validation

  if (params.validateProofs && params.signedMessages) {
    if (reply->has_signed_write()) {
      if(params.multiThreading){
      //TODO: RECOMMENT, just testing
      auto f = [this, reply](){
        return (void*) this->verifier->Verify(this->keyManager->GetPublicKey(reply->signed_write().process_id()),
                                              reply->signed_write().data(), reply->signed_write().signature());
      };
      auto cb = [this, reply, req](void* valid){
        if(!valid){
          //XXX TODO: FreeReadReply(reply);
          Debug("[group %i] Failed to validate signature for write.", this->group);
          return;
        }
        this->HandleReadReplyCB1(reply);
      };
     }
    }
  }

  HandleReadReplyCB1(reply);

}

void ShardClient::HandleReadReplyCB1(proto::ReadReply*reply){
  auto itr = this->pendingGets.find(reply->req_id());
  if (itr == this->pendingGets.end()) {
    return; // this is a stale request
  }
  PendingQuorumGet *req = itr->second;

  proto::Write *write = GetUnusedWrite(); //TODO: Need to allocate (use GetUnusedWrite)
  // XXX??? how to free in some cases, but assign write from reply->write otherwise...
  //Answer: probably need to make a copy. Or call release? But if I do that, not sure if I can re-use reply
  //Try this: Call getUnusedWrite and release in the if blocks respectively.
  // and always call FreeUnusedWrite  --> this way I can recycle the memory of the released write?


  //TODO: Try this first, and if it works, add it to handle p1 and p2 as well. (not as important)

  if (params.validateProofs && params.signedMessages) {
    if (reply->has_signed_write()) {
      //write = GetUnusedWrite();
      if(!write->ParseFromString(reply->signed_write().data())) {
        Debug("[group %i] Invalid serialization of write.", group);
        FreeReadReply(reply);
        FreeWrite(write);
        return;
      }

    } else {
      if (reply->has_write() && reply->write().has_committed_value()) {
        Debug("[group %i] Reply contains unsigned committed value.", group);
        FreeReadReply(reply);
        return;
      }

      if (params.verifyDeps && reply->has_write() && reply->write().has_prepared_value()) {
        Debug("[group %i] Reply contains unsigned prepared value.", group);
        FreeReadReply(reply);
        return;
      }

      *write = reply->write();
      //write = reply->release_write();

      UW_ASSERT(!write->has_committed_value());
      UW_ASSERT(!write->has_prepared_value() || !params.verifyDeps);
    }
  } else {
    *write = reply->write();
    //write = reply->release_write();
  }

  // value and timestamp are valid
  req->numReplies++;
  if (write->has_committed_value() && write->has_committed_timestamp()) {
    if (params.validateProofs) {
      if (!reply->has_proof()) {
        Debug("[group %i] Missing proof for committed write.", group);
        FreeReadReply(reply);
        FreeWrite(write);
        return;
      }

      std::string committedTxnDigest = TransactionDigest(
          reply->proof().txn(), params.hashDigest);


     auto mcb = [this, reply, req, write](void* result) mutable {
       if(!result){
         Debug("[group %i] Failed to validate committed value for read %lu.",
             this->group, reply->req_id());
         FreeReadReply(reply);
         FreeWrite(write);
         return;
       }
       else{
         HandleReadReplyCB2(reply, write);
       }
     };
    asyncValidateTransactionWrite(reply->proof(), &committedTxnDigest, req->key, write->committed_value(),
    write->committed_timestamp(), config, params.signedMessages, keyManager, verifier, mcb, transport,
    true);
    return;
  }
}
HandleReadReplyCB2(reply, write);
}

void ShardClient::HandleReadReplyCB2(proto::ReadReply* reply, proto::Write *write){

  auto itr = this->pendingGets.find(reply->req_id());
  if (itr == this->pendingGets.end()) {
    FreeReadReply(reply);
    FreeWrite(write);
    return; // this is a stale request
    //Or has already terminated (i.e. f+1 reads where received and processed before our verification returned)
  }
  PendingQuorumGet *req = itr->second;





  if (write->has_committed_value() && write->has_committed_timestamp()) {
    Timestamp replyTs(write->committed_timestamp());
    Debug("[group %i] ReadReply for %lu with committed %lu byte value and ts"
        " %lu.%lu.", group, reply->req_id(), write->committed_value().length(),
        replyTs.getTimestamp(), replyTs.getID());
    if (req->firstCommittedReply || req->maxTs < replyTs) {
      req->maxTs = replyTs;
      req->maxValue = write->committed_value();
    }
    req->firstCommittedReply = false;

  }

  if (params.maxDepDepth > -2 && write->has_prepared_value() &&
      write->has_prepared_timestamp() &&
      write->has_prepared_txn_digest()) {
    Timestamp preparedTs(write->prepared_timestamp());
    Debug("[group %i] ReadReply for %lu with prepared %lu byte value and ts"
        " %lu.%lu.", group, reply->req_id(), write->prepared_value().length(),
        preparedTs.getTimestamp(), preparedTs.getID());
    auto preparedItr = req->prepared.find(preparedTs);
    if (preparedItr == req->prepared.end()) {
      req->prepared.insert(std::make_pair(preparedTs,
            std::make_pair(*write, 1)));
    } else if (preparedItr->second.first == *write) {
      preparedItr->second.second += 1;
    }

    if (params.validateProofs && params.signedMessages && params.verifyDeps) {
      proto::Signature *sig = req->preparedSigs[preparedTs].add_sigs();
      sig->set_process_id(reply->signed_write().process_id());
      *sig->mutable_signature() = reply->signed_write().signature();
    }
  }

  if (req->numReplies >= req->rqs) {
    if (params.maxDepDepth > -2) {
      for (auto preparedItr = req->prepared.rbegin();
          preparedItr != req->prepared.rend(); ++preparedItr) {
        if (preparedItr->first < req->maxTs) {
          break;
        }

        if (preparedItr->second.second >= req->rds) {
          req->maxTs = preparedItr->first;
          req->maxValue = preparedItr->second.first.prepared_value();
          *req->dep.mutable_write() = preparedItr->second.first;
          if (params.validateProofs && params.signedMessages && params.verifyDeps) {
            *req->dep.mutable_write_sigs() = req->preparedSigs[preparedItr->first];
          }
          req->dep.set_involved_group(group);
          req->hasDep = true;
          break;
        }
      }
    }
    pendingGets.erase(itr);
    ReadMessage *read = txn.add_read_set();
    *read->mutable_key() = req->key;
    req->maxTs.serialize(read->mutable_readtime());
    readValues[req->key] = req->maxValue;
    req->gcb(REPLY_OK, req->key, req->maxValue, req->maxTs, req->dep,
        req->hasDep, true);
    delete req; //XXX VERY IMPORTANT: dont delete while something is still dispatched for this reqId
    //could cause segfault. Need to keep a counter of things that are dispatched and only delete
    //once its gone. (dont need counter: just check in each callback if req still in map.!)
  }
  FreeReadReply(reply);
  FreeWrite(write);
}



/* Callback from a group replica on get operation completion. */
void ShardClient::HandleReadReply(const proto::ReadReply &reply) {

  auto itr = this->pendingGets.find(reply.req_id());
  if (itr == this->pendingGets.end()) {
    return; // this is a stale request
  }
  PendingQuorumGet *req = itr->second;
  Debug("[group %i] ReadReply for %lu.", group, reply.req_id());

  const proto::Write *write;
  bool skip = false;

  if (params.validateProofs && params.signedMessages) {
    // consecutive_reads++;
    // skip = (consecutive_reads % 3 == 0) ? true : false;
    if (reply.has_signed_write()) {
      if (!skip && !verifier->Verify(keyManager->GetPublicKey(reply.signed_write().process_id()),
              reply.signed_write().data(), reply.signed_write().signature())) {
        Debug("[group %i] Failed to validate signature for write.", group);
        return;
      }

      if(!validatedPrepared.ParseFromString(reply.signed_write().data())) {
        Debug("[group %i] Invalid serialization of write.", group);
        return;
      }

      write = &validatedPrepared;
    } else {
      if (reply.has_write() && reply.write().has_committed_value()) {
        Debug("[group %i] Reply contains unsigned committed value.", group);
        return;
      }

      if (params.verifyDeps && reply.has_write() && reply.write().has_prepared_value()) {
        Debug("[group %i] Reply contains unsigned prepared value.", group);
        return;
      }

      write = &reply.write();
      UW_ASSERT(!write->has_committed_value());
      UW_ASSERT(!write->has_prepared_value() || !params.verifyDeps);
    }
  } else {
    write = &reply.write();
  }

  // value and timestamp are valid
  req->numReplies++;
  if (write->has_committed_value() && write->has_committed_timestamp()) {
    if (!skip && params.validateProofs) {
      if (!reply.has_proof()) {
        Debug("[group %i] Missing proof for committed write.", group);
        return;
      }

      std::string committedTxnDigest = TransactionDigest(
          reply.proof().txn(), params.hashDigest);
      if (!ValidateTransactionWrite(reply.proof(), &committedTxnDigest,
            req->key, write->committed_value(), write->committed_timestamp(),
            config, params.signedMessages, keyManager, verifier)) {
        Debug("[group %i] Failed to validate committed value for read %lu.",
            group, reply.req_id());
        // invalid replies can be treated as if we never received a reply from
        //     a crashed replica
        return;
      }
    }

    Timestamp replyTs(write->committed_timestamp());
    Debug("[group %i] ReadReply for %lu with committed %lu byte value and ts"
        " %lu.%lu.", group, reply.req_id(), write->committed_value().length(),
        replyTs.getTimestamp(), replyTs.getID());
    if (req->firstCommittedReply || req->maxTs < replyTs) {
      req->maxTs = replyTs;
      req->maxValue = write->committed_value();
    }
    req->firstCommittedReply = false;
  }

  //TODO: change so client does not accept reads with depth > some t... (fine for now since
  // servers dont fail and use the same param setting)
  if (params.maxDepDepth > -2 && write->has_prepared_value() &&
      write->has_prepared_timestamp() &&
      write->has_prepared_txn_digest()) {
    Timestamp preparedTs(write->prepared_timestamp());
    Debug("[group %i] ReadReply for %lu with prepared %lu byte value and ts"
        " %lu.%lu.", group, reply.req_id(), write->prepared_value().length(),
        preparedTs.getTimestamp(), preparedTs.getID());
    auto preparedItr = req->prepared.find(preparedTs);
    if (preparedItr == req->prepared.end()) {
      req->prepared.insert(std::make_pair(preparedTs,
            std::make_pair(*write, 1)));
    } else if (preparedItr->second.first == *write) {
      preparedItr->second.second += 1;
    }

    if (params.validateProofs && params.signedMessages && params.verifyDeps) {
      proto::Signature *sig = req->preparedSigs[preparedTs].add_sigs();
      sig->set_process_id(reply.signed_write().process_id());
      *sig->mutable_signature() = reply.signed_write().signature();
    }
  }

  if (req->numReplies >= req->rqs) {
    if (params.maxDepDepth > -2) {
      for (auto preparedItr = req->prepared.rbegin();
          preparedItr != req->prepared.rend(); ++preparedItr) {
        if (preparedItr->first < req->maxTs) {
          break;
        }

        if (preparedItr->second.second >= req->rds) {
          req->maxTs = preparedItr->first;
          req->maxValue = preparedItr->second.first.prepared_value();
          *req->dep.mutable_write() = preparedItr->second.first;
          if (params.validateProofs && params.signedMessages && params.verifyDeps) {
            *req->dep.mutable_write_sigs() = req->preparedSigs[preparedItr->first];
          }
          req->dep.set_involved_group(group);
          req->hasDep = true;
          break;
        }
      }
    }
    pendingGets.erase(itr);
    ReadMessage *read = txn.add_read_set();
    *read->mutable_key() = req->key;
    req->maxTs.serialize(read->mutable_readtime());
    readValues[req->key] = req->maxValue;
    req->gcb(REPLY_OK, req->key, req->maxValue, req->maxTs, req->dep,
        req->hasDep, true);
    delete req;
  }
}

void ShardClient::HandleQueryReply(const proto::QueryReply &reply) {
  std::cerr << "HANDLEING QUERY REPLY IN ShardClient" << std::endl;
  auto itr = this->pendingQuerys.find(reply.req_id());
  if (itr == this->pendingQuerys.end()) {
    return; // this is a stale request
  }
  PendingQuorumQuery *req = itr->second;
  Debug("[group %i] ReadReply for %lu.", group, reply.req_id());

  const proto::Write *write;
  bool skip = false;

  if (params.validateProofs && params.signedMessages) {
    // consecutive_reads++;
    // skip = (consecutive_reads % 3 == 0) ? true : false;
    if (reply.has_signed_write()) {
      if (!skip && !verifier->Verify(keyManager->GetPublicKey(reply.signed_write().process_id()),
              reply.signed_write().data(), reply.signed_write().signature())) {
        Debug("[group %i] Failed to validate signature for write.", group);
        return;
      }

      if(!validatedPrepared.ParseFromString(reply.signed_write().data())) {
        Debug("[group %i] Invalid serialization of write.", group);
        return;
      }

      write = &validatedPrepared;
    } else {
      if (reply.has_write() && reply.write().has_committed_value()) {
        Debug("[group %i] Reply contains unsigned committed value.", group);
        return;
      }

      if (params.verifyDeps && reply.has_write() && reply.write().has_prepared_value()) {
        Debug("[group %i] Reply contains unsigned prepared value.", group);
        return;
      }

      write = &reply.write();
      UW_ASSERT(!write->has_committed_value());
      UW_ASSERT(!write->has_prepared_value() || !params.verifyDeps);
    }
  } else {
    write = &reply.write();
  }

  // value and timestamp are valid
  req->numReplies++;
  if (write->has_committed_value() && write->has_committed_timestamp()) {
    if (!skip && params.validateProofs) {
      if (!reply.has_proof()) {
        Debug("[group %i] Missing proof for committed write.", group);
        return;
      }

      std::string committedTxnDigest = TransactionDigest(
          reply.proof().txn(), params.hashDigest);
      if (!ValidateTransactionWrite(reply.proof(), &committedTxnDigest,
            req->key, write->committed_value(), write->committed_timestamp(),
            config, params.signedMessages, keyManager, verifier)) {
        Debug("[group %i] Failed to validate committed value for read %lu.",
            group, reply.req_id());
        // invalid replies can be treated as if we never received a reply from
        //     a crashed replica
        return;
      }
    }

    Timestamp replyTs(write->committed_timestamp());
    Debug("[group %i] ReadReply for %lu with committed %lu byte value and ts"
        " %lu.%lu.", group, reply.req_id(), write->committed_value().length(),
        replyTs.getTimestamp(), replyTs.getID());
    if (req->firstCommittedReply || req->maxTs < replyTs) {
      req->maxTs = replyTs;
      req->maxValue = write->committed_value();
    }
    req->firstCommittedReply = false;
  }

  //TODO: change so client does not accept reads with depth > some t... (fine for now since
  // servers dont fail and use the same param setting)
  if (params.maxDepDepth > -2 && write->has_prepared_value() &&
      write->has_prepared_timestamp() &&
      write->has_prepared_txn_digest()) {
    Timestamp preparedTs(write->prepared_timestamp());
    Debug("[group %i] ReadReply for %lu with prepared %lu byte value and ts"
        " %lu.%lu.", group, reply.req_id(), write->prepared_value().length(),
        preparedTs.getTimestamp(), preparedTs.getID());
    auto preparedItr = req->prepared.find(preparedTs);
    if (preparedItr == req->prepared.end()) {
      req->prepared.insert(std::make_pair(preparedTs,
            std::make_pair(*write, 1)));
    } else if (preparedItr->second.first == *write) {
      preparedItr->second.second += 1;
    }

    if (params.validateProofs && params.signedMessages && params.verifyDeps) {
      proto::Signature *sig = req->preparedSigs[preparedTs].add_sigs();
      sig->set_process_id(reply.signed_write().process_id());
      *sig->mutable_signature() = reply.signed_write().signature();
    }
  }
  //TODO
  // does any of this change if dpnds are sent with reply
  if (req->numReplies >= req->rqs) {
    if (params.maxDepDepth > -2) {
      for (auto preparedItr = req->prepared.rbegin();
          preparedItr != req->prepared.rend(); ++preparedItr) {
        if (preparedItr->first < req->maxTs) {
          break;
        }

        if (preparedItr->second.second >= req->rds) {
          req->maxTs = preparedItr->first;
          req->maxValue = preparedItr->second.first.prepared_value();
          *req->dep.mutable_write() = preparedItr->second.first;
          if (params.validateProofs && params.signedMessages && params.verifyDeps) {
            *req->dep.mutable_write_sigs() = req->preparedSigs[preparedItr->first];
          }
          req->dep.set_involved_group(group);
          req->hasDep = true;
          break;
        }
      }
    }
    pendingQuerys.erase(itr);
    //TODO 
    // figure out how we should add to read set now and if we want to treat read var same way.
    ReadMessage *read = txn.add_read_set();
    *read->mutable_key() = req->key;
    req->maxTs.serialize(read->mutable_readtime());
    // readValues[req->key] = req->maxValue;
    req->gcb(REPLY_OK, req->key, req->maxValue, req->maxTs, req->dep,
        req->hasDep, true);
    delete req;
  }
}



void ShardClient::HandlePhase1Reply(proto::Phase1Reply &reply) {

  ProcessP1R(reply);
}

void ShardClient::ProcessP1R(proto::Phase1Reply &reply, bool FB_path, PendingFB *pendingFB, const std::string *txnDigest){

  PendingPhase1 *pendingPhase1;
  std::unordered_map<uint64_t, PendingPhase1 *>::iterator itr;
  if(!FB_path){
    itr = this->pendingPhase1s.find(reply.req_id());
    if (itr == this->pendingPhase1s.end()) {
      return; // this is a stale request
    }

    pendingPhase1 = itr->second;
  }
  else {
    pendingPhase1 = pendingFB->pendingP1;
  }

  bool hasSigned = (params.validateProofs && params.signedMessages) &&
   (!reply.has_cc() || reply.cc().ccr() != proto::ConcurrencyControl::ABORT);

  const proto::ConcurrencyControl *cc = nullptr;
  if (hasSigned) {
    Debug("[group %i] Verifying signed_cc from %lu with signatures bytes %lu"
        " because has_cc %d and ccr %d.",
        group, reply.signed_cc().process_id(), reply.signed_cc().signature().length(),
        reply.has_cc(),
        reply.cc().ccr());
    if (!reply.has_signed_cc()) {
      return;
    }

    if (!pendingPhase1->replicasVerified.insert(reply.signed_cc().process_id()).second) {
      Debug("Already verified signature from %lu.", reply.signed_cc().process_id());
      return;
    }

    if (!IsReplicaInGroup(reply.signed_cc().process_id(), group, config)) {
      Debug("[group %d] Phase1Reply from replica %lu who is not in group.",
          group, reply.signed_cc().process_id());
      return;
    }

    if (!verifier->Verify(keyManager->GetPublicKey(reply.signed_cc().process_id()),
          reply.signed_cc().data(), reply.signed_cc().signature())) {
      Debug("[group %i] Signature %s %s from replica %lu is not valid.", group,
            BytesToHex(reply.signed_cc().data(), 100).c_str(),
            BytesToHex(reply.signed_cc().signature(), 100).c_str(),
            reply.signed_cc().process_id());

      return;
    }
    if (!validatedCC.ParseFromString(reply.signed_cc().data())) { //validatedCC is a global variable of type proto:CC
      return;
    }

    cc = &validatedCC;

  } else {
    UW_ASSERT(reply.has_cc());

    cc = &reply.cc();
  }

  Debug("[group %i] PHASE1R process ccr=%d", group, cc->ccr());

  if (!pendingPhase1->p1Validator.ProcessMessage(*cc, (failureActive && !FB_path) )) {
    return;
  }

  if (hasSigned) {
    proto::Signature *sig = pendingPhase1->p1ReplySigs[cc->ccr()].add_sigs();
    sig->set_process_id(reply.signed_cc().process_id());
    *sig->mutable_signature() = reply.signed_cc().signature();
  }

  //Keep track of conflicts.
  if(reply.has_abstain_conflict()){
    proto::Transaction* abstain_conflict = reply.release_abstain_conflict();
    pendingPhase1->abstain_conflicts.insert(abstain_conflict);
  }

  Phase1ValidationState state = pendingPhase1->p1Validator.GetState();
  switch (state) {
    case EQUIVOCATE:
      Debug("[group %i] Equivocation path taken [%lu]", group, reply.req_id());
      pendingPhase1->decision = proto::COMMIT;
      pendingPhase1->fast = false;
      Phase1Decision(itr, true); //use non-default flag to elicit equivcocation path
      break;
    case FAST_COMMIT:
      Debug("P1Validator STATE: FAST_COMMIT");
      pendingPhase1->decision = proto::COMMIT;
      pendingPhase1->fast = true;
      !FB_path ? Phase1Decision(itr) : Phase1FBDecision(pendingFB);
      break;
    case FAST_ABORT:
      Debug("P1Validator STATE: FAST_ABORT");
      pendingPhase1->decision = proto::ABORT;
      pendingPhase1->fast = true;
      pendingPhase1->conflict_flag = true;
      if (params.validateProofs) {
        pendingPhase1->conflict = cc->committed_conflict();
      }
      !FB_path ? Phase1Decision(itr) : Phase1FBDecision(pendingFB);
      break;
    case FAST_ABSTAIN:  //INSERTED THIS NEW
      Debug("P1Validator STATE: FAST_ABSTAIN");
      pendingPhase1->decision = proto::ABORT;
      pendingPhase1->fast = true;
      !FB_path ? Phase1Decision(itr) : Phase1FBDecision(pendingFB);
      break;
    case SLOW_COMMIT_FINAL:
      Debug("P1Validator STATE: SLOW_COMMIT_FINAL");
      pendingPhase1->decision = proto::COMMIT;
      pendingPhase1->fast = false;
      !FB_path ? Phase1Decision(itr) : Phase1FBDecision(pendingFB);
      break;
    case SLOW_ABORT_FINAL:
      Debug("P1Validator STATE: SLOW_ABORT_FINAL");
      pendingPhase1->decision = proto::ABORT;
      pendingPhase1->fast = false;
      !FB_path ? Phase1Decision(itr) : Phase1FBDecision(pendingFB);
      break;
    case SLOW_COMMIT_TENTATIVE:
      Debug("P1Validator STATE: SLOW_COMMIT_TENTATIVE - START TIMER");
      if(phase1DecisionTimeout == 0 && pendingPhase1->first_decision){
        pendingPhase1->first_decision = false;
        pendingPhase1->decision = proto::COMMIT;
        pendingPhase1->fast = false;
        !FB_path ? Phase1Decision(itr) : Phase1FBDecision(pendingFB);
      }
      else{
        if (!pendingPhase1->decisionTimeoutStarted){
          if(!FB_path){
            uint64_t reqId = reply.req_id();
            pendingPhase1->decisionTimeout = new Timeout(transport,
                phase1DecisionTimeout, [this, reqId]() {
                  auto itr = pendingPhase1s.find(reqId);
                  if (itr == pendingPhase1s.end()) {
                    return;
                  }
                  itr->second->decision = proto::COMMIT;
                  itr->second->fast = false;
                  Phase1Decision(itr);
                }
              );
          }
          else{
            pendingPhase1->decisionTimeout = new Timeout(transport,
              phase1DecisionTimeout, [this, txnDig = *txnDigest]() {

                auto itr = pendingFallbacks.find(txnDig);
                  if (itr == pendingFallbacks.end()) {
                    return;
                  }
                  if(!itr->second->p1){
                    itr->second->pendingP1->decisionTimeout->Stop();
                    return;
                  }
                  Debug("P1Validator STATE: SLOW_COMMIT_TENTATIVE - Execute TIMER");
                  itr->second->pendingP1->decision = proto::COMMIT;
                  itr->second->pendingP1->fast = false;
                  Phase1FBDecision(itr->second);
                }
              );
          }
          pendingPhase1->decisionTimeout->Reset();
          pendingPhase1->decisionTimeoutStarted = true;
        }
      }
      break;

    case SLOW_ABORT_TENTATIVE:
      Debug("P1Validator STATE: SLOW_ABORT_TENTATIVE - START TIMER");
      if(phase1DecisionTimeout == 0 && pendingPhase1->first_decision){
        pendingPhase1->first_decision = false;
        pendingPhase1->decision = proto::ABORT;
        pendingPhase1->fast = false;
        !FB_path ? Phase1Decision(itr) : Phase1FBDecision(pendingFB);
      }
      else{
        if (!pendingPhase1->decisionTimeoutStarted) {
          if(!FB_path){
            uint64_t reqId = reply.req_id();
            pendingPhase1->decisionTimeout = new Timeout(transport,
                phase1DecisionTimeout, [this, reqId]() {
                  auto itr = pendingPhase1s.find(reqId);
                  if (itr == pendingPhase1s.end()) {
                    return;
                  }
                  itr->second->decision = proto::ABORT;
                  itr->second->fast = false;
                  Phase1Decision(itr);
                }
              );
          }
          else{
            pendingPhase1->decisionTimeout = new Timeout(transport,
              phase1DecisionTimeout, [this, txnDig = *txnDigest]() {

                auto itr = pendingFallbacks.find(txnDig);
                  if (itr == pendingFallbacks.end()) {
                    return;
                  }
                  if(!itr->second->p1){
                    itr->second->pendingP1->decisionTimeout->Stop();
                    return;
                  }
                  Debug("P1Validator STATE: SLOW_ABORT_TENTATIVE - Execute TIMER");
                  itr->second->pendingP1->decision = proto::ABORT;
                  itr->second->pendingP1->fast = false;
                  Phase1FBDecision(itr->second);
                }
              );
          }
          pendingPhase1->decisionTimeout->Reset();
          pendingPhase1->decisionTimeoutStarted = true;
        }
      }
      break;
    case SLOW_ABORT_TENTATIVE2:
        Debug("P1Validator STATE: SLOW_ABORT_TENTATIVE2 - START TIMER");
        if(phase1DecisionTimeout == 0 && pendingPhase1->first_decision){
          pendingPhase1->first_decision = false;
          pendingPhase1->decision = proto::ABORT;
          pendingPhase1->fast = false;
          !FB_path ? Phase1Decision(itr) : Phase1FBDecision(pendingFB);
        }
        else{
          if (!pendingPhase1->decisionTimeoutStarted) {
            if(!FB_path){
              uint64_t reqId = reply.req_id();
              pendingPhase1->decisionTimeout = new Timeout(transport,
                  phase1DecisionTimeout, [this, reqId]() {
                    auto itr = pendingPhase1s.find(reqId);
                    if (itr == pendingPhase1s.end()) {
                      return;
                    }
                    itr->second->decision = proto::ABORT;
                    itr->second->fast = false;
                    Phase1Decision(itr);
                  }
                );
            }
            else{
              pendingPhase1->decisionTimeout = new Timeout(transport,
                  phase1DecisionTimeout, [this, txnDig = *txnDigest]() {

                    auto itr = pendingFallbacks.find(txnDig);
                    if (itr == pendingFallbacks.end()) {
                      return;
                    }
                    if(!itr->second->p1){
                      itr->second->pendingP1->decisionTimeout->Stop();
                      return;
                    }
                    Debug("P1Validator STATE: SLOW_ABORT_TENTATIVE2 - Execute TIMER");
                    itr->second->pendingP1->decision = proto::ABORT;
                    itr->second->pendingP1->fast = false;
                    Phase1FBDecision(itr->second);
                  }
                );
            }
            pendingPhase1->decisionTimeout->Reset();
            pendingPhase1->decisionTimeoutStarted = true;
          }
        }
        break;
    case NOT_ENOUGH:
      break;
    default:
      break;
  }
}

void ShardClient::HandlePhase2Reply(const proto::Phase2Reply &reply) {
  auto itr = this->pendingPhase2s.find(reply.req_id());
  if (itr == this->pendingPhase2s.end()) {
    Debug("[group %i] Received stale Phase2Reply for request %lu.", group,
        reply.req_id());
    return; // this is a stale request
  }

  const proto::Phase2Decision *p2Decision = nullptr;
  if (params.validateProofs && params.signedMessages) {
    if (!reply.has_signed_p2_decision()) {
      Debug("[group %i] Phase2Reply missing signed_p2_decision.", group);
      return;
    }

    if (!itr->second->replicasVerified.insert(reply.signed_p2_decision().process_id()).second) {
      Debug("Already verified signature from %lu.", reply.signed_p2_decision().process_id());
      Panic("duplicate P2 from server %lu", reply.signed_p2_decision().process_id());
      return;
    }

    if (!IsReplicaInGroup(reply.signed_p2_decision().process_id(), group, config)) {
      Debug("[group %d] Phase2Reply from replica %lu who is not in group.",
          group, reply.signed_p2_decision().process_id());
      return;
    }

    //TODO: RECOMMENT, just testing
    if (!verifier->Verify(keyManager->GetPublicKey(
            reply.signed_p2_decision().process_id()),
          reply.signed_p2_decision().data(),
          reply.signed_p2_decision().signature())) {
      Debug("[group %d] Phase2Reply from replica %lu fails verification.",
                group, reply.signed_p2_decision().process_id());
      return;
    }

    if (!validatedP2Decision.ParseFromString(reply.signed_p2_decision().data())) {
      Debug("[group %d] Phase2Reply from replica %lu fails deserialization.",
                group, reply.signed_p2_decision().process_id());
      return;
    }

    p2Decision = &validatedP2Decision;

  } else {
    p2Decision = &reply.p2_decision();
  }

  Debug("[group %i] PHASE2 reply with decision %d", group,
      p2Decision->decision());

  if (params.validateProofs && params.signedMessages) {
    proto::Signature *sig = itr->second->p2ReplySigs.add_sigs();
    sig->set_process_id(reply.signed_p2_decision().process_id());
    *sig->mutable_signature()= reply.signed_p2_decision().signature();
  }

//TODO: Edit this to check for matching view too. Can be ommitted because correct client expects all in view 0?
//If it receives messages with view != 0 it needs to start its own fallback instance.
  if(params.validateProofs){
    if(!p2Decision->has_view()) return;
    if(p2Decision->view() != 0){
      Panic("Original client cannot handle view != 0");
      return;
    }  //TODO: start fallback instance here. (case can happen if client is slow)
    //TODO: start fb "instance" and roll over this request to that fallback function. (makes it so that when
    //client receives bunch of messages from different views, they directly count towards the new Quorums needed)
  }

//Correct client KNOWS to expect only matching replies so we can just count those.
  if (p2Decision->decision() == itr->second->decision) {
    itr->second->matchingReplies++;
  }

  if (itr->second->matchingReplies >= QuorumSize(config)) {
    PendingPhase2 *pendingPhase2 = itr->second;
    pendingPhase2->pcb(pendingPhase2->decision, 0, pendingPhase2->p2ReplySigs);
    this->pendingPhase2s.erase(itr);
    delete pendingPhase2;
  }
}

void ShardClient::HandlePhase2Reply_MultiView(const proto::Phase2Reply &reply) {

  //std::cerr << "Received P2R for Req Id:" << reply.req_id() << " mapping to TxnId["  << test_mapping[reply.req_id()] << "]"<< std::endl;

  auto itr = this->pendingPhase2s.find(reply.req_id());
  if (itr == this->pendingPhase2s.end()) {
     Debug("[group %i] Received stale Phase2Reply for request %lu.", group,
        reply.req_id());
    return; // this is a stale request
  }

  const proto::Phase2Decision *p2Decision = nullptr;
  if (params.validateProofs && params.signedMessages) {
    if (!reply.has_signed_p2_decision()) {
      Debug("[group %i] Phase2Reply missing signed_p2_decision.", group);
      return;
    }

    if (!IsReplicaInGroup(reply.signed_p2_decision().process_id(), group, config)) {
      Debug("[group %d] Phase2Reply from replica %lu who is not in group.",
          group, reply.signed_p2_decision().process_id());
      return;
    }

    if (!verifier->Verify(keyManager->GetPublicKey(
            reply.signed_p2_decision().process_id()),
          reply.signed_p2_decision().data(),
          reply.signed_p2_decision().signature())) {
      Debug("[group %d] Phase2Reply from replica %lu fails verification.",
                group, reply.signed_p2_decision().process_id());
      return;
    }

    if (!validatedP2Decision.ParseFromString(reply.signed_p2_decision().data())) {
      Debug("[group %d] Phase2Reply from replica %lu fails deserialization.",
                group, reply.signed_p2_decision().process_id());
      return;
    }

    if(!validatedP2Decision.has_view()) return;

    p2Decision = &validatedP2Decision;

  } else {
    p2Decision = &reply.p2_decision();
  }

  //WARNING: The following is a "hack" by which we register an honest client to receive
  //messages from view !=0, but do not have it proactively participate in the FB protocol itself
      //TODO: Fully merge normal and fallback code paths so that a client can issue FB code components for its own transaction.
  view_p2ReplySigs &viewP2RS =  itr->second->manage_p2ReplySigs[p2Decision->view()];

  if (params.validateProofs && params.signedMessages) {
    if (!viewP2RS.first.insert(reply.signed_p2_decision().process_id()).second) {
      Debug("Already verified signature from %lu. for view %lu", reply.signed_p2_decision().process_id(), p2Decision->view());
      Panic("duplicate P2 from server %lu", reply.signed_p2_decision().process_id());
      return;
    }
  }

  Debug("[group %i] PHASE2 reply with decision %d for view %lu", group,
      p2Decision->decision(), p2Decision->view());

  // std::cerr << "TxnId["  << test_mapping[reply.req_id()] <<"] received decision " << p2Decision->decision() << " from view " << p2Decision->decision() << std::endl;
  // if (!failureActive && p2Decision->decision() != itr->second->decision) {
  //       fprintf(stderr, "Expected decision %d, but got decision %d from view %lu", itr->second->decision, p2Decision->decision(), p2Decision->view());
  // }

  proto::Signatures &p2RS = viewP2RS.second[p2Decision->decision()];

  if (params.validateProofs && params.signedMessages) {
    proto::Signature *sig = p2RS.add_sigs();
    sig->set_process_id(reply.signed_p2_decision().process_id());
    *sig->mutable_signature()= reply.signed_p2_decision().signature();
  }

  if (p2RS.sigs().size() >= QuorumSize(config)) {
    //if(p2Decision->view() > 0) Panic("Original client subscribe works properly!");
    PendingPhase2 *pendingPhase2 = itr->second;
    pendingPhase2->pcb(p2Decision->decision(), p2Decision->view(), p2RS);
    this->pendingPhase2s.erase(itr);
    delete pendingPhase2;
  }
}

void ShardClient::Phase1Decision(uint64_t reqId) {
  auto itr = this->pendingPhase1s.find(reqId);
  if (itr == this->pendingPhase1s.end()) {
    return; // this is a stale request
  }

  Phase1Decision(itr);
}

void ShardClient::Phase1Decision(
    std::unordered_map<uint64_t, PendingPhase1 *>::iterator itr, bool eqv_ready) {

      // struct timeval tv;
      // gettimeofday(&tv, NULL);
      // uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds
      // total_elapsed += (current_time - start_time);
      // total_prepare++;
      // if(total_prepare == 200) std::cerr << "Average time to prepare: " << (total_elapsed / total_prepare) << " us" << std::endl;

  PendingPhase1 *pendingPhase1 = itr->second;
  // if (pendingPhase1->requestTimeout != nullptr) {
  //   delete pendingPhase1->requestTimeout;
  //   pendingPhase1->requestTimeout = nullptr;
  // }
  // if (pendingPhase1->decisionTimeout != nullptr) {
  //   delete pendingPhase1->decisionTimeout;
  //   pendingPhase1->decisionTimeout = nullptr;
  // }
  //std::cerr << "Failing on normal path" << std::endl;
  pendingPhase1->pcb(pendingPhase1->decision, pendingPhase1->fast, pendingPhase1->conflict_flag,
      pendingPhase1->conflict, pendingPhase1->p1ReplySigs, eqv_ready);

  //Start FB for conflicts if we abstained twice or more in a row:
  if(pendingPhase1->decision == proto::ABORT && !pendingPhase1->conflict_flag){
    consecutive_abstains++;
  }
  else{
    consecutive_abstains = 0;
  }
  if(!params.no_fallback && consecutive_abstains >= consecutiveMax){
    for(auto txn: pendingPhase1->abstain_conflicts){
      //TODO: dont process redundant digests
      if(!TransactionsConflict(pendingPhase1->txn_, *txn)) continue;
      std::string txnDigest(TransactionDigest(*txn, params.hashDigest));
      pendingPhase1->ConflictCB(txnDigest, txn);
    }
  }

   //Lookup and delete.
   this->pendingPhase1s.erase(itr);
   delete pendingPhase1;
}

//XXX Use StopP1 to halt all P1 processing on the shard client.
void ShardClient::StopP1(uint64_t client_seq_num){
  auto itr = client_seq_num_mapping.find(client_seq_num);
  if(itr != client_seq_num_mapping.end()){
    PendingReqIds &pRids = itr->second;
    auto itrP1 = pendingPhase1s.find(pRids.pendingP1_id);
    if(itrP1 != pendingPhase1s.end()){
      delete itrP1->second;
      pendingPhase1s.erase(itrP1);
    }
  }
}


void ShardClient::HandleForwardWB(proto::ForwardWriteback &forwardWB){

  //TODO: verify signatures
  if(forwardWB.has_req_id()){ // normal case
    auto itrP1 = this->pendingPhase1s.find(forwardWB.req_id());

    if (itrP1 != this->pendingPhase1s.end()) {
      //upcall to client with Foward message.
      itrP1->second->fwb(forwardWB);
      return;
    }

    auto itrP2 = this->pendingPhase2s.find(forwardWB.req_id());

    if (itrP1 != this->pendingPhase1s.end()) {
      //upcall to client with Foward message.
      itrP2->second->fwb(forwardWB);
      return;
    }
  }
  else if(forwardWB.has_txn_digest()){ //fb case
    auto itrFB = pendingFallbacks.find(forwardWB.txn_digest());
    if(itrFB != pendingFallbacks.end()){
      //upcall to client with Forward message
      itrFB->second->fwb(forwardWB); //might want more info for FB WB
      return;
    }
  }



  return;

}

///////////////// Utility /////////////////////////


proto::Write *ShardClient::GetUnusedWrite() {
  std::unique_lock<std::mutex> lock(writeProtoMutex);
  proto::Write *write;
  if (writes.size() > 0) {
    write = writes.back();
    write->Clear();
    writes.pop_back();
  } else {
    write = new proto::Write();
  }
  return write;
}

proto::QueryReply *ShardClient::GetUnusedQueryReply() {
  std::unique_lock<std::mutex> lock(readProtoMutex);
  proto::QueryReply *reply;
  if (queryReplies.size() > 0) {
    reply = queryReplies.back();
    reply->Clear();
    queryReplies.pop_back();
  } else {
    reply = new proto::QueryReply();
  }
  return reply;
}


proto::ReadReply *ShardClient::GetUnusedReadReply() {
  std::unique_lock<std::mutex> lock(readProtoMutex);
  proto::ReadReply *reply;
  if (readReplies.size() > 0) {
    reply = readReplies.back();
    reply->Clear();
    readReplies.pop_back();
  } else {
    reply = new proto::ReadReply();
  }
  return reply;
}

proto::Phase1Reply *ShardClient::GetUnusedPhase1Reply() {
  std::unique_lock<std::mutex> lock(p1ProtoMutex);
  proto::Phase1Reply *reply;
  if (p1Replies.size() > 0) {
    reply = p1Replies.back();
    //reply->Clear(); //can move this to Free if want more work at threads
    p1Replies.pop_back();
  } else {
    reply = new proto::Phase1Reply();
  }
  return reply;
}

proto::Phase2Reply *ShardClient::GetUnusedPhase2Reply() {
  std::unique_lock<std::mutex> lock(p2ProtoMutex);
  proto::Phase2Reply *reply;
  if (p2Replies.size() > 0) {
    reply = p2Replies.back();
    //reply->Clear();
    p2Replies.pop_back();
  } else {
    reply = new proto::Phase2Reply();
  }
  return reply;
}


void ShardClient::FreeWrite(proto::Write *write) {
  std::unique_lock<std::mutex> lock(writeProtoMutex);
  //reply->Clear();
  writes.push_back(write);
}

void ShardClient::FreeReadReply(proto::ReadReply *reply) {
  std::unique_lock<std::mutex> lock(readProtoMutex);
  //reply->Clear();
  readReplies.push_back(reply);
}

void ShardClient::FreePhase1Reply(proto::Phase1Reply *reply) {
  std::unique_lock<std::mutex> lock(p1ProtoMutex);
  reply->Clear();
  p1Replies.push_back(reply);
}

void ShardClient::FreePhase2Reply(proto::Phase2Reply *reply) {
  std::unique_lock<std::mutex> lock(p2ProtoMutex);
  reply->Clear();
  p2Replies.push_back(reply);
}











/////////////////////////////////////////FALLBACK CODE STARTS HERE ///////////////////////////////////////////
void ShardClient::CleanFB(const std::string &txnDigest){
  auto itr = pendingFallbacks.find(txnDigest);
  if(itr != pendingFallbacks.end()){
    delete itr->second;
    pendingFallbacks.erase(itr);
  }
  //EraseRelay(txnDigest);
}

void ShardClient::EraseRelay(const std::string &txnDigest){
  pendingRelays.erase(txnDigest);
}

void ShardClient::HandlePhase1Relay(proto::RelayP1 &relayP1){

  std::string txnDigest(TransactionDigest(relayP1.p1().txn(), params.hashDigest));

  //only process the first relay for a txn.
  //if (!pendingRelays.insert(txnDigest).second) return; //USING this works? seemingly does not either.
  if(this->pendingFallbacks.find(txnDigest) != this->pendingFallbacks.end()) return;

  Debug("RelayP1[%lu][%s].", relayP1.dependent_id(),
      BytesToHex(txnDigest, 64).c_str());
  uint64_t req_id = relayP1.dependent_id();

  if(req_id != -1){ //this is a dep of an ongoing p1 request.
      auto itr = this->pendingPhase1s.find(req_id);
      if (itr == this->pendingPhase1s.end()) {
        return; // this is a stale request and no upcall is necessary!
      }

      Debug("RECEIVED RELAY P1[%s] at shardclient %d, for conflict txId %d", BytesToHex(txnDigest, 16).c_str(), group, itr->second->client_seq_num); //"for digest: " << BytesToHex(itr->second->txnDigest_, 64) << std::endl;
      itr->second->rcb(relayP1, txnDigest); //upcall to the registered relayP1 callback function.

  } else{ //this is a dep for a fallback request (i.e. a deeper depth)
      auto itr = this->pendingFallbacks.find(relayP1.dependent_txn());
      if (itr == this->pendingFallbacks.end()) {
        return; // this is a stale request and no upcall is necessary!
      }
      Debug("RECEIVED RELAY P1[%s] at shardclient %d, for FB conflict txn %s", BytesToHex(txnDigest, 16).c_str(), group, BytesToHex(itr->first, 16).c_str());
      itr->second->rcb(relayP1.dependent_txn(), relayP1, txnDigest); //upcall to the registered relayP1 callback function.
  }
}

//TODO: Move all callbacks (also in client.)
void ShardClient::Phase1FB(uint64_t reqId, proto::Transaction &txn, const std::string &txnDigest,
 relayP1FB_callback rP1FB, phase1FB_callbackA p1FBcbA, phase1FB_callbackB p1FBcbB,
 phase2FB_callback p2FBcb, writebackFB_callback wbFBcb, invokeFB_callback invFBcb, int64_t logGrp) {

  Debug("[group %i] Sending PHASE1FB [%lu]", group, client_id);
  //uint64_t reqId = lastReqId++;

  PendingFB* pendingFB = new PendingFB();
  pendingFallbacks[txnDigest] = pendingFB;

  PendingPhase1 *pendingPhase1 = new PendingPhase1(reqId, group, txn,
      txnDigest, config, keyManager, params, verifier, 0);
  pendingFB->pendingP1 = pendingPhase1;

  pendingFB->logGrp = logGrp;

  //set all callbacks
  //TODO: need to have relayP1 of its own in theory, to support deeper deps.
  pendingFB->rcb = rP1FB;
  pendingFB->wbFBcb = wbFBcb;
  pendingFB->p1FBcbA = p1FBcbA;
  pendingFB->p1FBcbB = p1FBcbB;
  pendingFB->p2FBcb = p2FBcb;
  pendingFB->invFBcb = invFBcb;

  // create prepare request
  phase1FB.Clear();
  phase1FB.set_req_id(reqId);
  *phase1FB.mutable_txn() = txn;

  transport->SendMessageToGroup(this, group, phase1FB);

  //pendingPhase1->requestTimeout->Reset();
}

// update pendingFB state -- if complete, upcall to client
void ShardClient::HandlePhase1FBReply(proto::Phase1FBReply &p1fbr){

  const std::string &txnDigest = p1fbr.txn_digest();
  Debug("Handling P1FBReply [%s] on group %d", BytesToHex(txnDigest, 16).c_str(), group);
  auto itr = this->pendingFallbacks.find(txnDigest);
  if (itr == this->pendingFallbacks.end()) {
    Debug("P1FBReply [%s] is stale.", BytesToHex(txnDigest, 16).c_str());
    return; // this is a stale request
  }


  PendingFB *pendingFB = itr->second;

  //CASE 1: Received a fully formed WB message. TODO: verify it.
  if(p1fbr.has_wb()){
    proto::Writeback wb = p1fbr.wb();
    //std::cerr << "group[" << group << "] triggered FastWriteback for txn: " << BytesToHex(txnDigest, 16) << std::endl;
    pendingFB->wbFBcb(wb);
    //CleanFB(txnDigest);
    return;
  }


//Update current views, since those might become necessary.
//TODO: move this after message verification? to save processing cost if not necessary to compute views?
//TODO: Currently verifying signature for p1 reply, p2 reply and view seperately, that is wasteful
//-> integrate current view into all the responses? Problem: Makes messages different.
  if(!params.all_to_all_fb && pendingFB->logGrp == group){
    //std::cerr << "[UpdateView] Process P1FBR for txn: " << BytesToHex(txnDigest, 16) << std::endl;
    UpdateViewStructure(pendingFB, p1fbr.attached_view());
  }
  //CASE 2: Received a p2 decision
  if(p1fbr.has_p2r()){
    proto::Phase2Reply p2r = p1fbr.p2r();
    Debug("      processing Phase1FBReply P2 for txn: %s on shardclient %d", BytesToHex(txnDigest, 16).c_str(), group);
    if(ProcessP2FBR(p2r, pendingFB, txnDigest)){  //--> this will invoke the Fallback if inconsistency observed
      return; //XXX only return if successful p2 callback, otherwise, also eval the p1 case
    }
  }
  //CASE 3: Received a p1 vote and still processing p1
  if(pendingFB->p1 && p1fbr.has_p1r()){
    Debug("      trying to process Phase1FBReply P1 for txn: %s on shardclient %d.", BytesToHex(txnDigest, 16).c_str(), group);
    proto::Phase1Reply reply = p1fbr.p1r();
    ProcessP1FBR(reply, pendingFB, txnDigest);
  }
}


void ShardClient::ProcessP1FBR(proto::Phase1Reply &reply, PendingFB *pendingFB, const std::string &txnDigest){

  ProcessP1R(reply, true, pendingFB, &txnDigest);
}


  void ShardClient::Phase1FBDecision(PendingFB *pendingFB) {

    //std::cerr << "Failing on FB path" << std::endl;

    pendingFB->p1 = false;
    PendingPhase1 *pendingPhase1 = pendingFB->pendingP1;
    Debug("Calling Phase1FB callbackA for txn: %s from shardclient %d", BytesToHex(pendingPhase1->txnDigest_, 16).c_str(), group);
    pendingFB->p1FBcbA(pendingPhase1->decision, pendingPhase1->fast, pendingPhase1->conflict_flag, pendingPhase1->conflict, pendingPhase1->p1ReplySigs);
    //pendingPhase1 needs to be deleted -->> happens in pendingFB destructor
  }

  void ShardClient::StopP1FB(std::string &txnDigest){
    auto itr = pendingFallbacks.find(txnDigest);
    if(itr != pendingFallbacks.end()){
      itr->second->p1 = false;
    }
  }

//version A) for p1 based Phase2.  grouped_sigs. //TODO:change callbacks.
  void ShardClient::Phase2FB(uint64_t id,
      const proto::Transaction &txn, const std::string &txnDigest,
      proto::CommitDecision decision,
      const proto::GroupedSignatures &groupedSigs) {

    Debug("[group %i] Sending PHASE2FB [%lu]", group, id);

    phase2FB.Clear();
    phase2FB.set_req_id(id);
    phase2FB.set_decision(decision);
    *phase2FB.mutable_txn_digest() = txnDigest;
    if (params.validateProofs && params.signedMessages) {
      *phase2FB.mutable_p1_sigs() = groupedSigs;
    }
    transport->SendMessageToGroup(this, group, phase2FB);


  }
  //version B) for p2 based Phase2. p2_replies
//OVERLOAD Phase2FB so it has the 2 cases
  void ShardClient::Phase2FB(uint64_t id,
      const proto::Transaction &txn, const std::string &txnDigest,
      proto::CommitDecision decision,
      const proto::P2Replies &p2Replies) {

    Debug("[group %i] Sending PHASE2FB [%lu]", group, id);

    phase2FB.Clear();
    phase2FB.set_req_id(id);
    phase2FB.set_decision(decision);
    *phase2FB.mutable_txn_digest() = txnDigest;
    if (params.validateProofs && params.signedMessages) {
      *phase2FB.mutable_p2_replies() = p2Replies;
    }
    transport->SendMessageToGroup(this, group, phase2FB);


  }

//TODO: release Signed Message and move pointers instead of copying!!!!!!
void ShardClient::UpdateViewStructure(PendingFB *pendingFB, const proto::AttachedView &ac){

  uint64_t stored_view;
  bool update = false;
  uint64_t id;
  uint64_t set_view;
  //TODO:: check whether txn_digest matches

  if (params.validateProofs && params.signedMessages) {
        if(!ac.has_signed_current_view()) return;
        proto::SignedMessage signed_msg = std::move(ac.signed_current_view());
        proto::CurrentView new_view;
        new_view.ParseFromString(signed_msg.data());

        //std::cerr << "Received view " << new_view.current_view() << " from replica: " << signed_msg.process_id() << std::endl;

        //only update data strucure if new view is bigger.
        if(pendingFB->current_views.find(signed_msg.process_id()) != pendingFB->current_views.end()){
          stored_view =  pendingFB->current_views[signed_msg.process_id()].view;
          if(new_view.current_view() <= stored_view) return;
        }

        // Check if replica ID in group. //TODO:: only need to do all this for the logging group.
        if(!IsReplicaInGroup(signed_msg.process_id(), group, config)) return;

        if(!verifier->Verify(keyManager->GetPublicKey(signed_msg.process_id()),
              signed_msg.data(), signed_msg.signature())) return;

        set_view = new_view.current_view();
        id = signed_msg.process_id();
        update = true;
        pendingFB->current_views[new_view.replica_id()].view = set_view;
        pendingFB->current_views[new_view.replica_id()].signed_view = std::move(signed_msg);


  } else{
    if(!ac.has_current_view()) return;
    proto::CurrentView new_view = ac.current_view();

    if(pendingFB->current_views.find(new_view.replica_id()) != pendingFB->current_views.end()){
        stored_view =  pendingFB->current_views[new_view.replica_id()].view;
        if(new_view.current_view() <= stored_view) return;
    }
    if(!IsReplicaInGroup(new_view.replica_id(), group, config)) return;
    uint64_t set_view = new_view.current_view();
    update=true;
    pendingFB->current_views[new_view.replica_id()].view = set_view;
    id = new_view.replica_id();
  }

  if(update){
    pendingFB->view_levels[stored_view].erase(id);
    if(pendingFB->view_levels[stored_view].size() == 0){
      pendingFB->view_levels.erase(stored_view);
    }
    pendingFB->view_levels[set_view].insert(id);

    //Dont do this here?
    // if(new_view.view() >= itr->second->max_view){
    //   ComputeMaxLevel(txnDigest);
    // }
    //logic to call Invoke callback if it was missing new views to invoked
    if(pendingFB->call_invokeFB){
      pendingFB->view_invoker();
    }
  }
}

void ShardClient::ComputeMaxLevel(PendingFB *pendingFB){

    std::map<uint64_t, std::set<uint64_t>>::reverse_iterator rit;
    uint64_t count = 0;

    for (rit=pendingFB->view_levels.rbegin(); rit != pendingFB->view_levels.rend(); ++rit){
      if(rit->first < pendingFB->max_view) return;
      if(count + rit->second.size() >= 3*config->f + 1){
        pendingFB->max_view = rit->first + 1;
        pendingFB->catchup = false;
        return;
      }
      else if(count+ rit->second.size() >= config->f +1){
        pendingFB->max_view = rit->first;
        pendingFB->catchup = true;
        return;
      }
      count += rit->second.size();

    }
  return;
}

void ShardClient::HandlePhase2FBReply(proto::Phase2FBReply &p2fbr){

  const std::string &txnDigest = p2fbr.txn_digest();
  auto itr = this->pendingFallbacks.find(txnDigest);
  if (itr == this->pendingFallbacks.end()) {
    Debug("[group %i] Received stale Phase2FBReply for txn %s.", group, BytesToHex(txnDigest, 16).c_str());
    return; // this is a stale request
  }
  PendingFB *pendingFB = itr->second;

//TODO: move this after message verification? to save processing cost if not necessary to compute views?
  if(!params.all_to_all_fb && pendingFB->logGrp == group){
      //std::cerr << "[UpdateView] Process P2FBR for txn: " << BytesToHex(txnDigest, 16) << std::endl;
      UpdateViewStructure(pendingFB, p2fbr.attached_view());
  }

  proto::Phase2Reply p2r = p2fbr.p2r();
  ProcessP2FBR(p2r, pendingFB, txnDigest); //, p2fbr.attached_view());

}


bool ShardClient::ProcessP2FBR(proto::Phase2Reply &reply, PendingFB *pendingFB, const std::string &txnDigest){ //, proto::AttachedView &view){

    const proto::Phase2Decision *p2Decision = nullptr;
    if (params.validateProofs && params.signedMessages) {
      if (!reply.has_signed_p2_decision()) {
        Debug("[group %i] Phase2FBReply missing signed_p2_decision.", group);
        return false;
      }
      if (!IsReplicaInGroup(reply.signed_p2_decision().process_id(), group, config)) {
        Debug("[group %d] Phase2FBReply from replica %lu who is not in group.",
            group, reply.signed_p2_decision().process_id());
        return false;
      }

      if(!verifier->Verify(keyManager->GetPublicKey(reply.signed_p2_decision().process_id()),
            reply.signed_p2_decision().data(), reply.signed_p2_decision().signature())) return false;

      if (!validatedP2Decision.ParseFromString(reply.signed_p2_decision().data())) {
        return false;
      }

      p2Decision = &validatedP2Decision;

    } else {
      p2Decision = &reply.p2_decision();
    }
    //if(!p2Decision->has_view()) return;
    proto::CommitDecision decision = p2Decision->decision();
    uint64_t view = p2Decision->view();
    uint64_t reqID = reply.req_id();

    Debug("[group %i] PHASE2FB reply with decision %d and view %lu", group,
        decision, view);
    //fprintf(stderr, "[group %i] PHASE2FB reply with decision %d and view %lu \n", group, decision, view);

    //that message is from likely obsolete views.
    if(pendingFB->max_decision_view > view + 1 ){
        return false;
    }

    bool delete_old_views = false;

    //update respective view/decision pendingP2 item.
    //TODO: make sure that each replica is only counted once. (dont want byz providing full quorum)
    PendingPhase2 &pendingP2 = pendingFB->pendingP2s[view][decision];
    pendingP2.reqId = reqID;
    pendingP2.decision = decision;
    if (params.validateProofs && params.signedMessages) {
      proto::Signature *sig = pendingP2.p2ReplySigs.add_sigs();
      sig->set_process_id(reply.signed_p2_decision().process_id());
      *sig->mutable_signature()= reply.signed_p2_decision().signature();
    }

    pendingP2.matchingReplies++;

    if(pendingP2.matchingReplies > config->f){
      if(view > pendingFB->max_decision_view){
            pendingFB->max_decision_view = view;
            delete_old_views = true;
      } //TODO: can just add the check for 3f+1 also, in which case we go to v + 1? Maybe not quite as trivial due to vote subsumption
    }

    //can return directly to writeback (p2 complete)
    if (pendingP2.matchingReplies == QuorumSize(config)) { //make it >=? potentially duplicate cb then..
      pendingFB->p2FBcb(pendingP2.decision, pendingP2.p2ReplySigs, view);
      //dont need to clean, will be cleaned by callback.

      // if(view > 0) std::cerr << "elected FB for view [" << view << "] for txn: " << BytesToHex(txnDigest, 16) <<std::endl;
      return true;
    }


    //XXX Fast case for completing p2 forwarding
    //XXX have to story full reply here because the decision views might differ.
   if(pendingFB->p1){
      uint64_t id = reply.signed_p2_decision().process_id();
      if(pendingFB->process_ids.find(id) == pendingFB->process_ids.end()){
        pendingFB->process_ids.insert(id);
        proto::Phase2Reply *new_item  = pendingFB->p2Replies[decision].add_p2replies();
        *new_item = reply;
      }
      proto::P2Replies &p2Replies = pendingFB->p2Replies[decision];
      if(p2Replies.p2replies().size() == config->f +1 ){
        pendingFB->p1 = false;
        Debug("Calling Phase1FB callbackB for txn: %s from shardclient %d", BytesToHex(txnDigest, 16).c_str(), group);
        if(!pendingFB->p1FBcbB(decision, p2Replies)) return true;
      }
    }

                          //XXX If I used this case, then Signatures suffice since decision views are the same
                          //XXX But it might prove impossible to arrive at this case.
                          //Otherwise, check if we are still doing p1 simultaneously
                          // if(pendingFB->p1){
                          //   if(pendingP2.matchingReplies == config->f +1 ){
                          //     pendingFB->p1 = false;
                          //     pendingFB->p1FBcbB(pendingP2.decision, pendingP2.p2ReplySigs);
                          //   }
                          // }

  ////FALLBACK INVOCATION
  //max decision view represents f+1 replicas. Implies that this is the current view.
  //CALL Fallback if detected divergence for newest accepted view. (calling it for older ones is useless)

  if(pendingFB->max_decision_view == view
    && pendingFB->pendingP2s[view][proto::COMMIT].matchingReplies >= config->f +1
    && pendingFB->pendingP2s[view][proto::ABORT].matchingReplies >= config->f +1){
      pendingFB->p1 = false;
      pendingFB->conflict_view = view;
      if(pendingFB->conflict_view + 1 > pendingFB->last_view){
        Debug("Calling InvokeFB for [txn: %s][view %lu]", BytesToHex(txnDigest, 16).c_str(), view);
        if(!pendingFB->invFBcb()) return true;
      }
  }
      //TODO: Also need to call it after some timeout. I.e. if 4f+1 received are all honest but diverge.

  ////////////Garbage collection
  if(delete_old_views){
                  //delete all entries for views < max_view -1. They are pretty much obsolete.
                  //reasoning: if received f+1 for max view, then 2f+1 correct are in view >= max view -1 --> cant receive Quorum anymore. (still possible to receive a few outstanding ones)
    std::map<uint64_t, std::map<proto::CommitDecision, PendingPhase2>>::iterator it;
    for (it=pendingFB->pendingP2s.begin(); it != pendingFB->pendingP2s.end(); it++){
      if(it->first +1 >= pendingFB->max_decision_view){
        break;
      }
      else{
        pendingFB->pendingP2s.erase(it->first);
      }
    }
  }
  return false;
  ///////////END
}

void ShardClient::InvokeFB(uint64_t conflict_id, std::string &txnDigest, proto::Transaction &txn, proto::CommitDecision decision, proto::P2Replies &p2Replies){

  //TODO: start a timeout. If cannot receive enough P2FB replies from a view v, then invoke again.
  //TODO: do not send invokeFB requests for higher views than the "conflict-view" (i.e. view with inconsistent decisions)
          // before some timeout has expired. Otherwise one is needlessly pushing for elections (server should schedule them for post-time-out in any case)

  auto itr = this->pendingFallbacks.find(txnDigest);
  if(itr == this->pendingFallbacks.end()) return;
  PendingFB *pendingFB = itr->second;

  if(params.all_to_all_fb){
      //TODO: might not need to send this p2 always..
      phase2FB.Clear();
      phase2FB.set_req_id(conflict_id);
      phase2FB.set_decision(decision);
      phase2FB.set_txn_digest(txnDigest);
      *phase2FB.mutable_txn() = txn;
      *phase2FB.mutable_p2_replies() = p2Replies;

      invokeFB.Clear();
      invokeFB.set_req_id(conflict_id);
      invokeFB.set_txn_digest(txnDigest);
      *invokeFB.mutable_p2fb() = std::move(phase2FB); //XXX assuming FIFO channels, including the p2 is not necessary since it will already have been sent.

      transport->SendMessageToGroup(this, group, invokeFB);
      Debug("[group %i] Sent InvokeFB[%lu]", group, client_id);
      std::cerr << "Invoking on all to all FB" << std::endl;
  }

  else{
      ComputeMaxLevel(pendingFB); //Find max_view that we can propose.
      uint64_t &proposed_view = pendingFB->max_view;

      //std::cerr << "Called InvokeFB for view: " << pendingFB->max_view << " for txn: " << BytesToHex(txnDigest, 16) << std::endl;
      if(pendingFB->max_view > pendingFB->conflict_view + 1){
        proposed_view = pendingFB->conflict_view + 1;
        //TODO: Add timeout before electing the next one.

        //Panic("Proposing view %lu that is higher than our conflict view %lu.", pendingFB->max_view, pendingFB->conflict_view);
      }

      if(proposed_view <= pendingFB->last_view){
        pendingFB->call_invokeFB = true;
        pendingFB->view_invoker = std::move(std::bind(&ShardClient::InvokeFB, this, conflict_id, txnDigest, txn, decision, std::ref(p2Replies)));
        return; //Call only later, we already invoked for this view (or a larger one)
      }

      pendingFB->call_invokeFB = false;
      pendingFB->last_view = proposed_view;


        proto::SignedMessages view_signed;
        uint64_t count;
        if(pendingFB->catchup){
          count = config->f+1;
        }
        else{
          count = 3*config->f +1;
        }
        std::map<uint64_t, std::set<uint64_t>>::reverse_iterator rit;
        for (rit=pendingFB->view_levels.rbegin(); rit != pendingFB->view_levels.rend(); ++rit){
          for(auto id: rit->second){
            SignedView &sv = pendingFB->current_views[id];
            proto::SignedMessage* sm = view_signed.add_sig_msgs();
            *sm = sv.signed_view;

            count--;
            if(count == 0) break;
          }
          if(count == 0) break;
        }

        //TODO: might not need to send this p2 always..
        phase2FB.Clear();
        phase2FB.set_req_id(conflict_id);
        phase2FB.set_decision(decision);
        phase2FB.set_txn_digest(txnDigest);
        *phase2FB.mutable_txn() = txn;
        *phase2FB.mutable_p2_replies() = p2Replies;

        invokeFB.Clear();
        invokeFB.set_req_id(conflict_id);
        invokeFB.set_txn_digest(txnDigest);
        *invokeFB.mutable_p2fb() = std::move(phase2FB); //XXX assuming FIFO channels, including the p2 is not necessary since it will already have been sent.
        invokeFB.set_proposed_view(proposed_view);
        *invokeFB.mutable_view_signed() = std::move(view_signed);

        transport->SendMessageToGroup(this, group, invokeFB);
        Debug("[group %i] Sent InvokeFB[%lu]", group, client_id);
  }
}

void ShardClient::HandleSendViewMessage(proto::SendView &sendView){
  const std::string &txnDigest = sendView.txn_digest();
  auto itr = this->pendingFallbacks.find(txnDigest);
  if (itr == this->pendingFallbacks.end()) {
    Debug("[group %i] Received stale Phase2FBReply for txn %s.", group, txnDigest.c_str());
    return; // this is a stale request
  }
  PendingFB *pendingFB = itr->second;

//TODO: move this after message verification? to save processing cost if not necessary to compute views?
  if(!params.all_to_all_fb && pendingFB->logGrp == group){
      //std::cerr << "[UpdateView] Process SendView for txn: " << BytesToHex(txnDigest, 16) << std::endl;
      UpdateViewStructure(pendingFB, sendView.attached_view());
  }
}

void ShardClient::WritebackFB_fast(std::string txnDigest, proto::Writeback &wb) {

  transport->SendMessageToGroup(this, group, wb);
  Debug("[group %i] Sent FB-WRITEBACK[%lu]", group, client_id);

  // Delete PendingFB instance.  //TODO: delete dependents of instance as well (if we support more than depth 1)
  CleanFB(txnDigest);

}


} // namespace indicus
