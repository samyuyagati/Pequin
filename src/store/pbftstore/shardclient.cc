#include "store/pbftstore/shardclient.h"
#include "store/pbftstore/common.h"

namespace pbftstore {

ShardClient::ShardClient(const transport::Configuration& config, Transport *transport,
    uint64_t group_idx,
    bool signMessages, bool validateProofs,
    KeyManager *keyManager) :
    config(config), transport(transport),
    group_idx(group_idx),
    signMessages(signMessages), validateProofs(validateProofs),
    keyManager(keyManager) {
  transport->Register(this, config, -1, -1);
  readReq = 0;
}

ShardClient::~ShardClient() {}

bool ShardClient::validateReadProof(const proto::CommitProof& commitProof, const std::string& key,
  const std::string& value, const Timestamp& timestamp) {
    // Commit proof is proof that key -> value was prepared on the shard, not that it was written

    //TODO
    return true;
  }

void ShardClient::ReceiveMessage(const TransportAddress &remote,
    const std::string &t, const std::string &d,
    void *meta_data) {
      std::cout << "handling message of type " << t << std::endl;
  proto::SignedMessage signedMessage;
  std::string type;
  std::string data;
  if (t == signedMessage.GetTypeName()) {
    if (!signedMessage.ParseFromString(d)) {
      return;
    }

    if (!ValidateSignedMessage(signedMessage, keyManager, data, type)) {
      return;
    }
  } else {
    type = t;
    data = d;
  }

  proto::ReadReply readReply;
  proto::TransactionDecision transactionDecision;
  proto::GroupedDecisionAck groupedDecisionAck;
  if (type == readReply.GetTypeName()) {
    readReply.ParseFromString(data);
    // get the read request id from the reply
    uint64_t reqId = readReply.req_id();
    printf("got a read reply\n");

    // try and find a matching pending read based on the request
    if (pendingReads.find(reqId) != pendingReads.end()) {
      PendingRead* pendingRead = &pendingReads[reqId];
      // we always mark a reply even if it fails because the read could fail
      if (signMessages) {
        if (t == signedMessage.GetTypeName()) {
          // insert the signed replica id as a received reply
          pendingRead->receivedReplies.insert(signedMessage.replica_id());
        } else {
          return;
        }
      } else {
        // insert a new id into the received replies
        pendingRead->receivedReplies.insert(pendingRead->receivedReplies.size());
      }
      if (readReply.status() == REPLY_OK) {
        Timestamp rts(readReply.value_timestamp());
        if (validateProofs) {
          // if the proof is invalid, stop processing this reply
          if (!validateReadProof(readReply.commit_proof(), readReply.key(), readReply.value(), rts)) {
            return;
          }
        }
        // if we haven't recorded a read result yet or we have a higher read,
        // make this reply the new max
        if (pendingRead->status == REPLY_FAIL || rts > pendingRead->maxTs) {
            pendingRead->maxTs = rts;
            pendingRead->maxValue = readReply.value();
            pendingRead->maxCommitProof = readReply.commit_proof();
            pendingRead->status = REPLY_OK;
        }
      }

      printf("reply size: %d\n", pendingRead->receivedReplies.size());
      if (pendingRead->receivedReplies.size() >= pendingRead->numResultsRequired) {
        read_callback rcb = pendingRead->rcb;
        std::string value = pendingRead->maxValue;
        Timestamp readts = pendingRead->maxTs;
        std::string key = readReply.key();
        uint64_t status = pendingRead->status;
        pendingReads.erase(reqId);
        rcb(status, key, value, readts);
      }
    }


  } else if (type == transactionDecision.GetTypeName()) {
    transactionDecision.ParseFromString(data);
    std::string digest = transactionDecision.txn_digest();
    // NOTE: makes the assumption that numshards == numgroups
    if (transactionDecision.shard_id() == group_idx) {
      if (signMessages) {
        // make sure the message was signed
        if (t == signedMessage.GetTypeName()) {
          if (pendingSignedPrepares.find(digest) != pendingSignedPrepares.end()) {
            printf("Adding signed id to set: %d\n", signedMessage.replica_id());

            PendingSignedPrepare* psp = &pendingSignedPrepares[digest];
            uint64_t add_id = signedMessage.replica_id();
            psp->receivedDecs.insert(add_id);
            if (transactionDecision.status() == REPLY_OK) {
              // add the decision to the list as proof
              psp->receivedValidDecs[add_id] = signedMessage;
            }
            proto::GroupedSignedDecisions groupedSD;
            // once we have enough valid requests, construct the grouped decision
            // and return success
            if (psp->receivedValidDecs.size() >= (uint64_t) config.f + 1) {
              for (const auto& pair : psp->receivedValidDecs) {
                proto::SignedMessage* sm = groupedSD.add_decisions();
                *sm = pair.second;
              }

              // invoke the callback with the signed grouped decision
              signed_prepare_callback pcb = psp->pcb;
              pendingSignedPrepares.erase(digest);
              pcb(REPLY_OK, groupedSD);
              return;
            }
            // once it is impossible for there to be f+1 valid responses (# invalid responses > f+1)
            // return failure
            if (((int) psp->receivedDecs.size()) - ((int) psp->receivedValidDecs.size()) >= config.f + 1) {
              signed_prepare_callback pcb = psp->pcb;
              pendingSignedPrepares.erase(digest);
              pcb(REPLY_FAIL, groupedSD);
              return;
            }
          }
        }
      } else {
        if (pendingPrepares.find(digest) != pendingPrepares.end()) {
          PendingPrepare* pp = &pendingPrepares[digest];
          uint64_t add_id = pp->receivedDecs.size();
          pp->receivedDecs.insert(add_id);
          if (transactionDecision.status() == REPLY_OK) {
            // add the decision to the list as proof
            pp->receivedValidDecs[add_id] = transactionDecision;
          }
          proto::GroupedDecisions groupedD;
          // once we have enough valid requests, construct the grouped decision
          // and return success
          if (pp->receivedValidDecs.size() >= (uint64_t) config.f + 1) {
            // once we have enough, construct a signed grouped decision
            for (const auto& pair : pp->receivedValidDecs) {
              proto::TransactionDecision* td = groupedD.add_decisions();
              *td = pair.second;
            }
            // invoke the callback if we have enough of the same decision
            prepare_callback pcb = pp->pcb;
            pendingPrepares.erase(digest);
            pcb(REPLY_OK, groupedD);
            return;
          }
          // once it is impossible for there to be f+1 valid responses (# invalid responses > f+1)
          // return failure
          if (((int) pp->receivedDecs.size()) - ((int) pp->receivedValidDecs.size()) >= config.f + 1) {
            prepare_callback pcb = pendingPrepares[digest].pcb;
            pendingPrepares.erase(digest);
            pcb(REPLY_FAIL, groupedD);
            return;
          }
        }
      }
    }

  } else if (type == groupedDecisionAck.GetTypeName()) {
    groupedDecisionAck.ParseFromString(data);
    std::string digest = groupedDecisionAck.txn_digest();
    if (groupedDecisionAck.status() == REPLY_OK && pendingWritebacks.find(digest) != pendingWritebacks.end()) {
      std::unordered_set<uint64_t> *receivedAcks = &pendingWritebacks[digest].receivedAcks;
      // add the replica id that sent to ack to the list
      if (signMessages) {
        if (t == signedMessage.GetTypeName()) {
          printf("got a decision ack from %d\n", signedMessage.replica_id());
          receivedAcks->insert(signedMessage.replica_id());
        }
      } else {
        receivedAcks->insert(receivedAcks->size());
      }
      // 2f+1 because we want a quorum of honest users to acknowledge (for fault tolerance)
      if (receivedAcks->size() >= (uint64_t) 2*config.f + 1) {
        // if the list has enough replicas, we can invoke the callback
        writeback_callback wcb = pendingWritebacks[digest].wcb;
        pendingWritebacks.erase(digest);
        wcb();
      }
    }
  }
}

// Get the value corresponding to key.
void ShardClient::Get(const std::string &key, const Timestamp &ts,
    uint64_t numResults, read_callback gcb, read_timeout_callback gtcb,
    uint32_t timeout) {

  proto::Read read;
  uint64_t reqId = readReq++;
  read.set_req_id(reqId);
  read.set_key(key);
  ts.serialize(read.mutable_timestamp());

  transport->SendMessageToGroup(this, group_idx, read);
  PendingRead pr;
  pr.rcb = gcb;
  pr.numResultsRequired = numResults;
  pr.status = REPLY_FAIL;
  // every ts should be bigger than this one
  pr.maxTs = Timestamp();

  pendingReads[reqId] = pr;

  // TODO timeout
}

// send a request with this as the packed message
void ShardClient::Prepare(const proto::Transaction& txn, prepare_callback pcb,
    prepare_timeout_callback ptcb, uint32_t timeout) {

  std::string digest = TransactionDigest(txn);
  if (pendingPrepares.find(digest) == pendingPrepares.end()) {
    proto::Request request;
    request.set_digest(digest);
    request.mutable_packed_msg()->set_msg(txn.SerializeAsString());
    request.mutable_packed_msg()->set_type(txn.GetTypeName());

    transport->SendMessageToGroup(this, group_idx, request);

    PendingPrepare pp;
    pp.pcb = pcb;
    pendingPrepares[digest] = pp;

    // TODO timeout
  } else {
    // TODO warning
  }
}

void ShardClient::SignedPrepare(const proto::Transaction& txn, signed_prepare_callback pcb,
    prepare_timeout_callback ptcb, uint32_t timeout) {
      std::string digest = TransactionDigest(txn);
  if (pendingSignedPrepares.find(digest) == pendingSignedPrepares.end()) {
    proto::Request request;
    request.set_digest(digest);
    request.mutable_packed_msg()->set_msg(txn.SerializeAsString());
    request.mutable_packed_msg()->set_type(txn.GetTypeName());

    transport->SendMessageToGroup(this, group_idx, request);

    PendingSignedPrepare psp;
    psp.pcb = pcb;
    pendingSignedPrepares[digest] = psp;

    // TODO timeout
  } else {
    // TODO warning
  }
}

void ShardClient::Commit(const std::string& txn_digest, const proto::ShardDecisions& dec,
    writeback_callback wcb, writeback_timeout_callback wtcp, uint32_t timeout) {
  if (pendingWritebacks.find(txn_digest) == pendingWritebacks.end()) {
    proto::GroupedDecision groupedDecision;
    groupedDecision.set_status(REPLY_OK);
    groupedDecision.set_txn_digest(txn_digest);
    *groupedDecision.mutable_decisions() = dec;

    transport->SendMessageToGroup(this, group_idx, groupedDecision);

    PendingWritebackReply pwr;
    pwr.wcb = wcb;
    pendingWritebacks[txn_digest] = pwr;

    // TODO timeout
  } else {
    // TODO warning
  }
}

void ShardClient::CommitSigned(const std::string& txn_digest, const proto::ShardSignedDecisions& dec,
    writeback_callback wcb, writeback_timeout_callback wtcp, uint32_t timeout) {
  if (pendingWritebacks.find(txn_digest) == pendingWritebacks.end()) {
    proto::GroupedDecision groupedDecision;
    groupedDecision.set_status(REPLY_OK);
    groupedDecision.set_txn_digest(txn_digest);
    *groupedDecision.mutable_signed_decisions() = dec;

    transport->SendMessageToGroup(this, group_idx, groupedDecision);

    PendingWritebackReply pwr;
    pwr.wcb = wcb;
    pendingWritebacks[txn_digest] = pwr;

    // TODO timeout
  } else {
    // TODO warning
  }
}

void ShardClient::Abort(std::string txn_digest, writeback_callback wcb, writeback_timeout_callback wtcp,
    uint32_t timeout) {
  if (pendingWritebacks.find(txn_digest) == pendingWritebacks.end()) {
    proto::GroupedDecision groupedDecision;
    groupedDecision.set_status(REPLY_FAIL);
    groupedDecision.set_txn_digest(txn_digest);
    proto::ShardDecisions sd;
    *groupedDecision.mutable_decisions() = sd;

    transport->SendMessageToGroup(this, group_idx, groupedDecision);

    PendingWritebackReply pwr;
    pwr.wcb = wcb;
    pendingWritebacks[txn_digest] = pwr;

    // TODO timeout
  } else {
    // TODO warning
  }
}

}
