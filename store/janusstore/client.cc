// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
#include "store/janusstore/client.h"

namespace janusstore {

  using namespace std;
  using namespace proto;

  Client::Client(const string configPath, int nShards, int closestReplica, Transport * transport): nshards(nShards), transport(transport) {
    // initialize a random client ID
    client_id = 0;
    while (client_id == 0) {
      random_device rd;
      mt19937_64 gen(rd());
      uniform_int_distribution <uint64_t> dis;
      client_id = dis(gen);
    }

    // for now, it does not seem like we need txn_id or ballot
    // MSB = client_id, LSB = txn num
    txn_id = (client_id / 10000) * 10000;
    // MSB = client_id, LSB = ballot num
    ballot = (client_id / 10000) * 10000;

    bclient.reserve(nshards);
    Debug("Initializing Janus client with id [%llu] %llu", client_id, nshards);

    /* Start a client for each shard. */
    for (int i = 0; i < nshards; i++) {
      string shardConfigPath = configPath + to_string(i) + ".config";
      ShardClient * shardclient = new ShardClient(shardConfigPath,
        transport, client_id, i, closestReplica);
      // we use shardclients instead of bufferclients here
      bclient[i] = shardclient;
    }

    Debug("Janus client [%llu] created! %llu %lu", client_id, nshards, bclient.size());
  }

  Client::~Client() {
    // TODO delete the maps too?
    for (auto b: bclient) {
      delete b;
    }
  }

  uint64_t Client::keyToShard(string key, uint64_t nshards) {
    // default partition function from store/common/partitioner.cc
    uint64_t hash = 5381;
    const char * str = key.c_str();
    for (unsigned int i = 0; i < key.length(); i++) {
      hash = ((hash << 5) + hash) + (uint64_t) str[i];
    }
    return (hash % nshards);
  }

  void Client::setParticipants(Transaction * txn) {
    participants.clear();
    for (const auto & key: txn->read_set) {
      int i = this->keyToShard(key, nshards);
      if (participants.find(i) == participants.end()) {
        participants.insert(i);
      }
    }

    for (const auto & pair: txn->write_set) {
      int i = this->keyToShard(pair.first, nshards);
      if (participants.find(i) == participants.end()) {
        participants.insert(i);
      }
    }
  }

  void Client::PreAccept(Transaction * txn, uint64_t ballot, output_commit_callback ocb) {
    printf("preaccept here\r\n");
    uint64_t txn_id = txn->getTransactionId();
    has_fast_quorum[txn_id] = false;
    printf("here\n");
    this->output_commits[txn_id] = ocb;
    /*
    if (!txn) {
      ocb(0);
      return;
    } else {
      for (const auto &key: txn->read_set) {
        printf("%s\n", key.c_str());
      }
      for (const auto &pair: txn->write_set) {
        printf("%s %s\n", pair.first.c_str(), pair.second.c_str());
      }
      ocb(0);
      return;
    }
    */
    printf("(here)\n");
    txn->setTransactionId(txn_id);
    txn_id++;
    setParticipants(txn);


    // add the callback to map for post-commit action
    this->output_commits[txn->getTransactionId()] = ocb;

    for (auto p: participants) {
      auto pcb = std::bind(&Client::PreAcceptCallback, this,
        txn_id, placeholders::_1, placeholders::_2);

      bclient[p]->PreAccept(*txn, ballot, pcb);
    }
  }

  void Client::Accept(uint64_t txn_id, set <uint64_t> deps, uint64_t ballot) {
    for (auto p: participants) {
      std::vector<uint64_t> vec_deps(deps.begin(), deps.end());
      auto acb = std::bind(&Client::AcceptCallback, this, txn_id, placeholders::_1, placeholders::_2);

      bclient[p]->Accept(txn_id, vec_deps, ballot, acb);
    }
  }

  void Client::Commit(uint64_t txn_id, set<uint64_t> deps) {
    for (auto p: participants) {
      std::vector<uint64_t> vec_deps(deps.begin(), deps.end());
      auto ccb = std::bind(&Client::CommitCallback, this, txn_id, placeholders::_1, placeholders::_2);

      bclient[p]->Commit(txn_id, vec_deps, ccb);
    }
  }

  void Client::PreAcceptCallback(uint64_t txn_id, int shard, std::vector<janusstore::proto::Reply> replies) {

    /* shardclient invokes this when all replicas in a shard have responded */

    // update responded shards
    responded.insert(shard);

    // check if each replica within shard has the same dependencies
    // then aggregate dependencies
    bool fast_quorum = true;
    bool has_replica_deps = false;
    std::unordered_set<uint64_t> replica_deps;
    for (auto reply: replies) {
      std::unordered_set<uint64_t> current_replica_deps;
      if (reply.op() == Reply::PREACCEPT_OK) {
        // parse message for deps
        DependencyList msg = reply.preaccept_ok().dep();
        for (int i = 0; i < msg.txnid_size(); i++) {
          uint64_t dep_id = msg.txnid(i);
          // add dep to aggregated set
          this->aggregated_deps[txn_id].insert(dep_id);
          current_replica_deps.insert(dep_id);
        }
        if (has_replica_deps) {
          // check equality with current_replica_deps
          fast_quorum = fast_quorum && (current_replica_deps == replica_deps);
        } else {
          replica_deps = current_replica_deps;
          has_replica_deps = true;
        }
      } else {
        // meaning we will need to go to Accept phase
        fast_quorum = false;
      }
    }

    this->has_fast_quorum[txn_id] = has_fast_quorum[txn_id] && fast_quorum;
    // if all shards have responded, move onto Commit stage; else Accept stage
    if (responded.size() == participants.size()) {
      // check whether we have a fast quorum before doing commit
      responded.clear();

      if (fast_quorum) {
        Commit(txn_id, this->aggregated_deps[txn_id]);
      } else {
        this->ballot++;
        Accept(txn_id, this->aggregated_deps[txn_id], this->ballot);
      }
    }
  }

  void Client::AcceptCallback(uint64_t txn_id, int shard, std::vector<janusstore::proto::Reply> replies) {

    /* shardclient invokes this when all replicas in a shard have responded */

    responded.insert(shard);
    for (auto reply: replies) {
      if (reply.op() == Reply::ACCEPT_NOT_OK) {
        // if majority not okay, then goto failure recovery (not supported)
      }
    }

    if (responded.size() == participants.size()) {
      // no need to check for a quorum for every shard because we dont implement failure recovery
      responded.clear();
      Commit(txn_id, this->aggregated_deps[txn_id]);
    }
    return;
  }
  void Client::CommitCallback(uint64_t txn_id, int shard, std::vector<janusstore::proto::Reply> replies) {

    /* shardclient invokes this when all replicas in a shard have responded */

    responded.insert(shard);

    if (responded.size() == participants.size()) {
      // return results to client
      // TODO how to return results? may also need to update CommitOKMessage
      // for (auto reply : replies) {}
      // invoke output commit callback
      this->output_commits[txn_id](txn_id);
    }
    responded.clear();
    return;
  }
}
