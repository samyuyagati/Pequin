// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
#ifndef _JANUS_SERVER_H_
#define _JANUS_SERVER_H_

#include "replication/common/replica.h"
#include "store/server.h"

#include "store/janusstore/store.h"
#include "store/janusstore/transaction.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/janusstore/janus-proto.pb.h"

namespace janusstore {

class Server : public replication::AppReplica, public ::Server {
public:
    Server();
    virtual ~Server();

    // Invoke callback on the leader, with the option to replicate on success
    void LeaderUpcall(opnum_t opnum, const string &str1, bool &replicate, string &str2) { return; };

    // Invoke callback on all replicas
    void ReplicaUpcall(opnum_t opnum, const string &str1, string &str2) { return; };

    // Invoke call back for unreplicated operations run on only one replica
    // This will match on a RequestMessage and call a private handler function
    void UnloggedUpcall(const string &str1, string &str2);

    // TODO only need this if we are extending store/server.h, but i dont think
    // this is necessary
    void Load(const string &key, const string &value, const Timestamp timestamp);

private:
    // simple key-value store
    Store *store;

    // highest ballot accepted per txn id
    std::unordered_map<uint64_t, uint64_t> accepted_ballots;

    // maps Transaction ids in the graph to ancestor Transaction ids
    std::unordered_map<uint64_t, std::vector<uint64_t>> dep_map;

    // maps Transaction ids to Transcation objects
    std::unordered_map<uint64_t, Transaction> id_txn_map;

    // maps Txn to locally processed status
    std::unordered_map<uint64_t, bool> processed;

    // maps keys to transaction ids that read it
    // TODO ensure that this map is cleared per transaction
    std::unordered_map<string, std::vector<uint64_t>> read_key_txn_map;

    // maps keys to transaction ids that write to it
    // TODO ensure that this map is cleared per transaction
    std::unordered_map<string, std::vector<uint64_t>> write_key_txn_map;

    // maps txn_id -> list[other_ids] being blocked by txn_id
    std::unordered_map<uint64_t, std::vector<uint64_t>> blocking_ids;

    // functions to process shardclient requests
    // must take in a full Transaction object in order to correctly bookkeep and commit

    // returns the list of dependencies for given txn, NULL if PREACCEPT-NOT-OK
    std::vector<uint64_t> HandlePreAccept(Transaction txn, uint64_t ballot);

    // returns -1 if Accept-OK, the highest ballot for txn otherwise
    uint64_t HandleAccept(Transaction &txn, std::vector<std::uint64_t> msg_deps, uint64_t ballot);

    // TODO figure out what T.abandon and T.result are
    void HandleCommit(uint64_t txn_id, std::vector<std::uint64_t> deps);

    std::unordered_map<string, string> WaitAndInquire(uint64_t txn_id);
    std::unordered_map<string, string> _ExecutePhase(uint64_t txn_id);
    std::vector<uint64_t> _StronglyConnectedComponent(uint64_t txn_id);
    bool _ReadyToProcess(Transaction txn);

    // TODO determine the return type??
    std::unordered_map<string, string> Execute(Transaction txn);
    // for cyclic dependency case, compute SCCs and execute in order
    // to be called during the Commit phase from HandleCommitJanusTxn()
    void ResolveContention(std::vector<uint64_t> scc);
};
} // namespace janusstore

#endif /* _JANUS_SERVER_H_ */
