#include "store/apps/simple_app.h"

int main(int argc, char **argv) {
    // Construct the client
    Client client = new indicusstore::Client(config, clientId,
                                             FLAGS_num_shards,
                                             FLAGS_num_groups, closestReplicas, FLAGS_ping_replicas, 
                                             tport, part,
                                             FLAGS_tapir_sync_commit, readMessages, readQuorumSize,
                                             params, keyManager, FLAGS_indicus_phase1DecisionTimeout,
											 FLAGS_indicus_max_consecutive_abstains,
											 TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
    SyncClient syncClient = new SyncClient(client);
    uint32_t timeout = 30; // no idea what a reasonable value is

    // Do a simple series of transactions
    std::thread clientThread = new std::thread([syncClient, timeout]() {
        syncClient.Begin(timeout);
        syncClient.Put("x", "5", timeout);
        std::string readValue = syncClient.Get("x", timeout);
        syncClient.Commit(timeout);
    });
}