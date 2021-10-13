#include "store/apps/simple_app.h"

int main(int argc, char **argv) {
    // All values have been set to default values of corresponding
    // flags, which are prescribed in benchmark.cc

    // Shard config parameters
    transport::Configuration *config;
    std::string config_path = "shard-r0.config";
    std::ifstream configStream(config_path);
    if (configStream.fail()) {
        std::cerr << "Unable to read configuration file: " << config_path
                << std::endl;
        return -1;
    }
    config = new transport::Configuration(configStream);

    // TCP transport (next to last arg is indicus_hyper_threading; default val in benchmark.cc is true)
    TCPTransport* tport;
    tport = new TCPTransport(0.0, 0.0, 0, false, 0, 1, true, false);

    // key manager: first arg is indicus_key_path; "keys" assumes this is run inside src dir
    KeyManager* keyManager;
    keyMananger = new KeyManager("keys", 4, true);

    // Partitioner (presumably for sharding?)
    Partitioner *part;
    part = new DefaultPartitioner();

    // Failure parameters for injected failure (?)
    indicusstore::InjectFailure failure;
    indicusstore::InjectFailure failure;
    failure.type = indicusstore::InjectFailureType::CLIENT_EQUIVOCATE;
    failure.timeMs = rand() % 100; //offset client failures a bit.
	failure.enabled = 1 * 1 + 0 < floor(1 * 1 * 0 / 100); // Failure not enabled.
	std::cerr << "client_id = " << 0 << "thread_id = " << 1 << ". Failure enabled: "<< failure.enabled <<  std::endl;
	failure.frequency = 0; // 100 // indicus_inject_failure_freq;

    // Various parameters
    indicusstore::Parameters params(true, // indicus_sign_messages
                                    true, false, // indicus_validate_proofs, indicus_hash_digest
                                    true, 2, // indicus_verify_deps, indicus_sig_batch
                                    -1, // indicus_max_dep_depth 
                                    config->f + 1, // readDepSize
									false, false,
									false, false,
                                    2, // FLAGS_indicus_merkle_branch_factor
                                    failure,
                                    true, // indicus_multi_threading: not default 
                                          // (dispatch crypto to parallel threads)
                                    false, // indicus_batch_verification
									64, // indicus_batch_verification_size
									false,
									false,
									false,
									true, // indicus_parallel_CCC
									false,
									false, // indicus_all_to_all_fb
									false, // indicus_no_fallback,
									1, // indicus_relayP1_timeout,
									false);

    // Construct the client
    std::vector<int> closestReplicas;
    Client client = new indicusstore::Client(*config, // config object 
                                             (uint64_t) 0, // client id
                                             1, // number of shards
                                             1, // num groups
                                             closestReplicas, // closestReplicas (start w/ single server; haven't constructed add'l replicas)
                                             false, // ping_replicas
                                             *tport, 
                                             *part,
                                             true, // tapir_sync_commit 
                                             (uint64_t) 0, // readMessages: should be uint64_t, might be 0?
                                             (uint64_t) 1, // readQuorumSize: should be uint64_t; if readMessages is 0, this should be 1.
                                             params, 
                                             *keyManager, 
                                             (uint64_t) 1000, // indicus_phase1DecisionTimeout: should be uint64_t
											 (uint64_t) 1, // indicus_max_consecutive_abstains,
											 TrueTime(0, 0)); // clock_skew, clock_error

    // indicuststore::Client constructor signature (for reference)
    /* Client(transport::Configuration *config, uint64_t id, int nShards,
    int nGroups,
    const std::vector<int> &closestReplicas, bool pingReplicas, Transport *transport,
    Partitioner *part, bool syncCommit, uint64_t readMessages,
    uint64_t readQuorumSize, Parameters params,
    KeyManager *keyManager, uint64_t phase1DecisionTimeout, uint64_t consecutiveMax, TrueTime timeServer)
    */

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

