// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/tpccClient.cc:
 *   Benchmarking client for tpcc.
 *
 **********************************************************************/

#include "lib/latency.h"
#include "lib/timeval.h"
#include "lib/tcptransport.h"
#include "store/common/truetime.h"
#include "store/common/frontend/async_client.h"
#include "store/common/frontend/async_adapter_client.h"
#include "store/strongstore/client.h"
#include "store/weakstore/client.h"
#include "store/tapirstore/client.h"
#include "store/janusstore/client.h"
#include "store/benchmark/async/bench_client.h"
#include "store/benchmark/async/common/key_selector.h"
#include "store/benchmark/async/common/uniform_key_selector.h"
#include "store/benchmark/async/retwis/retwis_client.h"
#include "store/benchmark/async/tpcc/tpcc_client.h"

#include <gflags/gflags.h>

#include <vector>
#include <algorithm>

#define N 1000
void SendTxn(janusstore::Client *client, size_t *sent) {
	commit_callback ccb = [client, sent] (uint64_t committed) {
		// TODO (andy): what is this for?s
		// if (*sent < N) {
		// 	SendTxn(client, sent);
		// }
		printf("ccb here\r\n");
	};
	client->PreAccept(NULL, 0, ccb);
	printf("preaccept done\r\n");
}

int main(int argc, char **argv) {
	// transport is used to send messages btwn replicas and schedule msgs
	UDPTransport transport(0.0, 0.0, 0, false);
	size_t sent = 0;
	janusstore::Client *client;
	transport.Timer(0, [client, &sent]() { SendTxn(client, &sent); });
	transport.Run();
	return 0;
}
