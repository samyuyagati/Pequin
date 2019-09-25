//
// Created by Janice Chan on 9/24/19.
//

#ifndef MORTY_TAPIR_SMALLBANK_CLIENT_H
#define MORTY_TAPIR_SMALLBANK_CLIENT_H

#include "store/common/frontend/sync_client.h"
#include "store/benchmark/async/smallbank/smallbank_transaction.h"

namespace smallbank {
    constexpr int num_customers_ = 18000;
    class SmallBankClient {
    public:
        SmallBankClient(SyncClient *client,
                        const uint32_t timeout, const uint32_t balance_ratio, const uint32_t deposit_checking_ratio,
                        const uint32_t transact_saving_ratio, const uint32_t amalgamate_ratio,
                        const uint32_t num_hotspot_keys,
                        const uint32_t num_non_hotspot_keys, string all_keys[num_customers_]);

        void startBenchmark(const uint32_t duration);

    private:
        SmallBankTransaction transaction_;
        uint32_t balance_ratio_;
        uint32_t deposit_checking_ratio_;
        uint32_t transact_saving_ratio_;
        uint32_t amalgamate_ratio_;
        uint32_t num_hotspot_keys_;
        uint32_t num_non_hotspot_keys_;
        std::string all_keys_[num_customers_];
        std::string getCustomerKey();
        std::pair <string, string> getCustomerKeyPair();
    };
} //namespace smallbank


#endif //MORTY_TAPIR_SMALLBANK_CLIENT_H