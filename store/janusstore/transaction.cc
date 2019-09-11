#include "store/janusstore/transaction.h"

namespace janusstore {

Transaction::Transaction(uint64_t txn_id, uint64_t server_id) {
	this->txn_id = txn_id;
	this->server_id = server_id;
}

Transaction::Transaction(uint64_t txn_id, uint64_t server_id, const TransactionMessage &msg) {
	this->txn_id = txn_id;
	this->server_id = server_id;
}

Transaction::~Transaction() {}

void Transaction::setTransactionId(uint64_t txn_id) {
	this->txn_id = txn_id;
}
void Transaction::setTransactionStatus(janusstore::proto::TransactionMessage::Status status) {
	this->status = status;
}
const uint64_t Transaction::getTransactionId() const {
	return txn_id;
}

const janusstore::proto::TransactionMessage::Status Transaction::getTransactionStatus() const {
	return status;
}
const std::unordered_set<std::string>& Transaction::getReadSet() const {
	return read_set;
}
const std::unordered_map<std::string, std::string>& Transaction::getWriteSet() const {
	return write_set;
}

void Transaction::addReadSet(const std::string &key) {
	read_set.insert(key);
}
void Transaction::addWriteSet(const std::string &key, const std::string &value){
	write_set[key] = value;
}
void Transaction::serialize(janusstore::proto::TransactionMessage *msg) const {
	// TODO(andy): implement conversion from Transaction to TransactionMessage
	// TODO(andy): replace with this->status when the type is updated
	msg->set_status(janusstore::proto::TransactionMessage::Status(0));
}

} // namespace janusstore
