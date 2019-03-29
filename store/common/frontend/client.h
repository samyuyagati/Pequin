// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/client.h:
 *   Interface for a multiple shard transactional client.
 *
 **********************************************************************/

#ifndef _CLIENT_API_H_
#define _CLIENT_API_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"

#include <functional>
#include <string>
#include <vector>

typedef std::function<void(int, const std::string &,
    const std::string &, Timestamp)> get_callback;
typedef std::function<void(int, const std::string &)> get_timeout_callback;

typedef std::function<void(int, const std::string &,
    const std::string &)> put_callback;
typedef std::function<void(int, const std::string &,
    const std::string &)> put_timeout_callback;

typedef std::function<void(bool)> commit_callback;
typedef std::function<void(int)> commit_timeout_callback;

typedef std::function<void()> abort_callback;
typedef std::function<void(int)> abort_timeout_callback;

class Client {
 public:
  Client() {};
  virtual ~Client() {};

  // Begin a transaction.
  virtual void Begin() = 0;

  // Get the value corresponding to key.
  virtual void Get(const std::string &key, get_callback gcb,
      get_timeout_callback gtcb, uint32_t timeout) = 0;

  // Set the value for the given key.
  virtual void Put(const std::string &key, const std::string &value,
      put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) = 0;

  // Commit all Get(s) and Put(s) since Begin().
  virtual void Commit(commit_callback ccb, commit_timeout_callback ctcb,
      uint32_t timeout) = 0;
  
  // Abort all Get(s) and Put(s) since Begin().
  virtual void Abort(abort_callback acb, abort_timeout_callback atcb,
      uint32_t timeout) = 0;

  // Returns statistics (vector of integers) about most recent transaction.
  virtual std::vector<int> Stats() = 0;

  // Sharding logic: Given key, generates a number b/w 0 to nshards-1
  static uint64_t key_to_shard(const std::string &key, uint64_t nshards) {
      uint64_t hash = 5381;
      const char* str = key.c_str();
      for (unsigned int i = 0; i < key.length(); i++) {
          hash = ((hash << 5) + hash) + (uint64_t)str[i];
      }

      return (hash % nshards);
  };
};

#endif /* _CLIENT_API_H_ */
