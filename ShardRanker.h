/*
 * ShardRanker.h
 *
 *  Created on: Oct 23, 2013
 *      Author: yubink
 */

#ifndef SHARDRANKER_H_
#define SHARDRANKER_H_

#include "FeatureStore.h"
#include "indri/Repository.hpp"

using namespace std;

class ShardRanker {
private:
  FeatureStore* stores;
  indri::collection::Repository* repo;

public:
  ShardRanker();
  virtual ~ShardRanker();

  void init();
  void rank(string query);

};

#endif /* SHARDRANKER_H_ */

