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
  // array of FeatureStore pointers
  // stores[0] is the whole collection store; stores[1] onwards is each shard; length is numShards+1
  vector<FeatureStore*> _stores;

  // a single indri index built the same way; just for stemming term
  indri::collection::Repository* _repo;

  // number of shards
  uint _numShards;

  // Taily parameter used in Eq (11)
  uint _n_c;

  // retrieves the mean/variance for query terms and fills in the given queryMean/queryVar arrays
  // and marks shards that have at least one doc for one query term in given bool array
  void _getQueryFeats(vector<string>& stems, double* queryMean, double* queryVar, bool* hasATerm);

  // tokenizes, stems and stops query term into output vector
  void _getStems(string query, vector<string>* output);

  // calculates All from Eq (10)
  void _getAll(vector<string>& stems, double* all);

public:
  ShardRanker(vector<string> dbPaths, indri::collection::Repository* repo, uint numShards, uint n_c);
  virtual ~ShardRanker();

  void init();
  void rank(string query, vector<pair<int, double> >* ranking);
};

#endif /* SHARDRANKER_H_ */

