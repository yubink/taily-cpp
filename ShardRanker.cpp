/*
 * ShardRanker.cpp
 *
 *  Created on: Oct 23, 2013
 *      Author: yubink
 */

#include "ShardRanker.h"
#include <math.h>
#include <boost/math/distributions/gamma.hpp>

ShardRanker::ShardRanker() {
  // TODO Auto-generated constructor stub

}

ShardRanker::~ShardRanker() {
  // TODO Auto-generated destructor stub
}

void ShardRanker::_getStems(string query, vector<string>* output) {
  char mutableLine[query.size() + 1];
  std::strcpy(mutableLine, query.c_str());
  char* value = std::strtok(mutableLine, " ");

  while (value != NULL) {
    // tokenize and stem/stop query
    value = std::strtok(NULL, " ");
    string term(value);
    string stem = _repo->processTerm(term);

    // if stopword, skip to next term
    if (stem.length() == 0) continue;

    output->push_back(stem);
  }
}

void ShardRanker::_getQueryFeats(vector<string>& stems, float* queryMean, float* queryVar) {
  // calculate mean and variances for query for all shards
  vector<string>::iterator it;

  for(it = stems.begin(); it != stems.end(); ++it) {
    string stem = (*it);

    // get minimum doc feature value for this stem
    float minVal;
    string minFeat(stem);
    minFeat.append(FeatureStore::MIN_FEAT_SUFFIX);
    _stores[0]->getFeature((char*)minFeat.c_str(), &minVal);

    for(uint i = 0; i <= _numShards; i++) {
      // add current term's mean to shard; also shift by min feat value Eq (5)
      float mean;
      string meanFeat(stem);
      meanFeat.append(FeatureStore::FEAT_SUFFIX);
      _stores[i]->getFeature((char*)meanFeat.c_str(), &mean);
      queryMean[i] += mean - minVal;

      // add current term's variance to shard Eq (6)
      float f2;
      string f2Feat(stem);
      f2Feat.append(FeatureStore::SQUARED_FEAT_SUFFIX);
      _stores[i]->getFeature((char*)f2Feat.c_str(), &f2);
      queryVar[i] += f2 - pow(mean,2);
    }
  }
}

void ShardRanker::_getAll(vector<string>& stems, float* all) {
  // calculate Any_i & all_i
   float any[_numShards+1];
   string sizeKey(FeatureStore::SIZE_FEAT_SUFFIX);

   for (int i = 0; i < _numShards+1; i++) {
     // initialize Any_i & all_i
     any[i] = 1.0;
     all[i] = 0.0;

     // get size of current shard
     float shardSize;
     _stores[i]->getFeature((char*)sizeKey.c_str(), &shardSize);

     // for each query term, calculate inner bracket of any_i equation
     vector<string>::iterator it;
     float dfs[stems.size()];
     int dfCnt = 0;
     for(it = stems.begin(); it != stems.end(); ++it) {
       string stem(*it);
       stem.append(FeatureStore::SIZE_FEAT_SUFFIX);
       float df;
       _stores[i]->getFeature((char*)stem.c_str(), &df);

       // smooth it
       if (df < 5) df = 5;

       // store df for all_i calculation
       dfs[dfCnt++] = df;

       any[i] *= (1 - df/shardSize);
     }

     // calculation of any_i
     any[i] = shardSize*(1 - any[i]);

     // calculation of all_i Eq (10)
     all[i] = any[i];
     for (int j = 0; j < stems.size(); j++) {
       all[i] *= dfs[j]/any[i];
     }

   }
}

bool shardPairSort (pair<int, float> i, pair<int,float> j) { return (i.second < j.second); }

void ShardRanker::rank(string query, vector<pair<int, float> >* ranking) {
  float queryMean[_numShards+1];
  float queryVar[_numShards+1];

  // query total means and variances for each shard
  for (int i = 0; i < _numShards+1; i++) {
    queryMean[i] = queryVar[i] = 0.0;
  }
  vector<string> stems;
  _getStems(query, &stems);
  _getQueryFeats(stems, queryMean, queryVar);

  // calculate k and theta from mean/vars Eq (7) (8)
  float k[_numShards+1];
  float theta[_numShards+1];

  for (int i = 0; i < _numShards+1; i++) {
    k[i] = pow(queryMean[i], 2) / queryVar[i];
    theta[i] = queryVar[i] / queryMean[i];
  }

  // all from Eq (10)
  float all[_numShards+1];
  for (int i = 0; i < _numShards+1; i++) {
    all[i] = 0.0;
  }
  _getAll(stems, all);

  // calculate s_c from inline equation after Eq (11)
  float p_c = _n_c / all[0];
  boost::math::gamma_distribution<> collectionGamma(k[0], theta[0]);
  float s_c = boost::math::quantile(complement(collectionGamma, p_c));

  // calculate n_i for all shards and store it in ranking vector so we can sort (unnormalized)
  for (int i = 1; i < _numShards+1; i++) {
    boost::math::gamma_distribution<> shardGamma(k[i], theta[i]);
    float p_i = boost::math::cdf(complement(shardGamma, s_c));
    ranking->push_back(make_pair(i, all[i]*p_i));
  }

  // sort shards by n
  sort(ranking->begin(), ranking->end(), shardPairSort);

  // get normalization factor (top 5 shards sufficient)
  float sum = 0.0;
  for (uint i = 0; i < min(5u, ranking->size()); i++) {
    sum += (*ranking)[i].second;
  }
  float norm = _n_c/sum;

  // renormalize shard scores Eq (12)
  vector<pair<int, float> >::iterator nit;
  for (nit = ranking->begin(); nit != ranking->end(); ++nit) {
    (*nit).second = (*nit).second * norm;
  }
}

