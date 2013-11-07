/*
 * ShardRanker.cpp
 *
 *  Created on: Oct 23, 2013
 *      Author: yubink
 */

#include "ShardRanker.h"
#include <math.h>
#include <boost/math/distributions/gamma.hpp>

ShardRanker::ShardRanker(vector<string> dbPaths, indri::collection::Repository* repo,
    uint numShards, uint n_c) : _repo(repo), _numShards(numShards), _n_c(n_c) {
  for (uint i = 0; i < dbPaths.size(); i++) {
    cout << dbPaths[i] << endl;
    _stores.push_back(new FeatureStore(dbPaths[i], true));
  }
}

ShardRanker::~ShardRanker() {
  vector<FeatureStore*>::iterator it;
  for (it = _stores.begin(); it != _stores.end(); ++it) {
    delete (*it);
  }
}

void ShardRanker::_getStems(string query, vector<string>* output) {
  char mutableLine[query.size() + 1];
  std::strcpy(mutableLine, query.c_str());

  for (char* value = std::strtok(mutableLine, " ");
      value != NULL;
      value = std::strtok(NULL, " ")) {
    // tokenize and stem/stop query
    string term(value);
    string stem = _repo->processTerm(term);

    // if stopword, skip to next term
    if (stem.length() == 0) continue;

    output->push_back(stem);
  }
}

void ShardRanker::_getQueryFeats(vector<string>& stems, double* queryMean, double* queryVar,
    bool* hasATerm) {
  // calculate mean and variances for query for all shards
  vector<string>::iterator it;

  for(it = stems.begin(); it != stems.end(); ++it) {
    string stem = (*it);

    // get minimum doc feature value for this stem
    double minVal = DBL_MAX;
    string minFeat(stem);
    minFeat.append(FeatureStore::MIN_FEAT_SUFFIX);
    int found = _stores[0]->getFeature((char*)minFeat.c_str(), &minVal);

    bool calcMin = false;
    if (found != 0) {
      calcMin = true;
    }

    // sums of individual shard features to calculate corpus-wide feature
    double globalFSum = 0;
    double globalF2Sum = 0;
    double globalDf = 0;

    // for each shard (not including whole corpus db), calculate mean/var
    // keep track of totals to use in the corpus-wide features
    for(uint i = 1; i <= _numShards; i++) {
      // get current term's shard df
      double df = 0;
      string dfFeat(stem);
      dfFeat.append(FeatureStore::SIZE_FEAT_SUFFIX);
      _stores[i]->getFeature((char*)dfFeat.c_str(), &df);
      globalDf += df;

      // if this shard doesn't have this term, skip; otherwise you get nan everywhere
      if (df == 0) continue;
      hasATerm[i] = true;

      // add current term's mean to shard; also shift by min feat value Eq (5)
      double fSum = 0;
      string meanFeat(stem);
      meanFeat.append(FeatureStore::FEAT_SUFFIX);
      _stores[i]->getFeature((char*)meanFeat.c_str(), &fSum);
      //queryMean[i] += fSum/df - minVal;
      queryMean[i] += fSum/df; // handle min values separately afterwards
      globalFSum += fSum;

      // add current term's variance to shard Eq (6)
      double f2Sum = 0;
      string f2Feat(stem);
      f2Feat.append(FeatureStore::SQUARED_FEAT_SUFFIX);
      _stores[i]->getFeature((char*)f2Feat.c_str(), &f2Sum);
      queryVar[i] += f2Sum/df - pow(fSum/df,2);
      globalF2Sum += f2Sum;

      // if there is no global min stored, figure out the minimum from shards
      if (calcMin) {
        double currMin;
        string minFeat(stem);
        minFeat.append(FeatureStore::MIN_FEAT_SUFFIX);
        _stores[i]->getFeature((char*)minFeat.c_str(), &currMin);
        if (currMin < minVal) {
          minVal = currMin;
        }
      }
    }

    // adjust shard mean by minimum value
    for(uint i = 1; i <= _numShards; i++) {
      if (hasATerm[i]) {
        queryMean[i] -= minVal;
      }
    }

    if (globalDf > 0) hasATerm[0] = true;

    // calculate global mean/variances based on shard sums
    queryMean[0] += globalFSum/globalDf - minVal;
    queryVar[0] += globalF2Sum/globalDf - pow(globalFSum/globalDf, 2);
  }
}

void ShardRanker::_getAll(vector<string>& stems, double* all) {
  // calculate Any_i & all_i
   double any[_numShards+1];
   string sizeKey(FeatureStore::SIZE_FEAT_SUFFIX);

   for (int i = 0; i < _numShards+1; i++) {
     // initialize Any_i & all_i
     any[i] = 1.0;
     all[i] = 0.0;

     // get size of current shard
     double shardSize;
     _stores[i]->getFeature((char*)sizeKey.c_str(), &shardSize);

     // for each query term, calculate inner bracket of any_i equation
     vector<string>::iterator it;
     double dfs[stems.size()];
     int dfCnt = 0;
     for(it = stems.begin(); it != stems.end(); ++it) {
       string stem(*it);
       stem.append(FeatureStore::SIZE_FEAT_SUFFIX);
       double df;
       _stores[i]->getFeature((char*)stem.c_str(), &df);

       // smooth it
       if (df < 1) df = 1;

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

//reverse sort order
bool shardPairSort (pair<int, double> i, pair<int,double> j) { return (i.second > j.second); }

void ShardRanker::rank(string query, vector<pair<int, double> >* ranking) {
  double queryMean[_numShards+1];
  double queryVar[_numShards+1];
  bool hasATerm[_numShards+1];  // to mark shards that have at least one doc for one query term

  // query total means and variances for each shard
  for (int i = 0; i < _numShards+1; i++) {
    queryMean[i] = queryVar[i] = 0.0;
    hasATerm[i] = false;
  }
  vector<string> stems;
  _getStems(query, &stems);
  _getQueryFeats(stems, queryMean, queryVar, hasATerm);

  // fast fall-through for 2 degenerate cases
  if (!hasATerm[0]) {
    // case 1: there are no documents in the entire collection that matches any query term
    // return empty ranking
    return;
  } else if (queryVar[0] < 1e-10) {
    // FIXME: these var ~= 0 cases should really be handled more carefully; instead of
    // n_i = 1, it could be there are two or more very similarly scoring docs; I should keep
    // track of the df of these shards and use that instead of n_i = 1...

    // case 2: there is only 1 document in entire collection that matches any query term
    // return the shard with the document with n_i = 1
    for (int i = 1; i < _numShards+1; i++) {
      if (hasATerm[i]) {
        ranking->push_back(make_pair(i, 1));
        break;
      }
    }
    return;
  }

  // calculate k and theta from mean/vars Eq (7) (8)
  double k[_numShards+1];
  double theta[_numShards+1];

  for (int i = 0; i < _numShards+1; i++) {
    // special case, if df = 1, then var ~= 0 (or if no terms occur in shard)
    if (queryVar[i] < 1e-10) {
      k[i] = -1;
      theta[i] = -1;
      continue;
    }

    k[i] = pow(queryMean[i], 2) / queryVar[i];
    theta[i] = queryVar[i] / queryMean[i];
  }

  // all from Eq (10)
  double all[_numShards+1];
  for (int i = 0; i < _numShards+1; i++) {
    all[i] = 0.0;
  }
  _getAll(stems, all);

  // calculate s_c from inline equation after Eq (11)
  double p_c = _n_c / all[0];

  // if n_c > all[0], set probability to 1
  if (p_c > 1.0) p_c = 1.0;

  boost::math::gamma_distribution<> collectionGamma(k[0], theta[0]);
  double s_c = boost::math::quantile(complement(collectionGamma, p_c));

  // calculate n_i for all shards and store it in ranking vector so we can sort (unnormalized)
  for (int i = 1; i < _numShards+1; i++) {
    // if there are no query terms in shard, skip
    if (!hasATerm[i]) continue;

    // if var is ~= 0, then don't build a distribution.
    // based on the mean of the shard (which is the score of the single doc), n_i is either 0 or 1
    if (queryVar[i] < 1e-10 && hasATerm[i]) {
      if (queryMean[i] >= s_c) {
        ranking->push_back(make_pair(i, 1));
      }
    } else {
      // do normal Taily stuff pre-normalized Eq (12)
      boost::math::gamma_distribution<> shardGamma(k[i], theta[i]);
      double p_i = boost::math::cdf(complement(shardGamma, s_c));
      ranking->push_back(make_pair(i, all[i]*p_i));
    }
  }

  // sort shards by n
  sort(ranking->begin(), ranking->end(), shardPairSort);

  // get normalization factor (top 5 shards sufficient)
  double sum = 0.0;
  for (uint i = 0; i < min(5, (int)ranking->size()); i++) {
    sum += (*ranking)[i].second;
  }
  double norm = _n_c/sum;

  // normalize shard scores Eq (12)
  vector<pair<int, double> >::iterator nit;
  for (nit = ranking->begin(); nit != ranking->end(); ++nit) {
    (*nit).second = (*nit).second * norm;
  }
}

