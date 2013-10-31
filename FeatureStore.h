/*
 * FeatureStore.h
 *
 * Wrapper around Berkeley DB which stores Taily features for shard ranking.
 *
 *  Created on: Oct 23, 2013
 *      Author: yubink
 */

#ifndef FEATURESTORE_H_
#define FEATURESTORE_H_

#include <db_cxx.h>
#include <string.h>
#include <stdlib.h>

using namespace std;

class FeatureStore {

private:
  Db freqDb; // db storing frequent terms; see FREQUENT_TERMS
  Db infreqDb; // db storing infrequent terms

public:
  static const char* FEAT_SUFFIX;
  static const char* SQUARED_FEAT_SUFFIX;
  static const int FREQUENT_TERMS = 1000; // tf required for a term to be considered "frequent"

public:
  FeatureStore(string dir, bool readOnly);
  virtual ~FeatureStore();

  void putFeature(char* key, float value, int frequency);

  // returns feature in value; if feature isn't found, returns non-zero
  int getFeature(char* key, float* value);

private:
  void _openDb(const char* dbPath, Db* db, u_int32_t oFlags);
  void _closeDb(Db* db);
};


#endif /* FEATURESTORE_H_ */
