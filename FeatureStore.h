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
  Db _freqDb; // db storing frequent terms; see FREQUENT_TERMS
  Db _infreqDb; // db storing infrequent terms

public:
  static const char* FEAT_SUFFIX;
  static const char* SQUARED_FEAT_SUFFIX;
  static const char* MIN_FEAT_SUFFIX;
  static const char* SIZE_FEAT_SUFFIX;
  static const char* TERM_SIZE_FEAT_SUFFIX;
  static const int FREQUENT_TERMS = 1000; // tf required for a term to be considered "frequent"
  static const uint MAX_TERM_SIZE = 512;

public:
  class TermIterator {

  private:
    Db* _freqDb;
    Db* _infreqDb;
    Dbc* _freqCursor;
    Dbc* _infreqCursor;
    bool _finished;
    pair<string, double> _current;

  public:
    TermIterator(Db* freqDb, Db* infreqDb);
    virtual ~TermIterator();
    void nextTerm();
    bool finished();

    // returns a stem and its df (# of documents that contain it)
    pair<string, double> currrentEntry();
  };

  FeatureStore(string dir,  bool readOnly = false, DBTYPE type = DB_HASH);
  virtual ~FeatureStore();

  void putFeature(char* key, double value, int frequency, int flags = DB_NOOVERWRITE);

  // returns feature in value; if feature isn't found, returns non-zero
  int getFeature(char* key, double* value);

  // add val to the keyStr feature if it exists already; otherwise, create the feature
  void addValFeature(char* keyStr, double val, int frequency);

  TermIterator* getTermIterator();

private:
  void _openDb(const char* dbPath, Db* db, u_int32_t oFlags, DBTYPE type = DB_HASH);
  void _closeDb(Db* db);
};


#endif /* FEATURESTORE_H_ */
