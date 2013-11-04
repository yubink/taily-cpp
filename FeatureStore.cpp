/*
 * FeatureStore.cpp
 *
 *  Created on: Oct 23, 2013
 *      Author: yubink
 */

#include "FeatureStore.h"

using namespace std;

const char* FeatureStore::FEAT_SUFFIX = "#f";
const char* FeatureStore::SQUARED_FEAT_SUFFIX = "#f2";
const char* FeatureStore::MIN_FEAT_SUFFIX = "#m";
const char* FeatureStore::SIZE_FEAT_SUFFIX = "#s";

FeatureStore::FeatureStore(string dir, bool readOnly = false) : _freqDb(NULL, 0), _infreqDb(NULL, 0) {
  string freqPath = dir + "/freq.db";
  string infreqPath = dir + "/infreq.db";

  u_int32_t flags = readOnly ? DB_RDONLY : DB_CREATE | DB_EXCL;

  _openDb(freqPath.c_str(), &_freqDb, flags);
  _openDb(infreqPath.c_str(), &_infreqDb, flags);
}

FeatureStore::~FeatureStore() {
  _closeDb(&_freqDb);
  _closeDb(&_infreqDb);
}

int FeatureStore::getFeature(char* keyStr, float* val) {
  Dbt key, data;

  key.set_data(keyStr);
  key.set_size(strlen(keyStr) + 1);

  data.set_data(val);
  data.set_ulen(sizeof(float));
  data.set_flags(DB_DBT_USERMEM);

  int retval = _freqDb.get(NULL, &key, &data, 0);

  if (retval == DB_NOTFOUND) {
    retval = _infreqDb.get(NULL, &key, &data, 0);
    if (retval == DB_NOTFOUND) {
      return 1;
    }
  }

  return 0;
}

void FeatureStore::putFeature(char* stem, float val, int frequency) {
  Dbt key(stem, strlen(stem) + 1);
  Dbt data(&val, sizeof(float));

  Db* db;
  if (frequency >= FREQUENT_TERMS) {
    db = &_freqDb;
  } else {
    db = &_infreqDb;
  }

  int ret = db->put(NULL, &key, &data, DB_NOOVERWRITE);
  if (ret == DB_KEYEXIST) {
    db->err(ret, "Put failed because key %s already exists", stem);
  }
}


void FeatureStore::_openDb(const char* dbPath, Db* db, u_int32_t oFlags) {
  try {
    // Open the database
    db->open(NULL, // Transaction pointer
        dbPath, // Database file name
        NULL, // Optional logical database name
        DB_HASH, // Database access method
        oFlags, // Open flags
        0); // File mode (using defaults)
  } catch (DbException &e) {
    cerr << "Error opening DB. Exiting." << endl << e.what() << endl;
    exit(EXIT_FAILURE);
  } catch (std::exception &e) {
    cerr << "Error opening DB. Exiting." << endl << e.what() << endl;
    exit(EXIT_FAILURE);
  }
}

void FeatureStore::_closeDb(Db* db) {
  try {
    db->close(0);
  } catch (DbException &e) {
    cerr << "Error while closing DB. Exiting." << endl << e.what() << endl;
  } catch (std::exception &e) {
    cerr << "Error while closing DB. Exiting." << endl << e.what() << endl;
  }
}
