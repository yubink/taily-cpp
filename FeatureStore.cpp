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
const char* FeatureStore::SIZE_FEAT_SUFFIX = "#d";
const char* FeatureStore::TERM_SIZE_FEAT_SUFFIX = "#t";

FeatureStore::FeatureStore(string dir, bool readOnly, DBTYPE type) : _freqDb(NULL, 0),  _infreqDb(NULL, 0) {
  string freqPath = dir + "/freq.db";
  string infreqPath = dir + "/infreq.db";

  u_int32_t flags = readOnly ? DB_RDONLY : DB_CREATE | DB_EXCL;

  _openDb(freqPath.c_str(), &_freqDb, flags, type);
  _openDb(infreqPath.c_str(), &_infreqDb, flags, type);
}

FeatureStore::~FeatureStore() {
  _closeDb(&_freqDb);
  _closeDb(&_infreqDb);
}

int FeatureStore::getFeature(char* keyStr, double* val) {
  Dbt key, data;

  key.set_data(keyStr);
  key.set_size(strlen(keyStr) + 1);

  data.set_data(val);
  data.set_ulen(sizeof(double));
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

void FeatureStore::putFeature(char* stem, double val, int frequency, int flags) {
  Dbt key(stem, strlen(stem) + 1);
  Dbt data(&val, sizeof(double));

  Db* db;
  if (frequency >= FREQUENT_TERMS) {
    db = &_freqDb;
  } else {
    db = &_infreqDb;
  }

  int ret = db->put(NULL, &key, &data, flags);
  if (ret == DB_KEYEXIST) {
    db->err(ret, "Put failed because key %s already exists", stem);
  }
}

void FeatureStore::addValFeature(char* keyStr, double val, int frequency) {
  double prevVal;

  Dbt key, data;
  key.set_data(keyStr);
  key.set_size(strlen(keyStr) + 1);

  data.set_data(&prevVal);
  data.set_ulen(sizeof(double));
  data.set_flags(DB_DBT_USERMEM);

  int retval = _freqDb.get(NULL, &key, &data, 0);

  if (retval == DB_NOTFOUND) {
    retval = _infreqDb.get(NULL, &key, &data, 0);
    if (retval == DB_NOTFOUND) {
      prevVal = 0.0;
    } else {
      frequency = FREQUENT_TERMS - 1;
    }
  } else {
    frequency = FREQUENT_TERMS + 1;
  }

  putFeature(keyStr, val+prevVal, frequency, 0);
}

FeatureStore::TermIterator* FeatureStore::getTermIterator() {
  return new FeatureStore::TermIterator(&_freqDb, &_infreqDb);
}

void FeatureStore::_openDb(const char* dbPath, Db* db, u_int32_t oFlags, DBTYPE type) {
  try {
    // Open the database
    db->open(NULL, // Transaction pointer
        dbPath, // Database file name
        NULL, // Optional logical database name
        type, // Database access method
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

FeatureStore::TermIterator::TermIterator(Db* freqDb, Db* infreqDb): _freqDb(freqDb),
    _infreqDb(infreqDb), _finished(false), _current() {
  _freqDb->cursor(NULL, &_freqCursor, 0);
}

FeatureStore::TermIterator::~TermIterator() {
  if (_infreqCursor) {
    _infreqCursor->close();
  }
  if (_freqCursor) {
    _freqCursor->close();
  }
}

void FeatureStore::TermIterator::nextTerm() {
  char keyStr[MAX_TERM_SIZE+1];
  double val;

  Dbt key, data;
  key.set_data(keyStr);
  key.set_ulen(MAX_TERM_SIZE+1);
  key.set_flags(DB_DBT_USERMEM);

  data.set_data(&val);
  data.set_ulen(sizeof(double));
  data.set_flags(DB_DBT_USERMEM);

  int ret;

  // Iterate over the database, retrieving each record in turn.
  Dbc* cursor = _freqCursor ? _freqCursor : _infreqCursor;
  while(true) {
    ret = cursor->get(&key, &data, DB_NEXT);

    if (ret != 0) {
      if (cursor == _freqCursor) {
        _freqCursor->close();
        _freqCursor = NULL;
        _infreqDb->cursor(NULL, &_infreqCursor, 0);
        cursor = _infreqCursor;
      } else {
        _finished = true;
        _infreqCursor->close();
        _infreqCursor = NULL;
        break;
      }
    } else {
      string stemKey(keyStr);
      size_t idx = stemKey.find(SIZE_FEAT_SUFFIX);

      // is it a stem df key value pair? (there is a #d key for the doc count of entire corpus)
      if (idx != string::npos && idx != 0) {
        string stem = stemKey.substr(0, stemKey.size() - strlen(SIZE_FEAT_SUFFIX));
        _current = make_pair(stem, val);
        break;
      }
    }
  }
}

bool FeatureStore::TermIterator::finished() {
  return _finished;
}

pair<string, double> FeatureStore::TermIterator::currrentEntry() {
  return _current;
}
