#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <algorithm>
#include <string>
#include <db_cxx.h>
#include <boost/math/distributions/gamma.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include "indri/QueryEnvironment.hpp"
#include "indri/Repository.hpp"
#include "indri/CompressedCollection.hpp"
#include "indri/ScopedLock.hpp"

char* getOption(char ** begin, char ** end, const std::string & option) {
  char ** itr = std::find(begin, end, option);
  if (itr != end && ++itr != end) {
    return *itr;
  }
  return 0;
}

bool hasOption(char** begin, char** end, const std::string& option) {
  return std::find(begin, end, option) != end;
}

void readParams(const char* paramFile, map<string, string> *params) {
  ifstream file;
  file.open(paramFile);

  string line;
  if (file.is_open()) {

    while (getline(file, line)) {
      char mutableLine[line.size() + 1];
      std::strcpy(mutableLine, line.c_str());

      char* key = std::strtok(mutableLine, "=");
      char* value = std::strtok(NULL, "=");
      (*params)[key] = value;

      std::cout << line << endl;
    }
    file.close();
  }
}

void openDb(const char* dbPath, Db* db, u_int32_t oFlags) {
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

void closeDb(Db* db) {
  try {
    db->close(0);
  } catch (DbException &e) {
    cerr << "Error while closing DB. Exiting." << endl << e.what() << endl;
  } catch (std::exception &e) {
    cerr << "Error while closing DB. Exiting." << endl << e.what() << endl;
  }
}

int main(int argc, char * argv[]) {
  boost::math::gamma_distribution<> my_gamma(1, 1);
  boost::math::cdf(my_gamma, 0.5);

  char* paramFile = getOption(argv, argv + argc, "-p");

  // read parameter file
  std::map<string, string> params;
  readParams(paramFile, &params);

  if (strcmp(argv[1], "build") == 0) {

    string dbPath = params["db"];
    string indexPath = params["index"];

    // create and open the data store
    Db db(NULL, 0);
    openDb(dbPath.c_str(), &db, DB_CREATE | DB_EXCL);

    indri::collection::Repository repo;
    repo.openRead(indexPath);

    indri::collection::Repository::index_state state = repo.indexes();

    for(size_t i = 0; i < state->size; i++) {
      indri::index::Index* index = (*state)[i];
      indri::thread::ScopedLock( index->iteratorLock() );

    }

    char *stem = (char*) "whoop#min";
    float minval = 0.6;

    Dbt key(stem, strlen(stem) + 1);
    Dbt data(&minval, sizeof(float));

    int ret = db.put(NULL, &key, &data, DB_NOOVERWRITE);
    if (ret == DB_KEYEXIST) {
      db.err(ret, "Put failed because key %s already exists", stem);
    }

    closeDb(&db);

  } else if (strcmp(argv[1], "run") == 0) {
    // create and open db
    Db db(NULL, 0);
    openDb(params["db"].c_str(), &db, DB_RDONLY);

    char *stem = (char*) "whoop#min";
    float minval;

    Dbt key, data;

    key.set_data(stem);
    key.set_size(strlen(stem) + 1);

    data.set_data(&minval);
    data.set_ulen(sizeof(float));
    data.set_flags(DB_DBT_USERMEM);

    db.get(NULL, &key, &data, 0);

    std::cout << "Data is " << minval << std::endl;

    closeDb(&db);

  } else {
    std::cout << "Unrecognized option." << std::endl;
  }

  puts("Hello World!!!");

  return EXIT_SUCCESS;
}
