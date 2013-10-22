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

float calcIndriFeature(float tf, float ctf, float totalTermCount, float docLength, int mu = 2500) {
  return log( (tf + mu*(ctf/totalTermCount)) / (docLength + mu) );
}

int main(int argc, char * argv[]) {
  int MU = 2500;

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

    if (state->size() > 1) {
      cout << "Index has more than 1 part. Can't deal with this, man.";
      exit(EXIT_FAILURE);
    }


    for(size_t i = 0; i < state->size(); i++) {

      using namespace indri::index;
      Index* index = (*state)[i];
      indri::thread::ScopedLock( index->iteratorLock() );

      DocListFileIterator* iter = index->docListFileIterator();
      iter->startIteration();
      cout << index->termCount() << " " << index->documentCount() << endl;

      float totalTermCount = index->termCount();

      while (!iter->finished()) {
        DocListFileIterator::DocListData* entry = iter->currentEntry();
        TermData* termData = entry->termData;
        float ctf = termData->corpus.totalCount;

        double featSum = 0.0f;
        double squaredFeatSum = 0.0f;

        entry->iterator->startIteration();

        while (!entry->iterator->finished()) {
          DocListIterator::DocumentData* doc = entry->iterator->currentEntry();
          float length = index->documentLength(doc->document);
          float tf = doc->positions.size();

          // calulate Indri score feature and sum it up
          featSum += calcIndriFeature(tf, ctf, totalTermCount, length);
          squaredFeatSum += pow(calcIndriFeature(tf, ctf, totalTermCount, length), 2);

          entry->iterator->nextEntry();
        }
        featSum /= ctf;
        squaredFeatSum /= ctf;

        float var = squaredFeatSum - pow(featSum, 2);

        string featKey(termData->term);
        featKey.append("#f");
        string squaredFeatKey(termData->term);
        squaredFeatKey.append("#f2");

        string varKey(termData->term);
        varKey.append("#v");


        iter->nextEntry();
      }
      delete iter;

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
