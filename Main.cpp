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

#include "FeatureStore.h"

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

    FeatureStore store(dbPath, false);

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
        int ctf = termData->corpus.totalCount;

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

        string featKey(termData->term);
        featKey.append(FeatureStore::FEAT_SUFFIX);
        store.putFeature((char*) featKey.c_str(), featSum, ctf);

        string squaredFeatKey(termData->term);
        squaredFeatKey.append(FeatureStore::SQUARED_FEAT_SUFFIX);
        store.putFeature((char*) squaredFeatKey.c_str(), squaredFeatSum, ctf);

        iter->nextEntry();
      }
      delete iter;

    }

  } else if (strcmp(argv[1], "run") == 0) {
    // create and open db
    FeatureStore store(params["db"], true);

    char *stem = (char*) "whoop#min";
    float minval;
    std::cout << "Data is " << minval << std::endl;


  } else {
    std::cout << "Unrecognized option." << std::endl;
  }

  puts("Hello World!!!");

  return EXIT_SUCCESS;
}
