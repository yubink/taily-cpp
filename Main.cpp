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
#include "ShardRanker.h"

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


double calcIndriFeature(double tf, double ctf, double totalTermCount, double docLength, int mu = 2500) {
  return log( (tf + mu*(ctf/totalTermCount)) / (docLength + mu) );
}

void tokenize(string line, const char * delim, vector<string>* output) {
  char mutableLine[line.size() + 1];
  std::strcpy(mutableLine, line.c_str());
  for (char* value = std::strtok(mutableLine, delim); 
      value != NULL; 
      value = std::strtok(NULL, delim)) {
    output->push_back(value);
  }
}

int main(int argc, char * argv[]) {
  int MU = 2500;

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

      // get the total term length of shard
      double totalTermCount = index->termCount();

      // store the shard size (# of docs) feature
      int shardSizeFeat = index->documentCount();
      string featSize(FeatureStore::SIZE_FEAT_SUFFIX);
      store.putFeature((char*)featSize.c_str(), (double)shardSizeFeat, shardSizeFeat);

      // for each stem in the index
      while (!iter->finished()) {
        DocListFileIterator::DocListData* entry = iter->currentEntry();
        TermData* termData = entry->termData;
        int ctf = termData->corpus.totalCount;
        int df = termData->corpus.documentCount;

        double featSum = 0.0f;
        double squaredFeatSum = 0.0f;

        entry->iterator->startIteration();

        // calculate E[f] and E[f^2] eq (3) (4)
        while (!entry->iterator->finished()) {
          DocListIterator::DocumentData* doc = entry->iterator->currentEntry();
          double length = index->documentLength(doc->document);
          double tf = doc->positions.size();

          // calulate Indri score feature and sum it up
          double feat = calcIndriFeature(tf, ctf, totalTermCount, length);
          featSum += feat;
          squaredFeatSum += pow(feat, 2);

          entry->iterator->nextEntry();
        }
        featSum /= df;
        squaredFeatSum /= df;

        // store df feature for term
        string dfFeatKey(termData->term);
        dfFeatKey.append(FeatureStore::SIZE_FEAT_SUFFIX);
        store.putFeature((char*)dfFeatKey.c_str(), df, ctf);

        // store E[f]
        string featKey(termData->term);
        featKey.append(FeatureStore::FEAT_SUFFIX);
        store.putFeature((char*) featKey.c_str(), featSum, ctf);

        // store E[f^2]
        string squaredFeatKey(termData->term);
        squaredFeatKey.append(FeatureStore::SQUARED_FEAT_SUFFIX);
        store.putFeature((char*) squaredFeatKey.c_str(), squaredFeatSum, ctf);

        iter->nextEntry();
      }
      delete iter;

    }
  } else if (strcmp(argv[1], "buildall") == 0) {
    using namespace indri::collection;
    using namespace indri::index;

    string dbPath = params["db"];
    string indexstr = params["index"];

    FeatureStore store(dbPath, false);
    vector<Repository*> indexes;

    char mutableLine[indexstr.size() + 1];
    std::strcpy(mutableLine, indexstr.c_str());

    // get all shard indexes and add to vector
    for (char* value = std::strtok(mutableLine, ":"); 
        value != NULL; 
        value = std::strtok(NULL, ":")) {
      Repository* repo = new Repository();
      repo->openRead(value);
      indexes.push_back(repo);
    }

    // go through all indexes and collect ctf and df statistics.
    long totalTermCount = 0;
    long totalDocCount = 0;

    vector<Repository*>::iterator it;
    for (it = indexes.begin(); it != indexes.end(); ++it) {

      // if it has more than one index, quit
      Repository::index_state state = (*it)->indexes();
      if (state->size() > 1) {
        cout << "Index has more than 1 part. Can't deal with this, man.";
        exit(EXIT_FAILURE);
      }
      Index* index = (*state)[0];
      DocListFileIterator* iter = index->docListFileIterator();
      iter->startIteration();

      // add the total term length of shard
      totalTermCount += index->termCount();
      // add the shard size (# of docs)
      totalDocCount += index->documentCount();

      // go through all terms in the index and collect df/ctf
      while (!iter->finished()) {
        DocListFileIterator::DocListData* entry = iter->currentEntry();
        TermData* termData = entry->termData;
        int ctf = termData->corpus.totalCount;
        int df = termData->corpus.documentCount;

        // store df feature for term
        string dfFeatKey(termData->term);
        dfFeatKey.append(FeatureStore::SIZE_FEAT_SUFFIX);
        store.addValFeature((char*)dfFeatKey.c_str(), df, ctf);

        // store ctf feature for term
        string ctfFeatKey(termData->term);
        ctfFeatKey.append(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
        store.addValFeature((char*)dfFeatKey.c_str(), df, ctf);

        iter->nextEntry();
      }
      delete iter;
    }

    // add collection global features needed for shard ranking
    string totalTermKey(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
    store.putFeature((char*)totalTermKey.c_str(), totalTermCount, FeatureStore::FREQUENT_TERMS+1);
    string featSize(FeatureStore::SIZE_FEAT_SUFFIX);
    store.putFeature((char*)featSize.c_str(), totalDocCount, FeatureStore::FREQUENT_TERMS+1);

    // iterate through the database for all terms and calculate features
    FeatureStore::TermIterator* termit = store.getTermIterator();
    while (!termit->finished()) {
      // get a stem and its df
      pair<string,double> termAndDf = termit->currrentEntry();
      string stem = termAndDf.first;
      double df = termAndDf.second;

      // retrieve the stem's ctf
      double ctf;
      string ctfKey(stem);
      ctfKey.append(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
      store.getFeature((char*) ctfKey.c_str(), &ctf);

      double featSum = 0.0f;
      double squaredFeatSum = 0.0f;
      double minFeat = DBL_MAX;

      for (it = indexes.begin(); it != indexes.end(); ++it) {
        Repository::index_state state = (*it)->indexes();
        Index* index = (*state)[0];

        DocListIterator* iter = index->docListIterator(termAndDf.first);
        iter->startIteration();
        while (!iter->finished()) {
          DocListIterator::DocumentData* doc = iter->currentEntry();

          double length = index->documentLength(doc->document);
          double tf = doc->positions.size();

          // calulate Indri score feature and sum it up
          double feat = calcIndriFeature(tf, ctf, totalTermCount, length);
          if (feat < minFeat) {
            minFeat = feat;
          }

          featSum += feat;
          squaredFeatSum += pow(feat, 2);

          iter->nextEntry();
        }
        delete iter;
      }
      featSum /= df;
      squaredFeatSum /= df;

      // store min feature for term
      string minFeatKey(stem);
      minFeatKey.append(FeatureStore::MIN_FEAT_SUFFIX);
      store.putFeature((char*)minFeatKey.c_str(), minFeat, (int)ctf);

      // store E[f]
      string featKey(stem);
      featKey.append(FeatureStore::FEAT_SUFFIX);
      store.putFeature((char*) featKey.c_str(), featSum, (int)ctf);

      // store E[f^2]
      string squaredFeatKey(stem);
      squaredFeatKey.append(FeatureStore::SQUARED_FEAT_SUFFIX);
      store.putFeature((char*) squaredFeatKey.c_str(), squaredFeatSum, (int)ctf);

      termit->nextTerm();
    }
    delete termit;

  } else if (strcmp(argv[1], "run") == 0) {
    using namespace indri::collection;

    string dbstr = params["db"];
    string index = params["index"];
    int n_c = atoi(params["n_c"].c_str());
    int numShards = atoi(params["numShards"].c_str());

    // get list of shard statistic dbs
    vector<string> dbs;
    tokenize(dbstr, ":", &dbs);

    // get indri index
    Repository repo;
    repo.openRead(index);

    // initialize Taily ranker
    ShardRanker ranker(dbs, &repo, numShards, n_c);

    // get query file
    char* queryFile = getOption(argv, argv + argc, "-q");
    ifstream qfile;
    qfile.open(queryFile);

    string line;
    if (qfile.is_open()) {

      while (getline(qfile, line)) {
        char mutableLine[line.size() + 1];
        std::strcpy(mutableLine, line.c_str());

        char* qnum = std::strtok(mutableLine, ":");
        char* query = std::strtok(NULL, ":");

        vector<pair<int, double> > ranking;
        ranker.rank(query, &ranking);

        cout << qnum << ":" << query << endl;
        for(int i = 0; i < ranking.size(); i++) {
          cout << ranking[i].first << " " << ranking[i].second << endl;
        }
      }
      qfile.close();
    }


  } else {
    std::cout << "Unrecognized option." << std::endl;
    string dbPath = params["db"];
    string indexPath = params["index"];

    // create and open the data store
    FeatureStore store(dbPath, true);
    
    double val;
    cout << "love ";
    store.getFeature((char*)"love#f", &val);
    cout << val << " " ;
    store.getFeature((char*)"love#f2", &val);
    cout << val << endl;

    cout << "and ";
    store.getFeature((char*)"and#f", &val);
    cout << val << " " ;
    store.getFeature((char*)"and#f2", &val);
    cout << val << endl;

    cout << "orange ";
    store.getFeature((char*)"orange#f", &val);
    cout << val << " " ;
    store.getFeature((char*)"orange#d", &val);
    cout << val << " " ;
    store.getFeature((char*)"orange#f2", &val);
    cout << val << endl;

    cout << "apple ";
    store.getFeature((char*)"apple#f", &val);
    cout << val << " " ;
    store.getFeature((char*)"apple#d", &val);
    cout << val << " " ;
    store.getFeature((char*)"apple#f2", &val);
    cout << val << endl;
  }

  puts("Hello World!!!");

  return EXIT_SUCCESS;
}
