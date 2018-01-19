#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <algorithm>
#include <string>
#include <vector>
#include <set>
#include <db_cxx.h>
#include <boost/math/distributions/gamma.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/unordered_map.hpp>

#include "indri/QueryEnvironment.hpp"
#include "indri/Repository.hpp"
#include "indri/CompressedCollection.hpp"
#include "indri/ScopedLock.hpp"

#include "FeatureStore.h"
#include "ShardRanker.h"

using namespace indri::index;
using namespace indri::collection;

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

struct shard_data {
  double min;
  double shardDf;
  double f;
  double f2;

  shard_data(): min(DBL_MAX), shardDf(0.0), f(0.0), f2(0.0) {};
};

void storeTermStats(FeatureStore* store, string term, int ctf, double min,
    double shardDf, double f, double f2) {
  // store min feature for term (for this shard; will later be merged into corpus-wide Db)
  string minFeatKey(term);
  minFeatKey.append(FeatureStore::MIN_FEAT_SUFFIX);
  store->putFeature((char*)minFeatKey.c_str(), min, ctf);

  // get and store shard df feature for term
  string dfFeatKey(term);
  dfFeatKey.append(FeatureStore::SIZE_FEAT_SUFFIX);
  store->putFeature((char*)dfFeatKey.c_str(), shardDf, ctf);

  // store sum f
  string featKey(term);
  featKey.append(FeatureStore::FEAT_SUFFIX);
  store->putFeature((char*) featKey.c_str(), f, ctf);

  // store sum f^2
  string squaredFeatKey(term);
  squaredFeatKey.append(FeatureStore::SQUARED_FEAT_SUFFIX);
  store->putFeature((char*) squaredFeatKey.c_str(), f2, ctf);
}

// innards of buildshard
void collectShardStats(DocListIterator* docIter, TermData* termData, FeatureStore* corpusStats,
    FeatureStore* store, Index* index, double totalTermCount) {
  // get ctf of term from corpus-wide stats Db
  double ctf;
  string ctfKey(termData->term);
  ctfKey.append(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
  corpusStats->getFeature((char*)ctfKey.c_str(), &ctf);

  double featSum = 0.0f;
  double squaredFeatSum = 0.0f;
  double minFeat = DBL_MAX;

  docIter->startIteration();

  // calculate Sum(f) and Sum(f^2) top parts of eq (3) (4)
  while (!docIter->finished()) {
    DocListIterator::DocumentData* doc = docIter->currentEntry();
    double length = index->documentLength(doc->document);
    double tf = doc->positions.size();

    // calulate Indri score feature and sum it up
    double feat = calcIndriFeature(tf, ctf, totalTermCount, length);
    featSum += feat;
    squaredFeatSum += pow(feat, 2);

    // keep track of this shard's minimum feature
    if (feat < minFeat) {
      minFeat = feat;
    }
    docIter->nextEntry();
  }

  double shardDf = termData->corpus.documentCount;
  storeTermStats(store, termData->term, (int)ctf, minFeat, shardDf, featSum, squaredFeatSum);
}

// innards of buildcorpus
void collectCorpusStats(DocListIterator* docIter, TermData* termData,
    FeatureStore* store) {
  double ctf = termData->corpus.totalCount;
  double df = termData->corpus.documentCount;

  // this seems pointless, but if I don't do this, it crashes.
  while (!docIter->finished()) {
    indri::index::DocListIterator::DocumentData* doc = docIter->currentEntry();
    docIter->nextEntry();
  }

  // store df feature for term
  string dfFeatKey(termData->term);
  dfFeatKey.append(FeatureStore::SIZE_FEAT_SUFFIX);
  store->addValFeature((char*) dfFeatKey.c_str(), df, (int) ctf);

  // store ctf feature for term
  string ctfFeatKey(termData->term);
  ctfFeatKey.append(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
  store->addValFeature((char*) ctfFeatKey.c_str(), ctf, (int) ctf);
}

void buildFromMap(std::map<string, string>& params) {

  vector<Repository*>::iterator rit;

  string dbPath = params["db"]; // in this case, this will be a path to a folder
  string indexstr = params["index"];

  int ram = 1500;
  if (params.find("ram") != params.end()) {
    ram = atoi(params["ram"].c_str());
  }

  vector<string> terms;
  if (params.find("terms") != params.end()) {
    tokenize(params["terms"], ":", &terms);
  }

  vector<string> mapFiles;
  if (params.find("mapFile") != params.end()) {
    tokenize(params["mapFile"], ":", &mapFiles);
  }

  // open all indri indexes; the 10/20 part indexes used
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

  // open up all output feature storages for each mapping file we are accessing
  vector<FeatureStore*> stores;

  // list of shard maps matching to each index
  vector<boost::unordered_map<int, int>*> shardMapList;
  for (int i = 0; i < indexes.size(); i++) {
	  shardMapList.push_back(new boost::unordered_map<int, int>());
  }

  vector<int> shardIds;

  // read in the mapping files given and construct a reverse mapping,
  // i.e. doc -> shard, and create FeatureStore dbs for each shard
  vector<string>::iterator it;
  for(it = mapFiles.begin(); it != mapFiles.end(); ++it) {

    // find the map file name, which is its shard id
    size_t loc = (*it).find_last_of('/') + 1;
    string shardIdStr = (*it).substr(loc);

    // create output directory for the feature store dbs
    const char* cPath = (dbPath+"/"+shardIdStr).c_str();
    if (mkdir(cPath,0777) == -1) {
      cerr << "Error creating output DB dir. Dir '" << cPath << "' may already exist." << endl;
      exit(EXIT_FAILURE);
    }

    // create feature store for shard
    FeatureStore* store = new FeatureStore(dbPath+"/"+shardIdStr, false, ram/mapFiles.size());
    stores.push_back(store);

    // grab shard id and create reverse mapping between doc -> shard
    // from contents in the file
    int shardId = atoi(shardIdStr.c_str());
    shardIds.push_back(shardId);

    // lines from the file should be in format of INDEX_ID.INTERNAL_DOC_NO
    int lineNum = 0;
    ifstream file;
    file.open((*it).c_str());
    string line;
    while (getline(file, line)) {
      int divisor = line.find('.');
      int indexId = atoi(line.substr(0, divisor).c_str());
      int docId = atoi(line.substr(divisor+1, string::npos).c_str());
      boost::unordered_map<int, int>* shardMap = shardMapList.at(indexId);
      (*shardMap)[docId] = shardId;
      ++lineNum;
    }
    file.close();

    // store the shard size (# of docs) feature
    string featSize(FeatureStore::SIZE_FEAT_SUFFIX);
    store->putFeature((char*) featSize.c_str(), (double) lineNum, lineNum);
  }

  cout << "Finished reading shard map." << endl;

  // get the total term length of the collection (for Indri scoring)
  double totalTermCount = 0;
  for (rit = indexes.begin(); rit != indexes.end(); ++rit) {
    // if it has more than one index, quit
    Repository::index_state state = (*rit)->indexes();
    if (state->size() > 1) {
      cout << "Index has more than 1 part. Can't deal with this, man.";
      exit(EXIT_FAILURE);
    }
    Index* index = (*state)[0];
    totalTermCount += index->termCount();
  }
//  string totalTermCountKey(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
//  corpusStats.getFeature((char*) totalTermCountKey.c_str(),
//      &totalTermCount);

  // only create shard statistics for specified terms
  set<string> stemsSeen;
  int termCnt = 0;
  for (it = terms.begin(); it != terms.end(); ++it) {

    termCnt++;
    if (termCnt % 100 == 0) {
      cout << "  Finished " << termCnt << " terms" << endl;
    }

    // stemify term
    string stem = (indexes[0])->processTerm(*it);
    if (stemsSeen.find(stem) != stemsSeen.end()) {
      continue;
    }
    stemsSeen.insert(stem);
    cout << "Processing: " << (*it) << " (" << stem << ")" << endl;

    // if this is a stopword, skip
    if (stem.size() == 0)
      continue;

    // get term ctf
    double ctf = 0;
//    string ctfKey(stem);
//    ctfKey.append(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
//    corpusStats.getFeature((char*) ctfKey.c_str(), &ctf);
    for (rit = indexes.begin(); rit != indexes.end(); ++rit) {
      // if it has more than one index, quit
      Repository::index_state state = (*rit)->indexes();
      if (state->size() > 1) {
        cout << "Index has more than 1 part. Can't deal with this, man.";
        exit(EXIT_FAILURE);
      }
      Index* index = (*state)[0];
      totalTermCount += index->termCount();

      DocListIterator* docIter = index->docListIterator(stem);

      // term not found
      if (docIter == NULL) continue;

      docIter->startIteration();
      TermData* termData = docIter->termData();
      ctf += termData->corpus.totalCount;
      delete docIter;
    }

    //track df for this term for each shard; initialize
    boost::unordered_map<int, shard_data> shardDataMap;

    // for each index
    int idxCnt = 0;
    for (rit = indexes.begin(); rit != indexes.end(); ++rit) {
      indri::collection::Repository::index_state state = (*rit)->indexes();
      if (state->size() > 1) {
        cout << "Index has more than 1 part. Can't deal with this, man.";
        exit(EXIT_FAILURE);
      }
      Index* index = (*state)[0];

      // get inverted list iterator for this index
      DocListIterator* docIter = index->docListIterator(stem);

      // term not found
      if (docIter == NULL)
        continue;

      // go through each doc in index containing the current term
      // calculate Sum(f) and Sum(f^2) top parts of eq (3) (4)
      for (docIter->startIteration(); !docIter->finished(); docIter->nextEntry()) {
        DocListIterator::DocumentData* doc = docIter->currentEntry();

        // find the shard id, if this doc belongs to any
        boost::unordered_map<int, int>::iterator smit = shardMapList[idxCnt]->find(doc->document);
        if (smit == shardMapList[idxCnt]->end()) continue;
        int currShardId = (*smit).second;

        double length = index->documentLength(doc->document);
        double tf = doc->positions.size();

        // calulate Indri score feature and sum it up
        double feat = calcIndriFeature(tf, ctf, totalTermCount, length);
        shard_data & currShard = shardDataMap[currShardId];
        currShard.f += feat;
        currShard.f2 += pow(feat,2);
        currShard.shardDf += 1;
         
        if (feat < currShard.min) {
          currShard.min = feat;
        }
      } // end doc iter
      // free iterator to save RAM!
      delete docIter;

      cout << "  Index #" << idxCnt << endl;
      ++idxCnt;
    } // end index iter

    // add term info to correct shard dbs
    for (int i = 0; i < shardIds.size(); i++)
    {
      int shardId = shardIds[i];
      // don't store empty terms
      if (shardDataMap[shardId].shardDf == 0) continue;
      storeTermStats(stores[i], stem, (int)ctf, shardDataMap[shardId].min,
          shardDataMap[shardId].shardDf, shardDataMap[shardId].f,
          shardDataMap[shardId].f2);

    }

  } // end term iter

  // clean up
  vector<FeatureStore*>::iterator fit;
  for (fit = stores.begin(); fit != stores.end(); ++fit) {
    delete (*fit);
  }

  for (rit = indexes.begin(); rit != indexes.end(); ++rit) {
    (*rit)->close();
    delete (*rit);
  }

  vector<boost::unordered_map<int, int>*>::iterator mit;
  for (mit = shardMapList.begin(); mit != shardMapList.end(); ++mit) {
	delete (*mit);
  }
}

void buildShard(std::map<string, string>& params) {

  string dbPath = params["db"];
  string indexPath = params["index"];
  string corpusDbPath = params["corpusDb"];

  int ram = 2000;
  if (params.find("ram") != params.end()) {
    ram = atoi(params["ram"].c_str());
  }

  vector<string> terms;
  if (params.find("terms") != params.end()) {
    tokenize(params["terms"], ":", &terms);
  }

  // open corpus statistics db
  FeatureStore corpusStats(corpusDbPath, true);

  // create and open the data store
  FeatureStore store(dbPath, false, ram);

  indri::collection::Repository repo;
  repo.openRead(indexPath);

  indri::collection::Repository::index_state state = repo.indexes();

  if (state->size() > 1) {
    cout << "Index has more than 1 part. Can't deal with this, man.";
    exit(EXIT_FAILURE);
  }

  for(size_t i = 0; i < state->size(); i++) {
    Index* index = (*state)[i];
    indri::thread::ScopedLock( index->iteratorLock() );

    cout << index->termCount() << " " << index->documentCount() << endl;

    // get the total term length of the collection (for Indri scoring)
    double totalTermCount = index->termCount();
    string totalTermCountKey(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
    corpusStats.getFeature((char*)totalTermCountKey.c_str(), &totalTermCount);

    // store the shard size (# of docs) feature
    int shardSizeFeat = index->documentCount();
    string featSize(FeatureStore::SIZE_FEAT_SUFFIX);
    store.putFeature((char*)featSize.c_str(), (double)shardSizeFeat, shardSizeFeat);

    // if there are no term constraints, build all terms
    if (terms.size() == 0) {
      DocListFileIterator* iter = index->docListFileIterator();
      iter->startIteration();

      int termCnt = 0;
      // for each stem in the index
      while (!iter->finished()) {
        termCnt++;
        if (termCnt % 100000 == 0) {
          cout << "  Finished " << termCnt << " terms" << endl;
        }

        DocListFileIterator::DocListData* entry = iter->currentEntry();
        TermData* termData = entry->termData;
        entry->iterator->startIteration();

        collectShardStats(entry->iterator, termData, &corpusStats, &store,
            index, totalTermCount);

        iter->nextEntry();
      }
      delete iter;

    } else {
      // only create shard statistics for specified terms
      set<string> stemsSeen;
      vector<string>::iterator it;
      int termCnt = 0;
      for (it = terms.begin(); it != terms.end(); ++it) {
        termCnt++;
        if (termCnt % 100 == 0) {
          cout << "  Finished " << termCnt << " terms" << endl;
        }
        // stemify term
        string stem = repo.processTerm(*it);
        if (stemsSeen.find(stem) != stemsSeen.end()) {
          continue;
        }
        stemsSeen.insert(stem);

        // if this is a stopword, skip
        if (stem.size() == 0) continue;

        DocListIterator* docIter = index->docListIterator(stem);

        // term not found
        if (docIter == NULL) continue;

        docIter->startIteration();
        TermData* termData = docIter->termData();
        collectShardStats(docIter, termData, &corpusStats, &store,
            index, totalTermCount);
      }

    }

  }
}

void buildCorpus(std::map<string, string>& params) {
  using namespace indri::collection;
  using namespace indri::index;

  string dbPath = params["db"];
  string indexstr = params["index"];

  int ram = 8000;
  if (params.find("ram") != params.end()) {
    ram = atoi(params["ram"].c_str());
  }

  vector<string> terms;
  if (params.find("terms") != params.end()) {
    tokenize(params["terms"], ":", &terms);
  }

  FeatureStore store(dbPath, false, ram);
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

  int idxCnt = 1;
  vector<Repository*>::iterator it;
  for (it = indexes.begin(); it != indexes.end(); ++it) {
    cout << "Starting index " << idxCnt++ << endl;

    // if it has more than one index, quit
    Repository::index_state state = (*it)->indexes();
    if (state->size() > 1) {
      cout << "Index has more than 1 part. Can't deal with this, man.";
      exit(EXIT_FAILURE);
    }
    Index* index = (*state)[0];

    // add the total term length of shard
    totalTermCount += index->termCount();
    // add the shard size (# of docs)
    totalDocCount += index->documentCount();

    if (terms.size() == 0) {
      DocListFileIterator* iter = index->docListFileIterator();
      iter->startIteration();

      int termCnt = 0;
      // go through all terms in the index and collect df/ctf
      while (!iter->finished()) {
        termCnt++;
        if (termCnt % 100000 == 0) {
          cout << "  Finished " << termCnt << " terms" << endl;
        }

        DocListFileIterator::DocListData* entry = iter->currentEntry();
        TermData* termData = entry->termData;
        entry->iterator->startIteration();

        collectCorpusStats(entry->iterator, termData, &store);
        iter->nextEntry();
      }
      delete iter;

    } else {

      // only create shard statistics for specified terms
      set<string> stemsSeen;

      vector<string>::iterator tit;
      int termCnt = 0;
      for (tit = terms.begin(); tit != terms.end(); ++tit) {
        termCnt++;
        if (termCnt % 100 == 0) {
          cout << "  Finished " << termCnt << " terms" << endl;
        }
        // stemify term; make sure we're not doing this again!
        string stem = (*it)->processTerm(*tit);
        if (stemsSeen.find(stem) != stemsSeen.end()) {
          continue;
        }
        stemsSeen.insert(stem);

        // if this is a stopword, skip
        if (stem.size() == 0) continue;

        DocListIterator* docIter = index->docListIterator(stem);

        // term not found
        if (docIter == NULL) continue;

        docIter->startIteration();
        TermData* termData = docIter->termData();
        collectCorpusStats(docIter, termData, &store);
        delete docIter;
      }
    }
  }

  // add collection global features needed for shard ranking
  string totalTermKey(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
  store.putFeature((char*)totalTermKey.c_str(), totalTermCount, FeatureStore::FREQUENT_TERMS+1);
  string featSize(FeatureStore::SIZE_FEAT_SUFFIX);
  store.putFeature((char*)featSize.c_str(), totalDocCount, FeatureStore::FREQUENT_TERMS+1);
}

void mergeMin(std::map<string, string>& params) {
  string dbstr = params["db"];
  string index = params["index"];

  // get list of shard statistic dbs
  vector<string> dbs;
  tokenize(dbstr, ":", &dbs);

  // open dbs for the corpus store and each shard store
  FeatureStore corpusStore(dbs[0], false);
  vector<FeatureStore*> stores;
  for (uint i = 1; i < dbs.size(); i++) {
    stores.push_back(new FeatureStore(dbs[i], true));
  }

  int termCnt = 0;
  // iterate through the database for all terms and find gloabl min feature
  FeatureStore::TermIterator* termit = corpusStore.getTermIterator();
  while (!termit->finished()) {
    termCnt++;
    if(termCnt % 100000 == 0) {
      cout << "  Finished " << termCnt << " terms" << endl;
    }

    // get a stem and its ctf
    pair<string,double> termAndCtf = termit->currrentEntry();
    string stem = termAndCtf.first;
    double ctf = termAndCtf.second;

    // keep track of min feature
    double globalMin = DBL_MAX;
    string minFeatKey(stem);
    minFeatKey.append(FeatureStore::MIN_FEAT_SUFFIX);

    // for each shard, grab the share min feature from its stats db and find global min
    vector<FeatureStore*>::iterator it;
    for (it = stores.begin(); it != stores.end(); ++it) {
      double currMin;
      (*it)->getFeature((char*)minFeatKey.c_str(), &currMin);
      if (currMin < globalMin) {
        globalMin = currMin;
      }
    }

    // store min feature for term
    corpusStore.putFeature((char*)minFeatKey.c_str(), globalMin, (int)ctf);

    termit->nextTerm();
  }
  delete termit;

  vector<FeatureStore*>::iterator it;
  for (it = stores.begin(); it != stores.end(); ++it) {
    delete (*it);
  }
}

void run(std::map<string, string>& params, char* queryFile) {
  using namespace indri::collection;

  string dbstr = params["db"];
  string index = params["index"];
  int n_c = atoi(params["n_c"].c_str());

  // get list of shard statistic dbs
  vector<string> dbs;
  tokenize(dbstr, ":", &dbs);

  // get indri index
  Repository repo;
  repo.openRead(index);

  // initialize Taily ranker
  ShardRanker ranker(dbs, &repo, n_c);

  // get query file
  ifstream qfile;
  qfile.open(queryFile);

  string line;
  if (qfile.is_open()) {

    while (getline(qfile, line)) {
      char mutableLine[line.size() + 1];
      std::strcpy(mutableLine, line.c_str());

      char* qnum = std::strtok(mutableLine, ":");
      char* query = std::strtok(NULL, ":");

      vector<pair<string, double> > ranking;
      if (query) {
        ranker.rank(query, &ranking);
      }

      cout << qnum << "\t" << query << endl;
      for(int i = 0; i < ranking.size(); i++) {
        cout << ranking[i].first << "\t" << ranking[i].second << endl;
      }
      cout << endl;
    }
    qfile.close();
  }
}

void buildFromDV(std::map<string, string>& params) {

  string dbPath = params["db"]; //path to where taily dbs will be created
  string dvFile = params["dvFile"];
  string corpusStatsFile = params["corpusStatsFile"];

  // shard mapping file sorted by docno
  string mapFile = params["mapFile"];

  // number of shards; shard names start from 1.
  int numShards = atoi(params["numShards"].c_str());

  int ram = 2000;
  if (params.find("ram") != params.end()) {
    ram = atoi(params["ram"].c_str());
  }

  // keep track of shard statistics
  map<int, map<string, shard_data> > shardData;
  for (int i = 1; i <= numShards; ++i) {
    shardData.insert(
        std::pair<int, map<string, shard_data> >(i, map<string, shard_data>()));
  }

  // read in the term corpus statistics
  map<string, int> termStats;

  ifstream statsFile;
  statsFile.open(corpusStatsFile.c_str());
  string line;
  if (!statsFile.is_open()) {
    cerr << "Couldn't open term stats file" << endl;
    exit(EXIT_FAILURE);
  }
  // first line is collection term size
  getline(statsFile, line);
  long totalTermCount = atol(line.c_str());

  // all subsequent lines are terms and their term counts
  while (getline(statsFile, line)) {
    vector<string> pair;
    tokenize(line, "\t", &pair);
    termStats[pair[0]] = atoi(pair[1].c_str());

    // initialize per-shard taily stats gathering data structure
    for (int i = 1; i <= numShards; ++i) {
      shardData[i].insert(std::pair<string, shard_data>(pair[0], shard_data()));
    }
  }
  statsFile.close();


  // open document vectors file and mapping file
  ifstream docVecs;
  docVecs.open(dvFile.c_str());
  if (!docVecs.is_open()) {
    cerr << "Couldn't open document vector file" << endl;
    exit(EXIT_FAILURE);
  }

  ifstream mapping;
  mapping.open(mapFile.c_str());
  if (!mapping.is_open()) {
    cerr << "Couldn't open shard map file" << endl;
    exit(EXIT_FAILURE);
  }

  string mapline;
  getline(mapping, mapline);
  vector<string> mapPair;
  tokenize(mapline, "\t", &mapPair);

  // get the appropriate shard's data structure
  int shardNum = atoi(mapPair[1].c_str());

  // run through the document vector and document shard map file in parallel
  // and gather taily statistics for the docs in the right shard assignment
  string docVec;
  while (getline(docVecs, docVec) && mapping) {
    vector<string> triplet;
    tokenize(docVec, "\t", &triplet);
    string& docno = triplet[0];
    int doclen = atoi(triplet[1].c_str());

    // find the shard assignment of the current document
    while (mapPair[0].compare(docno) < 0 && mapping) {
      mapPair.clear();
      getline(mapping, mapline);
      tokenize(mapline, "\t", &mapPair);
    }

    // then document couldn't be found in shard map
    if (mapPair[0].compare(docno) != 0) {
      cerr << "Couldn't find assignment for doc " << docno << endl;
      continue;
    }

    // get shard number of curr doc and get map of taily stats for the shard
    shardNum = atoi(mapPair[1].c_str());
    map<int, map<string, shard_data> >::iterator shardloc = shardData.find(shardNum);

    if (shardloc == shardData.end()) {
      cerr << "Bad shard id " << mapline << endl;
      exit(EXIT_FAILURE);
    }

    // get term vector for doc
    vector<string> termVec;
    tokenize(triplet[2], " ", &termVec);

    for (uint i = 0; i < termVec.size(); ++i) {
      vector<string> featPair;
      tokenize(termVec[i], ":", &featPair);

      string& term = featPair[0];
      int tf = atoi(featPair[1].c_str());

      // get the taily stats gathering struct for this term
      map<string, shard_data>::iterator termloc = (*shardloc).second.find(term);
      if (termloc == (*shardloc).second.end()) {
        cerr << "Statistics for term missing: " << termVec[i] << "; in doc " << docVec << endl;
        exit(EXIT_FAILURE);
      }

      // calulate Indri score feature and gather the taily stats
      double feat = calcIndriFeature(tf, termStats[term], totalTermCount, doclen);

      (*termloc).second.shardDf += 1;
      (*termloc).second.f += feat;
      (*termloc).second.f2 += pow(feat, 2);

      // keep track of this shard's minimum feature
      if (feat < (*termloc).second.min) {
        (*termloc).second.min = feat;
      }
    }
  }
  docVecs.close();
  mapping.close();

  // store all collected statistics, for each shard and term
  for (int i = 1; i <= numShards; ++i) {
    char shardIdStr[126];
    sprintf(shardIdStr,"%d",i);

    // create output directory for the feature store dbs
    const char* cPath = (dbPath+"/"+shardIdStr).c_str();
    if (mkdir(cPath,0777) == -1) {
      cerr << "Error creating output DB dir. Dir may already exist." << endl;
      exit(EXIT_FAILURE);
    }

    // create feature store for shard
    FeatureStore store(cPath, false, ram);

    // for all terms, store the collected features
    map<int, map<string, shard_data> >::iterator currShardMap = shardData.find(i);
    for (map<string, shard_data>::iterator iter = (*currShardMap).second.begin();
        iter != (*currShardMap).second.end(); ++iter) {
      storeTermStats(&store, (*iter).first, termStats[(*iter).first],
          (*iter).second.min, (*iter).second.shardDf, (*iter).second.f,
          (*iter).second.f2);
    }
  }

}

int main(int argc, char * argv[]) {
  int MU = 2500;

  char* paramFile = getOption(argv, argv + argc, "-p");

  // read parameter file
  std::map<string, string> params;
  readParams(paramFile, &params);

  if (strcmp(argv[1], "buildfrommap") == 0) {
    buildFromMap(params);

  } else if (strcmp(argv[1], "buildfromdv") == 0) {
    // build taily corpus statistics from document vector file generated by DumpDocVec.cpp
    buildFromDV(params);

  } else if (strcmp(argv[1], "buildshard") == 0) {
    buildShard(params);

  } else if (strcmp(argv[1], "buildcorpus") == 0) {
    buildCorpus(params);

  } else if (strcmp(argv[1], "mergemin") == 0) {
    mergeMin(params);

  } else if (strcmp(argv[1], "run") == 0) {
    run(params, getOption(argv, argv + argc, "-q"));

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
    cout << "f "<< val << " " ;
    store.getFeature((char*)"orange#d", &val);
    cout << "d "<< val << " " ;
    store.getFeature((char*)"orange#t", &val);
    cout << "t "<< val << " " ;
    store.getFeature((char*)"orange#f2", &val);
    cout << "f2 "<< val << endl;
    cout << "orange min ";
    store.getFeature((char*)"orange#m", &val);
    cout << val << endl ;

    cout << "apple ";
    store.getFeature((char*)"apple#f", &val);
    cout << val << " " ;
    store.getFeature((char*)"apple#d", &val);
    cout << val << " " ;
    store.getFeature((char*)"apple#t", &val);
    cout << val << " " ;
    store.getFeature((char*)"apple#f2", &val);
    cout << val << endl;

    cout << "apple min ";
    store.getFeature((char*)"apple#m", &val);
    cout << val << endl ;
    store.getFeature((char*)"#d", &val);
    cout << "size " << val << " " ;
    store.getFeature((char*)"#t", &val);
    cout << "size " << val << endl;
  }

  return EXIT_SUCCESS;
}
