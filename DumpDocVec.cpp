#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <algorithm>
#include <string>
#include <vector>
#include <set>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include "indri/QueryEnvironment.hpp"
#include "indri/Repository.hpp"
#include "indri/CompressedCollection.hpp"
#include "indri/ScopedLock.hpp"

using namespace indri::index;
using namespace indri::collection;
using namespace indri::api;

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

int main(int argc, char * argv[]) {
  int MU = 2500;

  string outDir(getOption(argv, argv + argc, "-o"));

  // open indri index
  char* indexPath = getOption(argv, argv + argc, "-i");
  Repository repo;
  repo.openRead(indexPath);

  // get file with list of terms we're interested in
  char* termFile = getOption(argv, argv + argc, "-t");
  vector<string> whitelist;

  // tokenize the term whitelist and process the term
  ifstream file;
  file.open(termFile);
  string line;
  if (file.is_open()) {

    while (getline(file, line)) {
      whitelist.push_back(repo.processTerm(line));
    }
    file.close();
  }

  indri::collection::Repository::index_state state = repo.indexes();
  if (state->size() > 1) {
    cout << "Index has more than 1 part. Can't deal with this, man.";
    exit(EXIT_FAILURE);
  }
  Index* index = (*state)[0];
  indri::thread::ScopedLock(index->iteratorLock());

  // output term counts; collection size and ctf for all whitelist terms
  ofstream termStats((outDir+"/termStats").c_str());
  if (!termStats.is_open()) {
    cerr << "Can't open termStats file in " << outDir << endl;
    return 1;
  }

  termStats << index->termCount() << endl;
  for (uint i = 0; i < whitelist.size(); i++) {
    termStats << whitelist[i] << "\t" << index->termCount(whitelist[i]) << endl;
  }
  termStats.close();

  ofstream docsOut((outDir + "/docVecs").c_str());
  if (!docsOut.is_open()) {
    cerr << "Can't open docVecs file in " << outDir << endl;
    return 1;
  }

  // get the document vector iterator for this index
  TermListFileIterator* termlistIt = index->termListFileIterator();
  termlistIt->startIteration();

  lemur::api::DOCID_T intDocno = 0;
  std::map<int, std::string> termIDStringMap;

  // dump document vectors
  while(!termlistIt->finished()) {
    intDocno++;

    // construct document vector object from extracted termlist
    TermList* termList = termlistIt->currentEntry();
    DocumentVector result(index, termList, termIDStringMap);

    vector<int> pos = result.positions();
    vector<string> stems = result.stems();

    // initialize map for counting stems in this doc (only care about whitelist terms)
    std::map<std::string, int> stemCount;
    for(vector<string>::iterator termIt = whitelist.begin(); termIt != whitelist.end(); termIt++) {
      stemCount[*termIt] = 0;
    }

    // count up stems
    bool hasWhitelist = false;
    for(vector<int>::iterator posIt = pos.begin(); posIt != pos.end(); posIt++) {
      string& stem = stems[(*posIt)];
      if (stemCount.find(stem) != stemCount.end()) {
        hasWhitelist = true;
        stemCount[stem] += 1;
      }
    }

    // if document has at least whitelist stem
    if (hasWhitelist) {
      // output external docno
      string extDocNum = repo.collection()->retrieveMetadatum(intDocno, "docno");
      docsOut << extDocNum << "\t";

      // output doc length
      docsOut << pos.size() << "\t";

      // go through words of interest and output counts (that are bigger than 0)
      for(vector<string>::iterator termIt = whitelist.begin(); termIt != whitelist.end(); termIt++) {
        std::map<std::string, int>::iterator loc = stemCount.find(*termIt);
        if (loc != stemCount.end() && (*loc).second > 0) {
          docsOut << *termIt << ":" << stemCount[*termIt] << " ";
        }
      }
      docsOut << endl;
    }

    termlistIt->nextEntry();
  }

  docsOut.close();
}
