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
using namespace lemur::api;

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

  indri::collection::Repository::index_state state = repo.indexes();
  if (state->size() > 1) {
    cout << "Index has more than 1 part. Can't deal with this, man.";
    exit(EXIT_FAILURE);
  }
  Index* index = (*state)[0];

  // get file with list of terms we're interested in
  char* termFile = getOption(argv, argv + argc, "-t");
  vector<TERMID_T> whitelist;

  // keep track of which term ids map to which stems
  std::map<int, std::string> termIDStringMap;

  // tokenize the term whitelist and process the term
  ifstream file;
  file.open(termFile);
  string line;
  if (file.is_open()) {

    while (getline(file, line)) {
      // stem/stop terms (depending on index settings)
      string stem = repo.processTerm(line);

      // skip stopwords
      if (stem.length() <= 0) {
        continue;
      }

      // convert stem to termID and keep track of mapping
      TERMID_T termID = index->term(stem);
      termIDStringMap[termID] = stem;
      whitelist.push_back(termID);
    }
    file.close();
  }

  indri::thread::ScopedLock(index->iteratorLock());

  // output term counts; collection size and ctf for all whitelist terms
  ofstream termStats((outDir+"/termStats").c_str());
  if (!termStats.is_open()) {
    cerr << "Can't open termStats file in " << outDir << endl;
    return 1;
  }

  termStats << index->termCount() << endl;
  for (uint i = 0; i < whitelist.size(); i++) {
    string& currTerm = termIDStringMap[whitelist[i]];
    termStats << currTerm << "\t" << index->termCount(currTerm) << endl;
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

  // dump document vectors
  while(!termlistIt->finished()) {
    intDocno++;

    // construct document vector object from extracted termlist
    TermList* termList = termlistIt->currentEntry();
    const indri::utility::greedy_vector<int>& pos = termList->terms();

    // initialize map for counting stems in this doc (only care about whitelist terms)
    std::map<TERMID_T, int> stemCount;
    for(vector<TERMID_T>::iterator termIt = whitelist.begin(); termIt != whitelist.end(); termIt++) {
      stemCount[*termIt] = 0;
    }

    // count up stems
    bool hasWhitelist = false;
    for(size_t i = 0; i < pos.size(); ++i) {
      std::map<TERMID_T, int>::iterator loc = stemCount.find(pos[i]);
      if (loc != stemCount.end()) {
        hasWhitelist = true;
        loc->second += 1;
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
      for(vector<TERMID_T>::iterator termIt = whitelist.begin(); termIt != whitelist.end(); termIt++) {
        std::map<TERMID_T, int>::iterator loc = stemCount.find(*termIt);
        if (loc != stemCount.end() && (*loc).second > 0) {
          docsOut << termIDStringMap[*termIt] << ":" << stemCount[*termIt] << " ";
        }
      }
      docsOut << endl;
    }

    termlistIt->nextEntry();
  }

  docsOut.close();
}
