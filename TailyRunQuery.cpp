/*
 * TailyRunQuery.cpp
 *
 *  Created on: Nov 6, 2013
 *      Author: yubink
 */

#include <time.h>
#include "indri/QueryEnvironment.hpp"
#include "indri/LocalQueryServer.hpp"
#include "indri/delete_range.hpp"
#include "indri/NetworkStream.hpp"
#include "indri/NetworkMessageStream.hpp"
#include "indri/NetworkServerProxy.hpp"

#include "indri/ListIteratorNode.hpp"
#include "indri/ExtentInsideNode.hpp"
#include "indri/DocListIteratorNode.hpp"
#include "indri/FieldIteratorNode.hpp"

#include "indri/Parameters.hpp"

#include "indri/ParsedDocument.hpp"
#include "indri/Collection.hpp"
#include "indri/CompressedCollection.hpp"
#include "indri/TaggedDocumentIterator.hpp"
#include "indri/XMLNode.hpp"

#include "indri/QueryExpander.hpp"
#include "indri/RMExpander.hpp"
#include "indri/PonteExpander.hpp"
// need a QueryExpanderFactory....
#include "indri/TFIDFExpander.hpp"

#include "indri/IndriTimer.hpp"
#include "indri/UtilityThread.hpp"
#include "indri/ScopedLock.hpp"
#include "indri/delete_range.hpp"
#include "indri/SnippetBuilder.hpp"
#include "ShardRanker.h"

#include <queue>

static bool copy_parameters_to_string_vector(std::vector<std::string>& vec,
    indri::api::Parameters p, const std::string& parameterName) {
  if (!p.exists(parameterName))
    return false;

  indri::api::Parameters slice = p[parameterName];

  for (size_t i = 0; i < slice.size(); i++) {
    vec.push_back(slice[i]);
  }

  return true;
}

struct query_t {

  query_t(int _index, std::string _number, const std::string& _text,
      const std::string &queryType, std::vector<std::string> workSet,
      std::vector<std::string> FBDocs) :
      index(_index), number(_number), text(_text), qType(queryType), workingSet(
          workSet), relFBDocs(FBDocs) {
  }

  std::string number;
  int index;
  std::string text;
  std::string qType;
  // working set to restrict retrieval
  std::vector<std::string> workingSet;
  // Rel fb docs
  std::vector<std::string> relFBDocs;
};

class QueryClient {
private:

  indri::api::QueryEnvironment _environment;
  indri::api::Parameters& _parameters;
  int _requested;
  int _initialRequested;

  bool _printDocuments;
  bool _printPassages;
  bool _printSnippets;
  bool _printQuery;

  std::string _runID;
  bool _trecFormat;
  bool _inexFormat;

  indri::query::QueryExpander* _expander;
  std::vector<indri::api::ScoredExtentResult> _results;
  indri::api::QueryAnnotation* _annotation;

  std::stringstream _output;
  query_t* _query;
  std::vector<std::pair<std::string, double> > _scores;
  std::vector<std::string> _servers;
  std::vector<std::string> _indexes;

  // Runs the query, expanding it if necessary.  Will print output as well if verbose is on.
  void _runQuery(std::stringstream& output, const std::string& query,
      const std::string &queryType, const std::vector<std::string> &workingSet,
      std::vector<std::string> relFBDocs) {
    try {
      if (_printQuery)
        output << "# query: " << query << std::endl;
      std::vector<lemur::api::DOCID_T> docids;
      ;
      if (workingSet.size() > 0)
        docids = _environment.documentIDsFromMetadata("docno", workingSet);

      if (relFBDocs.size() == 0) {
        if (_printSnippets) {
          if (workingSet.size() > 0)
            _annotation = _environment.runAnnotatedQuery(query, docids,
                _initialRequested, queryType);
          else
            _annotation = _environment.runAnnotatedQuery(query,
                _initialRequested);
          _results = _annotation->getResults();
        } else {
          if (workingSet.size() > 0)
            _results = _environment.runQuery(query, docids, _initialRequested,
                queryType);
          else
            _results = _environment.runQuery(query, _initialRequested,
                queryType);
        }
      }

      if (_expander) {
        std::vector<indri::api::ScoredExtentResult> fbDocs;
        if (relFBDocs.size() > 0) {
          docids = _environment.documentIDsFromMetadata("docno", relFBDocs);
          for (size_t i = 0; i < docids.size(); i++) {
            indri::api::ScoredExtentResult r(0.0, docids[i]);
            fbDocs.push_back(r);
          }
        }
        std::string expandedQuery;
        if (relFBDocs.size() != 0)
          expandedQuery = _expander->expand(query, fbDocs);
        else
          expandedQuery = _expander->expand(query, _results);
        if (_printQuery)
          output << "# expanded: " << expandedQuery << std::endl;
        if (workingSet.size() > 0) {
          docids = _environment.documentIDsFromMetadata("docno", workingSet);
          _results = _environment.runQuery(expandedQuery, docids, _requested,
              queryType);
        } else {
          _results = _environment.runQuery(expandedQuery, _requested,
              queryType);
        }
      }
    } catch (lemur::api::Exception& e) {
      _results.clear();
      LEMUR_RETHROW(e, "QueryThread::_runQuery Exception");
    }
  }

  void _getResultsRegion(std::string queryIndex, int start, int end) {
    std::vector<indri::api::ScoredExtentResult> resultSubSet;
    resultSubSet.assign(_results.begin() + start, _results.begin() + end);
    const std::vector<std::string>& documentNames =
        _environment.documentMetadata(resultSubSet, "docno");
    for (size_t i = 0; i < resultSubSet.size(); i++) {
      _scores.push_back(
          std::make_pair(documentNames[i], resultSubSet[i].score));
    }
  }

  void _printResultRegion(std::stringstream& output, std::string queryIndex,
      int start, int end) {
    std::vector<std::string> documentNames;
    std::vector<indri::api::ParsedDocument*> documents;

    std::vector<indri::api::ScoredExtentResult> resultSubset;

    resultSubset.assign(_results.begin() + start, _results.begin() + end);

    // Fetch document data for printing
    if (_printDocuments || _printPassages || _printSnippets) {
      // Need document text, so we'll fetch the whole document
      documents = _environment.documents(resultSubset);
      documentNames.clear();

      for (size_t i = 0; i < resultSubset.size(); i++) {
        indri::api::ParsedDocument* doc = documents[i];
        std::string documentName;

        indri::utility::greedy_vector<indri::parse::MetadataPair>::iterator iter =
            std::find_if(documents[i]->metadata.begin(),
                documents[i]->metadata.end(),
                indri::parse::MetadataPair::key_equal("docno"));

        if (iter != documents[i]->metadata.end())
          documentName = (char*) iter->value;

        // store the document name in a separate vector so later code can find it
        documentNames.push_back(documentName);
      }
    } else {
      // We only want document names, so the documentMetadata call may be faster
      documentNames = _environment.documentMetadata(resultSubset, "docno");
    }

    std::vector<std::string> pathNames;
    if (_inexFormat) {
      // retrieve path names
      pathNames = _environment.pathNames(resultSubset);
    }

    // Print results
    for (size_t i = 0; i < resultSubset.size(); i++) {
      int rank = start + i + 1;
      std::string queryNumber = queryIndex;

      if (_trecFormat) {
        // TREC formatted output: queryNumber, Q0, documentName, rank, score, runID
        output << queryNumber << " " << "Q0 " << documentNames[i] << " " << rank
            << " " << resultSubset[i].score << " " << _runID << std::endl;
      } else if (_inexFormat) {

        output << "    <result>" << std::endl << "      <file>"
            << documentNames[i] << "</file>" << std::endl << "      <path>"
            << pathNames[i] << "</path>" << std::endl << "      <rsv>"
            << resultSubset[i].score << "</rsv>" << std::endl << "    </result>"
            << std::endl;
      } else {
        // score, documentName, firstWord, lastWord
        output << resultSubset[i].score << "\t" << documentNames[i] << "\t"
            << resultSubset[i].begin << "\t" << resultSubset[i].end
            << std::endl;
      }

      if (_printDocuments) {
        output << documents[i]->text << std::endl;
      }

      if (_printPassages) {
        int byteBegin = documents[i]->positions[resultSubset[i].begin].begin;
        int byteEnd = documents[i]->positions[resultSubset[i].end - 1].end;
        output.write(documents[i]->text + byteBegin, byteEnd - byteBegin);
        output << std::endl;
      }

      if (_printSnippets) {
        indri::api::SnippetBuilder builder(false);
        output
            << builder.build(resultSubset[i].document, documents[i],
                _annotation) << std::endl;
      }

      if (documents.size())
        delete documents[i];
    }
  }

  void _printResults(std::stringstream& output, std::string queryNumber) {
    if (_inexFormat) {
      // output topic header
      output << "  <topic topic-id=\"" << queryNumber << "\">" << std::endl
          << "    <collections>" << std::endl
          << "      <collection>ieee</collection>" << std::endl
          << "    </collections>" << std::endl;
    }
    for (size_t start = 0; start < _results.size(); start += 50) {
      size_t end = std::min<size_t>(start + 50, _results.size());
      _printResultRegion(output, queryNumber, start, end);
    }
    if (_inexFormat) {
      output << "  </topic>" << std::endl;
    }
    delete _annotation;
    _annotation = 0;
  }

  void _getResults(std::string queryNumber) {
    for (size_t start = 0; start < _results.size(); start += 50) {
      size_t end = std::min<size_t>(start + 50, _results.size());
      _getResultsRegion(queryNumber, start, end);
    }
  }

public:
  QueryClient(indri::api::Parameters& params) :
      _parameters(params), _expander(0), _annotation(0) {
  }

  ~QueryClient() {
  }

  // default value is -1 ie. csi-client is not initiated.
  void initialize(int csidepth = -1) {
    _environment.setSingleBackgroundModel(
        _parameters.get("singleBackgroundModel", false));

    std::vector<std::string> stopwords;
    if (copy_parameters_to_string_vector(stopwords, _parameters,
        "stopper.word"))
      _environment.setStopwords(stopwords);

    std::vector<std::string> smoothingRules;
    if (copy_parameters_to_string_vector(smoothingRules, _parameters, "rule"))
      _environment.setScoringRules(smoothingRules);

    if (_parameters.exists("maxWildcardTerms"))
      _environment.setMaxWildcardTerms(
          _parameters.get("maxWildcardTerms", 100));

    _requested = csidepth == -1 ? _parameters.get("count", 1000) : csidepth;
    _initialRequested = _parameters.get("fbDocs", _requested);
    _runID = _parameters.get("runID", "indri");
    _trecFormat = _parameters.get("trecFormat", false);
    _inexFormat = _parameters.exists("inex");

    _printQuery = _parameters.get("printQuery", false);
    _printDocuments = _parameters.get("printDocuments", false);
    _printPassages = _parameters.get("printPassages", false);
    _printSnippets = _parameters.get("printSnippets", false);

    if (_parameters.exists("baseline")) {
      // doing a baseline
      std::string baseline = _parameters["baseline"];
      _environment.setBaseline(baseline);
      // need a factory for this...
      if (_parameters.get("fbDocs", 0) != 0) {
        // have to push the method in...
        std::string rule = "method:" + baseline;
        _parameters.set("rule", rule);
        _expander = new indri::query::TFIDFExpander(&_environment, _parameters);
      }
    } else {
      if (_parameters.get("fbDocs", 0) != 0) {
        _expander = new indri::query::RMExpander(&_environment, _parameters);
      }
    }

    if (_parameters.exists("maxWildcardTerms")) {
      _environment.setMaxWildcardTerms(
          (int) _parameters.get("maxWildcardTerms"));
    }

  }

  void clear() {
    delete _expander;
    _expander = 0;

    _results.clear();
    _scores.clear();

    //_environment.close();
  }

  void addServer(const std::string& server) {
    _servers.push_back(server);
    _environment.addServer(server);
  }

  void addIndex(const std::string& index) {
    _indexes.push_back(index);
    _environment.addIndex(index);
  }

  void removeExistingServers() {
    for (int i = 0; i < _servers.size(); i++)
      _environment.removeServer(_servers[i]);
    for (int i = 0; i < _indexes.size(); i++)
      _environment.removeIndex(_indexes[i]);
    // _servers.size();
    _servers.clear();
    _indexes.clear();
  }

  bool execute(query_t* query) {
    // run the query
    try {
      _query = query;
      if (_parameters.exists("baseline")
          && ((query->text.find("#") != std::string::npos)
              || (query->text.find(".") != std::string::npos))) {
        LEMUR_THROW( LEMUR_PARSE_ERROR,
            "Can't run baseline on this query: " + query->text + "\nindri query language operators are not allowed.");
      }
      _runQuery(_output, query->text, query->qType, query->workingSet,
          query->relFBDocs);
      return true;
    } catch (lemur::api::Exception& e) {
      std::cout << "# EXCEPTION in query " << query->number << ": " << e.what()
          << std::endl;
      return false;
    }
    return false;
  }

  string printResults(query_t* query) {
    // clear the previous stream
    _output.clear();

    // print the results to the output stream
    _printResults(_output, query->number);
    return _output.str();
  }

  std::vector<std::pair<std::string, double> > getResults(query_t* query) {
    _getResults(query->number);
    return _scores;
  }

};

void push_queue(std::queue<query_t*>& q, indri::api::Parameters& queries,
    int queryOffset) {

  for (size_t i = 0; i < queries.size(); i++) {
    std::string queryNumber;
    std::string queryText;
    std::string queryType = "indri";
    if (queries[i].exists("type"))
      queryType = (std::string) queries[i]["type"];
    if (queries[i].exists("text"))
      queryText = (std::string) queries[i]["text"];
    if (queries[i].exists("number")) {
      queryNumber = (std::string) queries[i]["number"];
    } else {
      int thisQuery = queryOffset + int(i);
      std::stringstream s;
      s << thisQuery;
      queryNumber = s.str();
    }
    if (queryText.size() == 0)
      queryText = (std::string) queries[i];

    // working set and RELFB docs go here.
    // working set to restrict retrieval
    std::vector<std::string> workingSet;
    // Rel fb docs
    std::vector<std::string> relFBDocs;
    copy_parameters_to_string_vector(workingSet, queries[i], "workingSetDocno");
    copy_parameters_to_string_vector(relFBDocs, queries[i], "feedbackDocno");

    q.push(
        new query_t(i, queryNumber, queryText, queryType, workingSet,
            relFBDocs));

  }
}

bool cmp_func(const std::pair<double, int>& A,
    const std::pair<double, int>& B) {
  return A.first > B.first;
}

// <0, host:port> or <1, index-path> ie, 0 is for server and 1 is for daemon
// -1 is invalid
void load_daemons(indri::api::Parameters& p,
    std::map<int, std::pair<std::string, int> >& daemons) {

  for (size_t i = 0; i < p.size(); i++) {
    indri::api::Parameters thisDaemon = p[i];

    if (!thisDaemon.exists("shard"))
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR, "Missing shard parameter");
    int shard = thisDaemon["shard"];

    if (thisDaemon.exists("index"))
      daemons[shard] = std::make_pair(std::string(thisDaemon["index"]), 1);
    else if (thisDaemon.exists("server"))
      daemons[shard] = std::make_pair(std::string(thisDaemon["server"]), 0);
    else
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR,
          "Missing index or server parameter");
  }
}

// 0th shard is the corpus wide statistics db
void load_dbs(indri::api::Parameters& p,
    std::vector<std::string>& dbs) {

  std::map<int, std::string> dbMapping;

  for (size_t i = 0; i < p.size(); i++) {
    int shard = p[i]["shard"];
    std::string path = p[i]["path"];
    dbMapping[shard] = path;
  }

  // shard numbers start at 1
  for (size_t i = 1; i < p.size()+1; i++) {
    dbs.push_back(dbMapping[i]);
  }
}

double getTime() {
  struct timeval tim;
  gettimeofday(&tim, NULL);
  return tim.tv_sec + tim.tv_usec / 1e6;

}

int main(int argc, char * argv[]) {
  try {
    indri::api::Parameters& param = indri::api::Parameters::instance();
    param.loadCommandLine(argc, argv);

    if (param.get("version", 0)) {
      std::cout << INDRI_DISTRIBUTION << std::endl;
    }

    if (!param.exists("query"))
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR,
          "Must specify at least one query.");

    if (param.exists("baseline") && param.exists("rule"))
      LEMUR_THROW( LEMUR_BAD_PARAMETER_ERROR,
          "Smoothing rules may not be specified when running a baseline.");

    if (!param.exists("sampleIndex"))
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR, "Must specify a indri index for term processing.");

    // trec output
    if (!param.exists("trecOutput"))
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR,
          "Must specify a trecOutput file");

    if (!param.exists("n_c"))
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR,
          "Must specify taily parameter n_c" );

    if (!param.exists("db"))
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR,
          "Must specify taily statistics dbs" );

    if (!param.exists("v"))
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR,
          "Must specify taily parameter v" );

    if (!param.exists("corpusDb"))
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR,
          "Must specify corpus-wide statistics db" );

    std::string trecOutput = param["trecOutput"];
    std::fstream trecout(trecOutput.c_str(), std::ios::out);

    // read the mapping <shard-daemon> from the parameter file
    std::map<int, std::pair<std::string, int> > daemons;
    indri::api::Parameters parameterDaemons = param["daemon"];
    load_daemons(parameterDaemons, daemons);

    // load some parameters for Taily
    int n_c = param.get("n_c");
    int v = param.get("v");

    // sample index used by Taily to do term tokenization/stem/stopping
    indri::collection::Repository sampleRepo;
    sampleRepo.open(param.get("sampleIndex"));

    // load taily statistics dbs; dbs[0] is corpus wide and the rest are shards
    std::string corpusDb = param.get("corpusDb");
    std::vector<std::string> dbs;
    dbs.push_back(corpusDb);
    indri::api::Parameters paramDbs = param["db"];
    load_dbs(paramDbs, dbs);

    // push all queries onto a queue
    std::queue<query_t*> queries;
    indri::api::Parameters parameterQueries = param["query"];
    int queryOffset = param.get("queryOffset", 0);
    push_queue(queries, parameterQueries, queryOffset);

    // Query client for CSI retrieval
    double start = getTime();
    std::cout << start << std::endl;

    // initialize shard ranker
    ShardRanker ranker(dbs, &sampleRepo, n_c);

    std::cout << getTime() - start << std::endl;

    while (!queries.empty()) {
      double start = getTime();

      query_t* query = queries.front();

      std::vector<std::pair<int, double> > ranking;
      ranker.rank(query->text, &ranking);

      std::cout << "Taily Ranking done : " << getTime() - start << std::endl; // csi-retr

      double shardRankStart = getTime();

      std::vector<int> shards;
      for(size_t i = 0; i < ranking.size(); i++) {
        if (ranking[i].second > v) {
          shards.push_back(ranking[i].first);
        } else {
          break;
        }
      }

      std::cout << "Ranking done : " << getTime() - start << std::endl; // shard-ranking
      std::cout << "Time taken to rank shards " << getTime() - shardRankStart
          << std::endl; // shard-ranking

      // Query client for shard retrievals
      QueryClient* client = new QueryClient(param);
      client->initialize();

      if (shards.size() == 0) {
        std::cout << "No shards selected!" << std::endl;
        return 0;
      }

      double shardRetrievalStart = getTime();
      int cnt = 0;
      for (int i = 0; i < shards.size(); i++) {
        int shardId = shards[i];
        if (daemons.count(shardId) == 0)
          continue;

        // server-value is 0
        std::cout << shardId << " " << daemons[shardId].first << std::endl;
        daemons[shardId].second == 0 ?
            client->addServer(daemons[shardId].first) :
            client->addIndex(daemons[shardId].first);
        cnt++;
      }

      client->execute(query);
      //            trecout << query->text << " " << query->number << "\n";

      trecout << client->printResults(query);
      std::cout << "Shard Retrieval done : " << getTime() - start << "\n\n"; // shard-retr
      std::cout << "Time taken to do the shard retreival "
          << getTime() - shardRetrievalStart << "\n\n"; // shard-retr

      client->clear();
      client->removeExistingServers();
      delete client;

      queries.pop();
    }
    std::cout << getTime() << std::endl;

  } catch (lemur::api::Exception& e) {
    LEMUR_ABORT(e);
  } catch (...) {
    std::cout << "Caught unhandled exception" << std::endl;
    return -1;
  }

  return 0;
}

