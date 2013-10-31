/*
 * ShardRanker.cpp
 *
 *  Created on: Oct 23, 2013
 *      Author: yubink
 */

#include "ShardRanker.h"

ShardRanker::ShardRanker() {
  // TODO Auto-generated constructor stub

}

ShardRanker::~ShardRanker() {
  // TODO Auto-generated destructor stub
}

void ShardRanker::rank(string query) {
    char mutableLine[query.size() + 1];
    std::strcpy(mutableLine, query.c_str());

    char* value = std::strtok(mutableLine, " ");
    while (value != NULL) {
      value = std::strtok(NULL, " ");
      string term(value);
      string processed = repo->processTerm(term);


    }
}

