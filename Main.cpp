#include <stdio.h>
#include <stdlib.h>
#include <algorithm>

#include "indri/QueryEnvironment.hpp"
#include "indri/Repository.hpp"
#include "indri/CompressedCollection.hpp"

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

  if (strcmp(argv[0], "build") == 0) {
    char* paramFile = getOption(argv, argv + argc, "-p");

    ifstream file;
    file.open(paramFile);

    string line;
    if (file.is_open())
    {
      while ( getline (file, line) )
      {

        std::cout << line << endl;
      }
      file.close();
    }

    indri::collection::Repository repo;


  } else if (strcmp(argv[0], "run") == 0) {

  } else {
    std::cout << "Unrecognized option." << std::endl;
  }

  puts("Hello World!!!");

  return EXIT_SUCCESS;
}
