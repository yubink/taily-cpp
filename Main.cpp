#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <algorithm>
#include <string>
#include <db_cxx.h>
#include <boost/math/distributions/gamma.hpp>

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
	boost::math::gamma_distribution<> my_gamma(1, 1);
	boost::math::cdf(my_gamma, 0.5);

	if (strcmp(argv[1], "build") == 0) {
		char* paramFile = getOption(argv, argv + argc, "-p");

		ifstream file;
		file.open(paramFile);

		string line;
		if (file.is_open()) {
			while (getline(file, line)) {

				std::cout << line << endl;
			}
			file.close();
		}

		Db db(NULL, 0); // Instantiate the Db object

		u_int32_t oFlags = DB_CREATE | DB_EXCL; // Open flags;

		try {
			// Open the database
			db.open(NULL, // Transaction pointer
					"taily_stats.db", // Database file name
					NULL, // Optional logical database name
					DB_HASH, // Database access method
					oFlags, // Open flags
					0); // File mode (using defaults)
			// DbException is not subclassed from std::exception, so
			// need to catch both of these.
		} catch (DbException &e) {
			// Error handling code goes here
		} catch (std::exception &e) {
			// Error handling code goes here
		}


		char *stem = "whoop#min";
		float minval = 0.6;

		Dbt key(stem, strlen(stem) + 1);
		Dbt data(&minval, sizeof(float));

		int ret = db.put(NULL, &key, &data, DB_NOOVERWRITE);
		if (ret == DB_KEYEXIST) {
		    db.err(ret, "Put failed because key %s already exists",
		                    stem);
		}

		try {
		    db.close(0);
		} catch(DbException &e) {
		} catch(std::exception &e) {
		}


		indri::collection::Repository repo;

	} else if (strcmp(argv[1], "run") == 0) {
		Db db(NULL, 0); // Instantiate the Db object
		u_int32_t oFlags = DB_RDONLY;
		try {
			// Open the database
			db.open(NULL, // Transaction pointer
					"taily_stats.db", // Database file name
					NULL, // Optional logical database name
					DB_HASH, // Database access method
					oFlags, // Open flags
					0); // File mode (using defaults)
			// DbException is not subclassed from std::exception, so
			// need to catch both of these.
		} catch (DbException &e) {
			// Error handling code goes here
		} catch (std::exception &e) {
			// Error handling code goes here
		}


		char *stem = "whoop#min";
		float minval;

		Dbt key, data;

		key.set_data(stem);
		key.set_size(strlen(stem)+1);

		data.set_data(&minval);
		data.set_ulen(sizeof(float));
		data.set_flags(DB_DBT_USERMEM);

		db.get(NULL, &key, &data, 0);

		std::cout << "Data is " << minval << std::endl;

		try {
		    db.close(0);
		} catch(DbException &e) {
		} catch(std::exception &e) {
		}


	} else {
		std::cout << "Unrecognized option." << std::endl;
	}

	puts("Hello World!!!");

	return EXIT_SUCCESS;
}
