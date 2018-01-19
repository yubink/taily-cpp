taily-cpp
=========

C++ implementation of Taily, a resource selection algorithm for selective search. Requires Indri search engine, Berkeley DB, and Boost. Please don't judge me; this is research code and horrible. :(

## How to Build Taily 
Taily first needs to collect corpus statistics and create some data structures in order to be used in querying.

### From indexed shards
The first way is to partition the collection into the desired shards and to index the shards (with Indri) and collect Taily statistics from these indexes. See below for details on parameter files.

```
$./Taily buildcorpus -p PARAM_FILE
$./Taily buildshard -p PARAM_FILE 

```

### From shard map files (and an index of the entire collection)
The second way requires an index of the entire collection (may be split into multiple indicies) and mapping files named 1, 2, 3, ... that contain the docno of the documents belonging to those shards (one per line). In this method, buildfrommap can generate statistics for multiple shards at a time. See below for details.

```
$./Taily buildcorpus -p PARAM_FILE
$./Taily buildfrommap -p PARAM_FILE 
```

## How to Run Taily

If you just want a list of shard rankings, use this:
```
$./Taily run -p PARAM_FILE -q QUERY_FILE
```
Each line in the output will be `shardId<tab>v` value. To specify a v value for the Taily algorithm, just discard everything that has less than the desired v value. I recommend 45 for v. Parameter and query files formats are described below.

If you want a full retrieval, that is a selective search retrieval which runs a query in selective search using Taily to select the shards, use this:
```
$./TailyRunQuery INDRI_STYLE_PARAM_FILE
```

## Parameter Files

### Commands using ./Taily

Parameter files are simple key=value pairs. No space before or after '='!
Example files can be found under the example directory.

Parameter files for buildcorpus must contain the following parameters:
* db: The directory where the corpus statistics files will be written.
* index: The index(es) of the entire corpus. May be multiple indexes. Separate index paths using ':'. Do not uses spaces!
Optionally, it may also contain:
* terms: list of terms to collect statistics for (as opposed to all terms in index). Separate using ':'.
* ram: Approximate limit for RAM (for the Berkeley DB). Specified in MB.

Parameter files for buildshard must contain the following parameters:
* db: Directory where shard statistics files will be written.
* index: Index of the shard to generate stats from.
* corpusDb: Location (directory) of corpus-wide statistics generated from buildcorpus
Optionally, it may also contain:
* terms: list of terms to collect statistics for (as opposed to all terms in index). Separate using ':'.
* ram: Approximate limit for RAM (for the Berkeley DB). Specified in MB.

Parameter files for buildfrommap must contain the following parameters (note that terms is mandatory!):
* terms: list of terms to collect statistics for (as opposed to all terms in index). Separate using ':'.
* index: The index(es) of the entire corpus. May be multiple indexes. Separate index paths using ':'. Do not uses   spaces!
* mapFile: one or more shard map files; lines are of format: `<index id>.<internal document id>`
    `<index id>` is the index where the document with the `<internal document id>` can be found. The id is an int that should correspond to the order that the indexes are given in the `index` parameter. 
* db: Directory where the program will create directories for the shard stats of each shard specified in mapFile

Optionally, it may also contain:
* ram: Approximate limit for RAM (for the Berkeley DB). Specified in MB.

Parameter file for Taily run:
* db: List of shard statistics Dbs in order of the desired shardId. First db MUST be the global db generated from buildcorpus. Following dbs should be the paths to the individual shard dbs. Separate paths using ':'. e.g. db=/path/to/corpusdb:/path/to/shard1db:/path/to/shard2db 
* index: An indri index; used for stemming/term processing.
* n_c: The n paramter for the Taily algorithm. Use 400 or so if you're not sure.

Query file for Taily run:
* Each line should contain the query in the format of QUERY_NUM:QUERY TEXT.
  Example:
```
1:obama family tree
2:a man a plan a canal
3:panama
```


### ./TailyRunQuery

Parameter file for TailyRunQuery is an XML file in style of Indri's IndriRunQuery file. It takes everything else an IndriRunQuery parameter file takes but also requires:

```
<n_c>n parameter for Taily</n_c>
<v>v parameter for Taily</v>
<sampleIndex>Path to sample index used for term processing</sampleIndex>
<corpusDb>Corpus-wide taily statistics dir</corpusDb>

<db>
  <shard>shardId</shard>
  <path>path to dir of taily statistics for this shard</path>
</db>
<db>
  <shard>shardId2</shard>
  <path>path to dir of taily statistics for shardId2</path>
</db>

<daemon>
  <shard>shardId</shard>
  <index>path to shard index</index> OR <server>server location of daemon hosting shard index</server>
</daemon>
<daemon>
  <shard>shardId2</shard>
  <index>path to shard 2 index</index> OR <server>server location of daemon hosting shard 2 index</server>
</daemon>
```
