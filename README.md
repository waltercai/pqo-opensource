# Pessimistic Cardinality Estimation: Tighter Upper Bounds for Intermediate Join Cardinalities

## Experimental Overview
There are two primary modules in this repository.

1. The first is the source code for a modified postgres instance.
The only real modification is in the `postgresql-9.6.6/src/backend/optimizer/path/costsize.c` method.
Instead of returning the naive default postgres query optimizer cardinality estimates, we parse a `info.txt` file for join cardinality bounds which we use in place of the estimates.
If a bound is found for a subquery, then the bound is returned.
Ideally, a bound is returned for all sought after subqueries.
2. The second is a java module that can decomposes queries and creates bounds for all necessary subqueries and populates the `info.txt` file which will then be able to be ingested by a modified postgres instance.

Following initial setup, one may simply run the java module which will execute each query in the desired workload.
For each query, we will populate `info.txt` with the necessary bounds.
Following this, each query is submitted to postgres.
It is also possible to simply run the queries using postgres' default cardinality estimates (this is done by simply leaving `info.txt` empty, a method for which is provided).

## Modified Postgres Instance

### Postgres Installation
First, navigate to the directory postgres directory:
~~~~
cd postgresql-9.6.6/
~~~~

The modified postgres source code may be installed in the same manner as with a normal postgres instance.
We suggest finding a more comprehensive guide to building postgres from source that is specific to the reader's OS.
We found the following guides helpful:
- [Linux](https://www.postgresql.org/docs/9.6/install-short.html)
- [OSX](https://labs.wordtothewise.com/postgresql-osx/)
- [Windows](https://www.postgresql.org/docs/9.6/install-windows.html)
Return to the home directory
~~~~
cd ..
~~~~

### Data Upload
One may clone our imdb database instance by first downloading the compressed database and unpacking.
The copressed data is available on [s3](https://s3-us-west-2.amazonaws.com/uwdbimdbsimple/imdb.dump.gz).
Readers are also welcome to use newer/older versions of the imdb dataset.
Bash script `populate_job.sh` will populate the imdb database or the reader may execute the commands separately.
~~~~
wget https://s3-us-west-2.amazonaws.com/uwdbimdbsimple/imdb.dump.gz
/usr/local/pgsql/bin/createdb imdb
gunzip -c imdb.dump.gz | psql imdb
~~~~

If using the provided imdb database snapashot, the reader should expect the database to take up 32Gb and take approximately 1 hour to populate.

## Bound Generation Module
The purpose of this module is primarly to populate the `info.txt` file.
The Driver class is also set up to execute and run the join order benchmark.
Results for default postgres execution are written to `/results/[DBName]/plan_execution_time_[budget].txt`.
For example, the result of running the join order benchmark with a hash budget of 4096 would be written to `/results/imdb/plan_execution_time_4096.txt`.
We also include the sketch processing time which includes the additional preprocessing time incurred by our method in `/results/[DBName]/sketch_preprocessing_[budget].txt`.
We also include the postgres [EXPLAIN ANALYZE](https://www.postgresql.org/docs/9.6/sql-explain.html) output for each query in `raw/[DBName]/bound_[budget].txt`.
These include a detailed writeup of the physical join plan and the estimated versus observed intermediate join cardinalities.

Similarly, the if one wishes to compare to default postgres execution, one will find these respective results in `results/[DBName]/default.txt` and `raw/[DBName]/default.txt`.
Please note that one will likely have to create the `result/` and `raw/` directories ahead of time to avoid errors.
This also goes for the `results/[DBName]/` and `raw/[DBName]/` subdirectories.

One may compile the java library using the following command (from the top level directory of the repo):
~~~~
javac -cp BoundSketch/src/.:BoundSketch/combinatoricslib3-3.2.0.jar:BoundSketch/jsqlparser-1.2-SNAPSHOT.jar:BoundSketch/postgresql-42.2.0.jar  BoundSketch/src/*.java
~~~~

One may execute the tests using the following command:
~~~~
java -cp BoundSketch/src/.:BoundSketch/combinatoricslib3-3.2.0.jar:BoundSketch/jsqlparser-1.2-SNAPSHOT.jar:BoundSketch/postgresql-42.2.0.jar Driver
~~~~
