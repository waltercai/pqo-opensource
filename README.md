# Pessimistic Cardinality Estimation: Tighter Upper Bounds for Intermediate Join Cardinalities

## Experimental Overview
There are two primary modules in this repository.

1. The first is the source code for a modified postgres instance. The only real modification is in the `postgresql-9.6.6/src/backend/optimizer/path/costsize.c` method. Instead of returning the naive default postgres query optimizer cradinality estimates, we parse a `info.txt` file for join cardinality bounds which we use in place of the estimates. If a bound is found for a subquery, then the bound is returned. Ideally, a bound is returned for all sought after subqueries.
2. The second is a java module that can decomposes queries and creates bounds for all necessary subqueries and populates the `info.txt` file which will then be able to be ingested by a modified postgres instance.

Following initial setup, one may simply run the java module which will execute each query in the desired workload. For each query, we will populate `info.txt` with the necessary bounds. Following this, each query is submitted to postgres. It is also possible to simply run the queries using postgres' default cardinality estimates (this is done by simply leaving `info.txt` empty, a method for which is provided).

## Modified Postgres Instance

### Postgres Installation
The instance may be installed in the same manner as with a normal postgres instance. We suggest finding a more comprehensive guide to building postgres from source that is specific to the reader's OS. We found the following guides helpful:

- [linux](https://www.postgresql.org/docs/9.6/install-short.html)
- [OSX](https://labs.wordtothewise.com/postgresql-osx/)
- [Windows](https://www.postgresql.org/docs/9.6/install-windows.html)

### Data Upload
We can clone our imdb database instance by first downloading the compressed clone and unpacking. The copressed data can be downloaded at: XXXXXXXXXX
~~~~
wget XXXXXXXXXXX
/usr/local/pgsql/bin/createdb imdb
gunzip -c imdb_dump.txt.gz | psql imdb
~~~~

## Bound Generation Module
