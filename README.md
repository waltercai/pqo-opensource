# Pessimistic Cardinality Estimation: Tighter Upper Bounds for Intermediate Join Cardinalities

## Experimental Overview
There are two primary modules in this repository.

1. The first is a java module that can decomposes queries and creates bounds for all necessary subqueries. These bounds are then submitted to an info file which will then be able to be ingested by a modified postgres instance.
2. The second is the source code for a modified postgres instance. The only real modification is in the `src/backend/optimizer/path/costsize.c` method. Instead of returning the naive postgres bounds, we parse the `info.txt` file for each subquery. If a bound is found for a subquery, then the bound is returned. Ideally, a biound is returned for all relevant subqueries.

