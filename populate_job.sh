wget https://s3-us-west-2.amazonaws.com/uwdbimdbsimple/imdb.dump.gz
/usr/local/pgsql/bin/createdb imdb
gunzip -c imdb.dump.gz | psql imdb
