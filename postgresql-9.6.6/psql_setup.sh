#!/bin/bash

# .bash_profile shortcuts:
# alias psql="/usr/local/pgsql/bin/psql -U postgres"
# alias startpsql="sudo -u postgres /usr/local/pgsql/bin/pg_ctl start -D /usr/local/pgsql/data/"
# alias stoppsql="sudo -u postgres /usr/local/pgsql/bin/pg_ctl stop -D /usr/local/pgsql/data/"
# alias restartpsql="sudo -u postgres /usr/local/pgsql/bin/pg_ctl restart -D /usr/local/pgsql/data/"

sudo pwd

stoppsql; make; sudo make install;

# sudo rm -rf /usr/local/pgsql/data/; sudo mkdir /usr/local/pgsql/data/; sudo chown postgres:postgres /usr/local/pgsql/data; sudo -u postgres /usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data; startpsql;
# sudo -u postgres /usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data; startpsql;
/usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data; startpsql;

#up to here is important stuff you can ignore the rest...

sudo -u postgres /usr/local/pgsql/bin/createdb gp
psql gp -f ~/Documents/datasets/test/scripts/mini_postgres_populate.sql
psql gp -f ~/Documents/datasets/test/scripts/test_run.sql






sudo -u postgres /usr/local/pgsql/bin/createdb imdb
date
gunzip -c ~/Desktop/imdb.dump.gz | sudo -u postgres /usr/local/pgsql/bin/psql imdb
date
