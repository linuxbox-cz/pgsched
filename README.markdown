
                 ___  _____          __          __     __       
                / _ \/ ___/ ___ ____/ /  ___ ___/ /_ __/ /__ ____
               / ___/ (_ / (_-</ __/ _ \/ -_) _  / // / / -_) __/
              /_/   \___/ /___/\__/_//_/\__/\_,_/\_,_/_/\__/_/

PG scheduler alias `pgsched` is a minimal python daemon providing cron/at/init
functionality at a PostgreSQL server developed by LinuxBox.cz.

HOW IT WORKS
------------
 * `pgsched` daemon connects to PostgreSQL server and looks for all databases
	containing `pgsched` schema.
 * `pgsched` schema contains (among other thing) 3 tables:
	* pgs_cron:   periodic tasks with cron scheduling
	* pgs_at:     one-time tasks ran at specific time
	* pgs_runner: continuous tasks

	By INSERTing into these tables, tasks are scheduled and later run by
	pgsched.

REQUIREMENTS
------------
server-side requirements:

 * PostgreSQL (tested on 9.0 and 9.1, should work on 8.4)
 * `hstore` module from `postgresql-contrib`

deamon-side requirements:

 * `python` >= 2.4
 * `psycopg2`
 * `lbasync`: LinuxBox library also hosted on [github](https://github.com/linuxbox-cz/lbasync)


INSTALL
-------

 * Install `hstore` from `postgresql-contrib` into public schema of all databases
   you wish to use `pgsched` in. (tip: `/usr/pgsql-9.X/share/contrib/hstore.sql`)

 * Create PostgreSQL `pgsched` superuser and install `pgsched` schema into all
   databases you wish to use `pgsched` in using `sql/pgsched.sql` (edit to your needs).

 * If not using RPM, copy `init-script` to `/etc/init.d/pgsched` on daemon side.

### RPM

LinuxBox: `/home/lbox/rpms/lbox-pgsched/`

#### building RPM

 1. Obtain source tarball by running `make dist` and copy lbox-pgsched-X.Y.tgz
	to `rpmbuild/SOURCES`.
 2. Copy `rpm/lbox-pgsched-X.Y.spec` to `rpmbuild/SPECS` and edit to your
	needs.
 3. `rpmbuild -ba lbox-pgsched-X.Y.spec`


USAGE
-----

`pgsched.py` deamon is controlled by `init-script` (which should be at
`/etc/init.d/pgsched`).

[lbasync and pgasync](https://github.com/linuxbox-cz/lbasync) are used, daemon
can be controlled by environmental variables:

	PGHOST, PGUSER, PGPASSWORD
	LOG_FILE

When using pgsched service from RPM, export set these in `/etc/sysconfig/pgsched`, i.e.:

	export PGHOST=hostname                                                                                        
	export PGUSER=pgsched                                                                                        
	export PGPASSWORD=secret
	
`INSERT into pgs_{cron,at,runner}` tables to schedule tasks.

Interesting columns are COMMENTed, use `\d+ pgs_{cron,at,runner}` in psql to
get some info.

LOGGING
-------
Task event logging is controled by per-task `log_level` column. Possible values are:

 * 0 - never log
 * 1 - log errors [default]
 * 2 - log errors & task run/finish

Logs are saved in `pgsched.pgs_log` table.

Daemon also logs into file using `lbasync` - `LOG_FILE` env. variable is used,
defaults to `/var/log/lbox/pgsched.log`
