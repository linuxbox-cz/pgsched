#!/usr/bin/env python
# -*- encoding: utf8 -*-

# XXX: DEBUG
from os import environ
environ['LOG_LEVEL'] = 'debug'

import lbasync
import re, sys, os, time, logging, getopt
import psycopg2, psycopg2.extensions
import logging
from pgasync import PGasync
from copy import copy
from exceptions import Exception


VERSION = '0.1'
PIDFILE = '/var/run/pgsched.pid'
SCHEMA = 'pgsched'
DEBUG = True
DAEMON = False
MAX_CONN = 5
CHECK_PERIOD = 30
DB_SEEK_TIMEOUT = 30


TS_WAITING, TS_RUNNING, TS_DONE = range(3)
class Task:
	def __init__(self, dbname, task_row, finished_callback):
		_, self.type, self.id, self.job, self.role, self.t_run, self.retro = task_row
		self.finished_cb = finished_callback
		self.dbname = dbname
		self.db = None
	
	def cb_finished(self, cur, req):
		self.done(cur.fetchone()[0])

	def cb_failed(self, cur, req, err):
		self.done(-10)

	def done(self, success):
		if success >= 0:
			ss = 'SUCCESS'
		else:
			ss = 'FAILURE (code: %d)' % success
		logging.debug("[%s] Finished %s task %d: %s" % (self.dbname, self.type, self.id, ss))
		self.db.stop()
		self.db = None
		self.finished_cb(self, success)

	def run(self):
		logging.debug("[%s] RUN %s" % (self.dbname, self))
		environ['PGDATABASE'] = self.dbname

		self.db = PGasync(name='[%s] %s task %s' % (self.dbname, self.type, self.id))
		self.db.execute('SELECT "%s".run_task(%%s, %%s);' % SCHEMA, (self.type, self.id), self.cb_finished, self.cb_failed)
	
	def __str__(self):
		return "%s task %s, job %s with role %s." % (self.type, self.id, self.job, self.role)


class PgSched:
	def __init__(self):
		self.tasks = []
		self.dbname = None
		self.db = None
		self.nt_timer = None
	
	def run_task(self, task):
		self.tasks.append(task)
		task.run()
	
	def cb_task_finished(self, task, success = True):
		self.tasks.remove(task)

	def cb_got_next_task(self, cur, req):
		row = cur.fetchone()
		wait = row[0]
		if wait == None:
			logging.debug("[%s] No more tasks. Waiting." % self.dbname)
			self.get_next_task_in(CHECK_PERIOD)
		elif wait > 0:
			if wait > CHECK_PERIOD:
				logging.debug("[%s] Next task in %g s, next check in %g s." % (self.dbname, wait, CHECK_PERIOD))
				wait = CHECK_PERIOD
			else:
				logging.debug("[%s] Waiting %g s for next task." % (self.dbname, wait))
			self.get_next_task_in(wait)
		else:
			self.run_task(Task(self.dbname, row, self.cb_task_finished))
			self.get_next_task_in(0)

	def cb_err_next_task(self, cur, req, err):
		logging.error("[%s] Error retrieving next task: %s" % (self.dbname, err))
		self.get_next_task_in(1)

	def has_max_conn(self):
		l = len([t for t in self.tasks if t.db])
		return l >= MAX_CONN - 1

	# always call this through get_next_task_in()
	def get_next_task(self, timer = None):
		if timer != None:
			if timer == self.nt_timer:
				self.nt_timer = None
			else:
				# TODO: temporary bug catcher - remove & shrink this shit to one line
				logging.warning('BUG: [%s] More than one next_task timer active.' % self.dbname)
		if self.has_max_conn():
			logging.debug("[%s] Too many connections. Waiting." % self.dbname)
			self.get_next_task_in(1)
		else:
			self.db.execute('SELECT * FROM "%s".next_task();' % SCHEMA, None, self.cb_got_next_task, self.cb_err_next_task)
	
	def get_next_task_in(self, time):
		if self.nt_timer:
			logging.debug("[%s] Replacing next_task timer." % self.dbname)
			lbasync.stop_timer(self.nt_timer)
		if time <= 0:
			self.nt_timer = None
			self.get_next_task()
		else:
			self.nt_timer = lbasync.set_timer(self.get_next_task, time)
	
	def cb_notify(self, notify, client):
		logging.debug('[%s] NOTIFY received.' % self.dbname)
		self.get_next_task_in(0)
	
	def cb_check(self, cur, req, error = None):
		if error == None and cur.fetchone()[0] == 1:
			self.check_fun(self, True)
		else:
			self.check_fun(self, False)

	def connect_and_check(self, check_fun, dbname = None):
		self.check_fun = check_fun
		if dbname:
			self.dbname = dbname
			environ['PGDATABASE'] = dbname
		else:
			self.dbname = environ['PGDATABASE']
		self.db = PGasync(name=self.dbname)
		# TODO: don't wait forever?
		self.db.execute("SELECT count(nspname) FROM pg_namespace WHERE nspname = '%s'" % SCHEMA, None, self.cb_check, self.cb_check)
		return False

	def run(self):
		self.db.listen('pgs_tasks_change', self.cb_notify)
		self.get_next_task_in(0)
	
	def stop(self):
		self.db.stop()
		self.db = None

class PgSchedSeeker(list):
	def __init__(self):
		self.checking = []
		self.db = None

	def cb_check(self, pgs, success):
		self.checking.remove(pgs) 
		if success:
			self.append(pgs)
			logging.debug('Scheduler connected to DB %s' % pgs.dbname)
			pgs.run()
		else:
			pgs.stop()

	def cb_got_dbs(self, cur, req):
		rows = cur.fetchall()
		self.db.stop()
		for row in rows:
			dbname = row[0]
			pgs = PgSched()
			self.checking.append(pgs)
			pgs.connect_and_check(self.cb_check, dbname)
		lbasync.set_timer(self.check_timeout, DB_SEEK_TIMEOUT)
	
	def check_timeout(self, timer = None):
		logging.debug("DB seek timed out. Killing %d connections." % len(self.checking))
		# TODO: stop all self.checking
		pass

	def cb_err_dbs(self, cur, req, err):
		logging.warning('Error retrieving database list: %s' % err)
		lbasync.exit(1)
	
	def start(self, first_db = 'postgres'):
		environ['PGDATABASE'] = first_db
		self.db = PGasync(name='DB seeker @ %s' % first_db)
		self.db.execute("SELECT datname::TEXT FROM pg_database WHERE datname NOT LIKE 'template%' AND datname != 'postgres'", None, self.cb_got_dbs, self.cb_err_dbs) 

		

#### script functions

def usage():
	print """usage: pgsched.py [-h] [-d] [-c CONFIG_FILE] [-C EXAMPLE_CONFIG_FILE] [-v]

PING daemon

optional arguments:
  -d, --daemon          run as a daemon
  -v, --version         output version information and exit
  -h, --help            show this help message and exit
"""

def load_args():
	global DAEMON, CONFIG_FILE
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'dvh',
			['daemon', 'version', 'help'])
	except getopt.GetoptError, err:
		print str(err), "\n"
		usage()
		sys.exit(2)
	for o, a in opts:
		if o in ('-h', '--help'):
			usage()
			sys.exit()
		elif o in ('-v', '--version'):
			print("pgsched version %s" % VERSION)
			sys.exit()

def main(daemon = False):
	load_args()
	try:
		pgscheds = PgSchedSeeker()
		pgscheds.start()
	except Exception, e:
		logging.exception("Unexpected error: " + str(e))
		sys.exit(1)

if __name__ == "__main__":
	lbasync.run(main)
