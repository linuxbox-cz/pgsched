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
PIDFILE = '/var/run/pgscheduler.pid'
SCHEMA = 'pgscheduler'
DEBUG = True
DAEMON = False
MAX_CONN = 5


TS_WAITING, TS_RUNNING, TS_DONE = range(3)
class Task:
	def __init__(self, task_row, finished_callback):
		_, self.type, self.id, self.job, self.role, self.t_run, self.retro = task_row
		self.finished_cb = finished_callback
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
		logging.debug("Finished %s task %d: %s" % (self.type, self.id, ss))
		self.db.stop()
		self.db = None
		self.finished_cb(self, success)

	def run(self):
		logging.debug("RUN %s" % self)
		self.db = PGasync('')
		self.db.execute('SELECT "%s".run_task(\'%s\', %d);' % (SCHEMA, self.type, self.id), None, self.cb_finished, self.cb_failed)
	
	def __str__(self):
		return "%s task %s, job %s with role %s." % (self.type, self.id, self.job, self.role)


class PgSched:
	def __init__(self):
		self.tasks = []
		self.db = None
		self.nt_timer = None
	
	def run_task(self, task):
		logging.debug('.run_next_task()')
		self.tasks.append(task)
		task.run()
	
	def cb_task_finished(self, task, success = True):
		self.tasks.remove(task)

	def cb_got_next_task(self, cur, req):
		logging.debug('.cb_got_next_task()')
		row = cur.fetchone()
		wait = row[0]
		if wait == None:
			logging.debug("No more tasks. Waiting.")
			self.get_next_task_in(5)
		elif wait > 0:
			logging.debug("Waiting %g s for next task." % wait)
			self.get_next_task_in(wait)
		else:
			self.run_task(Task(row, self.cb_task_finished))
			self.get_next_task()

	def cb_err_next_task(self, cur, req, err):
		logging.error("Error retrieving next task: %s" % err)
		self.get_next_task_in(1)

	def has_max_conn(self):
		l = len([t for t in self.tasks if t.db])
		return l >= MAX_CONN - 1

	# always call this through get_next_task_in()
	def get_next_task(self, timer = None):
		logging.debug('.get_next_task()')
		if timer != None:
			if timer == self.nt_timer:
				self.nt_timer = None
			else:
				# TODO: temporary bug catcher - remove & shrink this shit to one line
				logging.warning('BUG: More than one next_task timer active.')
		if self.has_max_conn():
			logging.debug("Too many connections. Waiting.")
			self.get_next_task_in(1)
		else:
			self.db.execute('SELECT * FROM "%s".next_task();' % SCHEMA, None, self.cb_got_next_task, self.cb_err_next_task)
	
	def get_next_task_in(self, time):
		if self.nt_timer:
			logging.debug("Replacing next_task timer.")
			lbasync.stop_timer(self.nt_timer)
		if time <= 0:
			self.nt_timer = None
			self.get_next_task()
		else:
			self.nt_timer = lbasync.set_timer(self.get_next_task, time)
	
	def cb_notify(self, notify, client):
		logging.debug('NOTIFY received.')
		self.get_next_task_in(0)
	
	def run(self):
		self.db = PGasync('')
		self.db.listen('pgs_tasks_change', self.cb_notify)
		self.get_next_task_in(0)
		print "---- lbasync.run() ----"
		lbasync.run()

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
		if o in ('-d', '--daemon'):
			DAEMON = True
			# TODO: this does nothing now, use lbasync `export DAEMON` mechanism
		elif o in ('-h', '--help'):
			usage()
			sys.exit()
		elif o in ('-v', '--version'):
			print("pgscheduler version %s" % VERSION)
			sys.exit()

def main(daemon = False):
	load_args()
	try:
		pgsched = PgSched()
		pgsched.run()
	except Exception, e:
		logging.exception("Unexpected error: " + str(e))
		sys.exit(1)

if __name__ == "__main__":
	main()
