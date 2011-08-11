#!/usr/bin/env python
# -*- encoding: utf8 -*-

import lbasync
import psycopg2, psycopg2.extensions
import re, sys, os, time, logging, getopt
from logging.handlers import SysLogHandler
from logging import StreamHandler
from daemon import Daemon
from pgasync import PGasync
from copy import copy
from exceptions import Exception


VERSION = '0.1'
PIDFILE = '/var/run/pgscheduler.pid'
SCHEMA = 'pgscheduler'
DEBUG = True
DAEMON = False
MAX_CONN = 5

logger = logging.getLogger()


class Task(PGasync):
	def __init__(self, task_row, finished_callback):
		PGasync.__init__(self, '')
		self.type, self.id, self.job, self.role, self.t_run, self.retro = task_row
		self.finished_cb = finished_callback
	
	def cb_finished(self, cur, req):
		logger.debug("FINISHED %s task %d." % (self.type, self.id))
		self.finished_cb(self)

	def cb_failed(self, cur, req, err):
		logger.error("Error running %s task %d: %s" % (self.type, self.id, err))
		self.finished_cb(self, False)

	def done(self, success = True):
		self.remove()
		self.close()
		self.finished_cb(self, success)

	def run(self):
		logger.debug("RUN %s" % self)
		self.execute('SELECT "%s".next_task();' % SCHEMA, None, self.cb_finished, self.cb_failed)
	
	def __str__(self):
		return "%s task %d, job %s with role %s." % (self.type, self.id, self.job, self.role)
		
		
class PgSched:
	def __init__(self):
		self.tasks = []
		self.next_task = None
		self.db = None
	
	def run_next_task(self, timer = None):
		logger.debug('.run_next_task()')
		self.tasks.append(self.next_task)
		self.next_task.run()
		self.next_task = None
		self.get_next_task()
	
	def cb_task_finished(self, task, success = True):
		logger.debug('.cb_task_finished()')
		self.tasks.remove(task)

	def cb_got_next_task(self, cur, req):
		logger.debug('.cb_got_next_task()')
		row = cur.fetchone()
		if row:
			self.next_task = Task(row, self.cb_task_finished)
			t_run = self.next_task.t_run
			t_now = time.time()
			if t_run > t_now:
				lbasync.setTimer(self.run_next_task, t_run - t_now)
				logger.debug("Scheduled next task: %s %d" % (task[0], task[1]))
			else:
				self.run_next_task()
		else:
			logger.debug("No more tasks. Waiting.")
			lbasync.setTimer(self.run_next_task, 10)

	def cb_err_next_task(self, cur, req, err):
		logger.error("Error retrieving next task: %s" % err)
		lbasync.setTimer(self.get_next_task, 1)

	def get_next_task(self, timer = None):
		logger.debug('.get_next_task()')
		if len(self.tasks) >= MAX_CONN - 1:
			logger.debug("Too many connections. Waiting.")
			lbasync.setTimer(self.get_next_task, 1)
		else:
			self.db.execute('SELECT * FROM "%s".next_task();' % SCHEMA, None, self.cb_got_next_task, self.cb_err_next_task)
	
	def run(self):
		self.db = PGasync('')
		self.get_next_task()
		print "---- lbasync.run() ----"
		lbasync.run()

class PgSchedDaemon(Daemon):
	def run(self):
		global DAEMON
		DAEMON = True
		pgs_main()

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
		elif o in ('-h', '--help'):
			usage()
			sys.exit()
		elif o in ('-v', '--version'):
			print("pgscheduler version %s" % VERSION)
			sys.exit()

def setup_logging():
	global DEBUG, DAEMON, LOG_LEVEL, logger
	if DEBUG:
		LOG_LEVEL = logging.DEBUG
	else:
		LOG_LEVEL = logging.INFO
	if DAEMON:
		handler = SysLogHandler(address='/dev/log')
		formatter = logging.Formatter('%(name)s: %(levelname)s: %(message)s')
	else:
		handler = StreamHandler(sys.stdout)
		formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
	logger = logging.getLogger('pgscheduler')
	logger.setLevel(LOG_LEVEL)
	handler.setFormatter(formatter)
	logger.addHandler(handler)

def pgs_main():
	setup_logging()
	try:
		pgsched = PgSched()
		pgsched.run()
	except Exception, e:
		logger.exception("Unexpected error: " + str(e))
		sys.exit(1)

def main(daemon = False):
	load_args()
	if DAEMON:
		daemon = PgSchedDaemon(PIDFILE)
		daemon.start()
	else:
		pgs_main()

if __name__ == "__main__":
	main()
