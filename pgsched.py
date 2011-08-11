#!/usr/bin/env python
# -*- encoding: utf8 -*-

from daemon import Daemon
import psycopg2, psycopg2.extensions
import re, sys, os, time
import logging, getopt, ConfigParser
from logging.handlers import SysLogHandler
from logging import StreamHandler
from copy import copy
from exceptions import Exception
from time import sleep

VERSION = '0.1'

# don't modify without a good reason
DEFAULT_SETTINGS = {
	'debug': 'false',
	'db_retry_delay': '2',
}
CONFIG_FILE = '/etc/lbox/pgscheduler/pgscheduler.cfg'
PIDFILE = '/var/run/pgscheduler.pid'
DEBUG = False
DAEMON = False

# global evil
logger = logging.getLogger()

# helper functions
def timedelta_sec(td):
	if td:
		return (td.days * 3600.0 * 24) + td.seconds + (td.microseconds / 100000.0)
	else:
		return None

def timestamp2pgtime(stamp):
	return "timestamptz 'epoch' + %f * interval '1 second'" % stamp

def datetime2stamp(dt):
	time.mktime(dt.timetuple())


# tasks

TT_GENERIC = -1
TT_CRON, TT_AT, TT_RUNNER = range(3)

# common parent class for all tasks
class Task(object):
	def __init__(self, id, job, desc):
		self.id = id
		self.job = job
		self.desc = desc

	def sched_remove(self, events):
		events.remove(self)


class AtTask(Task):
	def __init__(self, id, job, desc, dt_at, retroactive, db):
		super(AtTask, self).__init__(id, job, desc)
		self.t_at = datetime2stamp(dt_at)
		self.retroactive = retroactive
		self.db = db
		self.type = TT_AT
	
	def start_job(self, events):
		# TODO
		logger.debug("TODO: starting AT job '%s'" % self.job)
		self.db.start_job(self.job)
	
	def do_schedule(self, events):
		r = True
		if time.time() > self.t_at:
			if self.retroactive:
				t = 0
				r = False
			else:
				return False
		else:
			t = self.t_at
		events.add( Event(t, self.type, self) )
		if not r:
			db.delete_at_task(self.id)
		return r

	def sched_new(self, events):
		return self.do_schedule(events)

	def sched_update(self, events, old):
		# TODO
		logger.debug("TODO: AtTask.sched_update()")
		return True
	
	def __str__(self):
		if self.retroactive:
			r = 'RETROACTIVE'
		else:
			r = 'NOT retroactive'
		return 'AT task [%d]: (job: %s, desc: "%s", t_at: %s, %s)' % (self.id, self.job, self.desc, self.t_at, r) 

		
# Event calendar and friends

EVT_CRON, EVT_AT, EVT_RUNNER = range(3)
EVENT_NAMES = [ 'CRON TASK', 'AT TASK', 'RUNNER TASK' ]

class Event:
	def __init__(self, t, type, ref = None):
		self.t = t
		self.type = type
		self.ref = ref

	def to_str(self):
		return "%f: %s, %s" % (self.t, self.type, self.ref)

class EventCalendar:
	def __init__(self):
		self.events = []
	
	def add(self, event):
		i = 0
		for evt in self.events:
			if event.t < evt.t:
				break
			i += 1
		
		self.events.insert(i, event)
	
	def has_events(self):
		return self.events

	def t_next_event(self):
		if self.events:
			return self.events[0].t
		else:
			return -1
		
	def what_happened(self, t):
		happened = []
		i = 0
		for event in self.events:
			if t >= event.t:
				happened.append(event)
				i += 1
		self.events = self.events[i:]
		return happened

	def remove(self, ref = None, type = None):
		if not (ref or type):
			logger.warning("PROGRAMMING FAIL: removing events without the means to identify them.")
			return
		for event in self.events:
			if (not ref or event.ref == ref) and (not type or event.type == type):
				self.events.remove(event)

	def print_events(self):
		logger.logger.debug("SCHEDULED EVENTS:")
		for event in self.events:
			logger.logger.debug("         %s: %s" % (EVENT_NAMES[event.type], str(event.ref)))


# all the ugly DB crap isolated (model on steroids)
class PgSchedDBHelper:
	def __init__(self):
		self.db_ready = False
		self.schema   = "pgscheduler"

	def _connect(self):
		# TODO: check if not connected already
		try:
			self.conn = psycopg2.connect(DB_CONNECT_STRING)
			self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
			self.cur = self.conn.cursor()
		#	self.cur.execute("LISTEN pgscheduler_tasks_change;")
			self.db_ready = True
		except psycopg2.OperationalError, e:
			return False
		return True

	# blocking DB init
	def connect(self):
		if not self._connect():
			logger.warning("Initial PostgreSQL connection failed. "
			               "Will retry in %d sec intervals." % DB_RETRY_DELAY)
			n_retry = 1;
			while not self._connect():
				sleep(DB_RETRY_DELAY);
				n_retry += 1;
			logger.info("Initial PostgreSQL connection established after %d retries (%d s)."
			            % (n_retry, n_retry * DB_RETRY_DELAY))
		else:
			logger.info("PostgreSQL connection established.")

	def get_cron_tasks(self):
		# TODO
		return []
	
	def get_at_tasks(self):
		self.cur.execute("""SELECT id, job, "desc", t_at, retroactive"""
			""" FROM "%s".pgs_at""" 
			""" WHERE enabled IS TRUE""" % self.schema)
		tasks = []
		for t in self.cur.fetchall():
			tasks.append(AtTask(t[0], t[1], t[2], t[3], t[4]))
		return tasks

	def get_runner_tasks(self):
		# TODO
		return []
	
	def delete_at_task(id):
		self.cur.execute("""DELETE FROM "%s".pgs_at WHERE ID = %d""" % (self.schema, id))
		

# pwnage happens here
class PgSched:
	def __init__(self):
		self.db = PgSchedDBHelper()
		self.events = EventCalendar()
		self.cron_tasks   = []
		self.at_tasks     = []
		self.runner_tasks = []

	def init(self):
		self.db.connect()
		self.load_tasks()
	
	def update_tasks(self, old, new):
		print len(new)
		ot      = None
		active  = False
		to_kill = []
		for nt in new:
			found  = False
			for ot in old:
				if old.id == new.id:
					found = True
					break
			if found:
				logger.info("UPDATED %s" % nt)
				active = nt.sched_update(self.events, ot)
				old.remove(ot)
			else:
				logger.info("NEW %s" % nt)
				active = nt.sched_new(self.events)
			if not active:
				# TODO: tasks from past
				logger.debug("Removing EXPIRED task: %s" % nt)
				to_kill.append(nt)
		for t in to_kill:
			new.remove(t)
		for ot in old:
			logger.info("REMOVED %s" % ot)
			ot.sched_remove(self.events)
		return new

	def load_tasks(self):
		self.cron_tasks   = self.update_tasks(self.cron_tasks,   self.db.get_cron_tasks())
		self.at_tasks     = self.update_tasks(self.at_tasks,     self.db.get_at_tasks())
		self.runner_tasks = self.update_tasks(self.runner_tasks, self.db.get_runner_tasks())
		self.print_tasks()
	
	def print_tasks(self):
		logger.debug("--- TASKS ---")
		for t in self.at_tasks:
			logger.debug(str(t))

	def process_event(self, event):
		# all events are tasks so far
		event.ref.start_job(self.events)

	def run(self):
		while self.events.has_events():
			t_next = self.events.t_next_event()
			t_now = time.time()
			if t_next > t_now:
				time.sleep(t_next - t_now)
			#t_now = time.time()
			happened = self.events.what_happened(t_now)
			for event in happened:
				self.process_event(event)

	def start(self):
		self.run()
		
class PgSchedDaemon(Daemon):
	def run(self):
		global DAEMON
		DAEMON = True
		pgs_main()

#### script helper functions

def load_config():
	global DB_CONNECT_STRING, DB_RETRY_DELAY, DEBUG, DAEMON, LOG_LEVEL, DEFAULT_SETTINGS, logger

	# First, read settings
	config = ConfigParser.ConfigParser(DEFAULT_SETTINGS)
	config.add_section('pgscheduler')

	conf_read = config.read(CONFIG_FILE)

	# first read options realted to logging 
	DEBUG = config.getboolean('pgscheduler', 'debug')
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

	# set proper logging (enough info now)
	logger = logging.getLogger('pgscheduler')
	logger.setLevel(LOG_LEVEL)
	handler.setFormatter(formatter)
	logger.addHandler(handler)

	# we need SOME config.
	if len(conf_read) < 1:
		logger.error('Failed to load config file \'%s\'.' % CONFIG_FILE)
		sys.exit(1)

	# read the only mandatory option
	try:
		DB_CONNECT_STRING = config.get('pgscheduler', 'connstr')
	except ConfigParser.NoOptionError:
		logger.error("DB connection string 'connstr' is missing in the config file.")
		sys.exit(1)

	# read the rest of options
	DB_RETRY_DELAY = config.getint('pgscheduler', 'db_retry_delay')
	

def write_example_config(config_file):
	if os.path.isfile(config_file):
		print("File '%s' already exists - exiting in fear of doing something nasty." % config_file)
		sys.exit(1)

	f = open(config_file, "w")
	f.write("[pgscheduler]\n")
	f.write("connstr: dbname='lbstat' user='pgscheduler' host='sql2' password='secret'\n")
	for key, val in DEFAULT_SETTINGS.items(): 
		f.write('%s: %s\n' % (key, val))
	f.close
	print("Wrote example config to '%s'." % config_file)
	sys.exit(0)

def usage():
	print """usage: pgsched.py [-h] [-d] [-c CONFIG_FILE] [-C EXAMPLE_CONFIG_FILE] [-v]

PING daemon

optional arguments:
  -h, --help            show this help message and exit
  -d, --daemon          run as a daemon
  -c CONFIG_FILE, --config CONFIG_FILE
                        use the supplied configuration file
  -C EXAMPLE_CONFIG_FILE, --example-config EXAMPLE_CONFIG_FILE
                        create a configuration file with default values and
                        exit
  -v, --version         output version information and exit
"""

def load_args():
	global DAEMON, CONFIG_FILE
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'dc:C:vh',
			['help', 'daemon', 'config=', 'example-config='])
	except getopt.GetoptError, err:
		print str(err), "\n"
		usage()
		sys.exit(2)

	for o, a in opts:
		if o in ('-d', '--daemon'):
			DAEMON = True
		elif o in ('-c', '--config'):
			CONFIG_FILE = a
		elif o in ('-C', '--example-config'):
			write_example_config(a)
		elif o in ('-h', '--help'):
			usage()
			sys.exit()
		elif o in ('-v', '--version'):
			print("pgscheduler version %s" % VERSION)
			sys.exit()



def unexpected_death(e):
	logger.error("Unexpected error: " + str(e))
	sys.exit(1)
	

def pgs_main():
	# config
	load_config()

	try:
		# allmighty scheduler
		pgsched = PgSched()
		pgsched.init()
		pgsched.start()
	except psycopg2.OperationalError, e:
		logger.error("Operational error: " + str(e))
	#except Exception, e:
		#unexpected_death(e);

def main(daemon = False):
	load_args()
	
	if DAEMON:
		daemon = PgSchedDaemon(PIDFILE)
		daemon.start()
	else:
		pgs_main()

if __name__ == "__main__":
	main()
