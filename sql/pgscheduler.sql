CREATE SCHEMA pgsched;
GRANT ALL ON SCHEMA pgsched TO pgsched;

SET ROLE pgsched;
SET search_path TO pgsched;


CREATE OR REPLACE FUNCTION rrcheck(dr OID, rr OID) RETURNS BOOLEAN AS $$
	DECLARE
		pr OID;
	BEGIN
		FOR pr IN SELECT roleid FROM pg_auth_members WHERE member = dr LOOP
			IF pr = rr OR pgsched.rrcheck(pr, rr) THEN
				RETURN TRUE;
			END IF;
		END LOOP;
		RETURN FALSE;
	END;
$$ LANGUAGE plpgsql;

-- is desc_role descendant of root_role or superuser?
CREATE OR REPLACE FUNCTION role_check(desc_role TEXT, root_role TEXT) RETURNS BOOLEAN AS $$
	DECLARE
		dr RECORD;
		rr RECORD;
    BEGIN
		IF desc_role = root_role THEN
			RETURN TRUE;
		END IF;

		SELECT INTO dr oid, rolsuper FROM pg_roles WHERE rolname = desc_role;
		IF dr.oid IS NULL THEN
			RETURN FALSE;
		END IF;
		IF dr.rolsuper THEN
			RETURN TRUE;
		END IF;

		SELECT INTO rr oid FROM pg_roles WHERE rolname = root_role;
		IF rr.oid IS NULL THEN
			RETURN FALSE;
		END IF;

		RETURN pgsched.rrcheck(dr.oid, rr.oid);
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tasks_change_tr() RETURNS trigger AS $$
	DECLARE
		check_old_role BOOLEAN;
		check_new_role BOOLEAN;
    BEGIN
		check_old_role := TRUE;
		check_new_role := TRUE;

		IF TG_OP = 'INSERT' THEN
			check_old_role := FALSE;
		ELSIF TG_OP = 'UPDATE' THEN
			IF OLD.id != NEW.id THEN
				RAISE EXCEPTION 'ID change is forbidden.';
			END IF;
			IF OLD.role = NEW.role THEN
				check_new_role := FALSE;
			END IF;
		ELSIF TG_OP = 'DELETE' THEN
			check_new_role := FALSE;
		END IF;
		IF check_old_role AND NOT pgsched.role_check(current_user, OLD.role) THEN
			RAISE EXCEPTION 'Current user % can''t modify tasks with role %.', current_user, OLD.role;
		END IF;
		IF check_new_role AND NOT pgsched.role_check(current_user, NEW.role) THEN
			RAISE EXCEPTION 'Current user % can''t schedule tasks with role %.', current_user, NEW.role;
		END IF;
        NOTIFY pgs_tasks_change;
		RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

/*
 * Let "task" be a (a series of?) "job" scheduled to run at a specific time.
 * This table has common columns for all the scheduling methods (tables).
 */
CREATE TABLE pgs_task (
    id          SERIAL PRIMARY KEY,
	/* function name to call (without parenthesis) */
    job         TEXT NOT NULL,
    created     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run    TIMESTAMPTZ,
    "desc"      TEXT,
    role        TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    valid_from  TIMESTAMPTZ,
    valid_to    TIMESTAMPTZ
);
/*
 * Periodic cron style tasks.
 */
CREATE TABLE pgs_cron (

	/* cron time as seen in `man 5 crontab` (ranges, steps, names and specials
	 * are supported) */
	crontime TEXT NOT NULL DEFAULT '@daily',
    /* run retroactively like anacron? */
    retroactive BOOLEAN DEFAULT FALSE NOT NULL,

	/* internal caching */
    c_min boolean[] NOT NULL DEFAULT '{f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f}'::boolean[],
    c_hrs boolean[] NOT NULL DEFAULT '{f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f}'::boolean[],
    c_day boolean[] NOT NULL DEFAULT '{f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f}'::boolean[],
    c_mon boolean[] NOT NULL DEFAULT '{f,f,f,f,f,f,f,f,f,f,f,f}'::boolean[],
    c_dow boolean[] NOT NULL DEFAULT '{f,f,f,f,f,f,f}'::boolean[]
) 
INHERITS (pgs_task);

CREATE OR REPLACE FUNCTION cron_parse_tr() RETURNS trigger AS $$
	DECLARE 
		p pgsched.parsed_cron_t;
    BEGIN
		IF TG_OP = 'UPDATE' AND OLD.crontime = NEW.crontime THEN
			RETURN NEW;
		END IF;
		SELECT INTO p * FROM parse_cron(NEW.crontime);
		IF p.min IS NULL THEN
			RAISE EXCEPTION 'Invalid cron time string: %', NEW.crontime;
			RETURN NULL;
		END IF;
		NEW.c_min := p.min;
		NEW.c_hrs := p.hrs;
		NEW.c_day := p.day;
		NEW.c_mon := p.mon;
		NEW.c_dow := p.dow;
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS pgs_cron_change_tr ON pgs_cron;
CREATE TRIGGER pgs_cron_change_tr
BEFORE INSERT OR UPDATE OR DELETE ON pgs_cron
	FOR EACH ROW EXECUTE PROCEDURE tasks_change_tr();

DROP TRIGGER IF EXISTS pgs_cron_parse_tr ON pgs_cron;
CREATE TRIGGER pgs_cron_parse_tr
BEFORE INSERT OR UPDATE ON pgs_cron
	FOR EACH ROW EXECUTE PROCEDURE cron_parse_tr();


/*
 * One time `at` style tasks
 */
CREATE TABLE pgs_at (
    run_at timestamptz NOT NULL,
    /* run retroactively (when daemon was down at the time etc.)? */
    retroactive boolean DEFAULT false NOT NULL
) 
INHERITS (pgs_task);

DROP TRIGGER IF EXISTS pgs_at_change_tr ON pgs_at;
CREATE TRIGGER pgs_at_change_tr
AFTER INSERT OR UPDATE OR DELETE ON pgs_at
    FOR EACH ROW EXECUTE PROCEDURE tasks_change_tr();

/*
 * Periodic job runner.
 */
CREATE TABLE pgs_runner (
	/* how long after the task finished shall we run it again? */
    period interval  NOT NULL,
    last_finished    TIMESTAMPTZ
) 
INHERITS (pgs_task);

DROP TRIGGER IF EXISTS pgs_runner_change_tr ON pgs_runner;
CREATE TRIGGER pgs_runner_change_tr
AFTER INSERT OR UPDATE OR DELETE ON pgs_runner
    FOR EACH ROW EXECUTE PROCEDURE tasks_change_tr();


/*-*-*-*-*-*-*-*-*-*-* FUNCTIONS *-*-*-*-*-*-*-*-*-*-*/

DROP TYPE IF EXISTS task_t CASCADE;
CREATE TYPE task_t AS
(
	wait FLOAT,
	"type" TEXT,
	id INTEGER,
	job TEXT,
	role TEXT,
	run_at TIMESTAMPTZ,
	retroactive BOOLEAN
);

CREATE OR REPLACE FUNCTION next_task() RETURNS task_t AS $$
	DECLARE
		ncron      RECORD;
		nat        RECORD;
		nrun       RECORD;
		task       task_t;
		td         FLOAT;
		t          TIMESTAMPTZ;
		BEGIN
		SET search_path TO pgsched;
		/* TODO: if task.run_at == now() don't check further tables (AT first?) */

		/* AT */
		LOOP
			SELECT INTO nat * FROM pgs_at
				WHERE enabled IS TRUE AND last_run IS NULL
				ORDER BY run_at ASC LIMIT 1;
			/* purge missed not retroactive tasks if encountered one */
			IF nat.run_at IS NOT NULL AND NOT nat.retroactive AND nat.run_at < now() - '5 minutes'::interval THEN 
				/* DEBUG: remove in furure */
				RAISE NOTICE 'Purging missed not retroactive tasks.';
				DELETE FROM pgs_at WHERE enabled IS TRUE AND last_run IS NULL AND run_at < now() - '5 minutes'::interval;
			ELSE
				SELECT INTO task 0, 'at', nat.id, nat.job, nat.role, nat.run_at, nat.retroactive;   
				EXIT;
			END IF;
		END LOOP;

		/* CRON */
		SELECT INTO ncron id, job, role,
				cron_next_time(last_run, retroactive, c_min, c_hrs, c_day, c_mon, c_dow) AS run_at,
				retroactive FROM pgs_cron
			WHERE enabled IS TRUE
				AND (valid_from IS NULL OR valid_from <= now())
				AND (valid_to   IS NULL OR valid_to   >= now())
			ORDER BY run_at ASC LIMIT 1;
		IF ncron.run_at IS NOT NULL AND (task.run_at IS NULL OR ncron.run_at < task.run_at) THEN
			SELECT INTO task 0, 'cron', ncron.id, ncron.job, ncron.role, ncron.run_at, ncron.retroactive;   
		END IF;

		/* RUNNER */
		SELECT INTO nrun *, COALESCE(last_finished + period, now()) AS run_at FROM pgs_runner
			WHERE enabled IS TRUE
				AND (valid_from IS NULL OR valid_from <= now())
				AND (valid_to   IS NULL OR valid_to   >= now())
				AND (last_run IS NULL
					OR (last_finished IS NOT NULL AND last_finished >= last_run))
			ORDER BY run_at ASC LIMIT 1;
		IF nrun.run_at IS NOT NULL AND (task.run_at IS NULL OR nrun.run_at < task.run_at) THEN
			SELECT INTO task 0, 'runner', nrun.id, nrun.job, nrun.role, nrun.run_at, TRUE;   
		END IF;

		IF task.id IS NULL THEN
			task.wait := NULL;
			task."type" := NULL;
			RETURN task;
		END IF;
		td := extract(epoch FROM task.run_at - clock_timestamp());
		IF td > 0 THEN
			task.wait := td;
		ELSE
			EXECUTE 'UPDATE pgs_' || task."type" || ' SET last_run = ''' || clock_timestamp() || ''' WHERE id = ' || task.id ;
		END IF;
		RETURN task;
	END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION run_as(fun TEXT, role TEXT, proxyf TEXT) RETURNS INTEGER AS $$
	DECLARE
		useless RECORD;
    BEGIN
		RAISE NOTICE 'Proxy function: %()', proxyf ;
		EXECUTE 'CREATE OR REPLACE FUNCTION ' || proxyf || '() RETURNS VOID AS ''
			BEGIN PERFORM ' || fun || '()::VOID; END
			'' LANGUAGE plpgsql SECURITY DEFINER;
			ALTER FUNCTION ' || proxyf || '() OWNER TO ' || role;
		EXECUTE 'SET SESSION AUTHORIZATION ' || role;
		EXECUTE 'SELECT ' || proxyf || '()';
		EXECUTE 'DROP FUNCTION ' || proxyf || '();';
		RESET SESSION AUTHORIZATION;
		RETURN 0;
	EXCEPTION
		WHEN undefined_function THEN
			RAISE WARNING 'UNDEFINED FUNCTION: maybe %() is not defined?', fun;
			RETURN -1;
		WHEN undefined_object THEN
			RAISE WARNING 'UNDEFINED OBJECT: maybe ''%'' is invalid role?', role;
			RETURN -2;
		WHEN insufficient_privilege THEN
			RAISE WARNING 'INSUFFICIENT PRIVILEGE: maybe role ''%'' doesn''t have privilege to call %() ?', role, fun;
			RETURN -3;
		WHEN others THEN
			return -42;
	END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION run_task(task_type TEXT, task_id INTEGER) RETURNS INTEGER AS $$
	DECLARE
		task   RECORD;
		r      INTEGER;
    BEGIN
		EXECUTE 'SELECT job, role FROM pgsched.pgs_' || task_type || ' WHERE id = ' || task_id INTO task;
		IF task.job IS NULL THEN
			RAISE WARNING '% task % to run not found in pgs_runner. Ignoring.', task_type, task_id;
			RETURN -4;
		END IF;

		r := run_as(task.job, task.role, 'pgs_proxy_tmp');

		IF task_type = 'runner' THEN
			UPDATE pgsched.pgs_runner SET last_finished = clock_timestamp() WHERE id = task_id ;
		END IF;
			
		RETURN r;
	END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION month_days (year INTEGER, month INTEGER)
	RETURNS INTEGER
AS $$
	SELECT EXTRACT(day FROM
		(($1::text || '-' || $2::text || '-01')::date
			+ '1 month'::interval
			- '1 day'::interval))::INTEGER AS days
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION day_of_week (year INTEGER, month INTEGER, day INTEGER)
	RETURNS INTEGER
AS $$
	DECLARE
		dow INTEGER;
	BEGIN
		dow := to_char(($1::text || '-' || $2::text || '-' || $3::text)::date, 'D')::INTEGER - 1;
		IF dow = 0 THEN
			dow := 7;
		END IF;
		RETURN dow;
	END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cron_next_time (last_time TIMESTAMPTZ, retroactive BOOLEAN,
	c_min boolean[], c_hrs boolean[], c_day boolean[], c_mon boolean[], c_dow boolean[])
	RETURNS TIMESTAMPTZ
AS $$
	DECLARE
		min   INTEGER;
		hr    INTEGER;
		day   INTEGER;
		mon   INTEGER;
		dow   INTEGER;
		yr    INTEGER;
		yrmax INTEGER;
		old   INTEGER;
		mdays INTEGER;
		first BOOLEAN;
		next_min TIMESTAMPTZ;
		t_from   TIMESTAMPTZ;
	BEGIN
		IF last_time IS NULL THEN
			t_from := now();
		ELSE
			next_min := last_time + '1 minute'::interval;
			IF retroactive THEN
				t_from := next_min;
			ELSE
				t_from := now();
				IF t_from < next_min THEN
					t_from := next_min;
				END IF;
			END IF;
		END IF;

		yr  := EXTRACT(year   FROM t_from);
		mon := EXTRACT(month  FROM t_from);
		min := EXTRACT(minute FROM t_from);
		hr  := EXTRACT(hours  FROM t_from);
		day := EXTRACT(day    FROM t_from);
		RAISE NOTICE 't_from: %:%, %.%.%, %', hr, min, day, mon, yr, dow;

		first := TRUE;
		yrmax := yr + 5;
		WHILE yr <= yrmax LOOP
			-- year
			WHILE mon <= 12 LOOP
				-- month
				IF c_mon[mon] THEN
					-- day
					mdays := month_days(yr, mon);
					dow   := day_of_week(yr, mon, day);
					WHILE day <= mdays LOOP
						IF c_day[day] AND c_dow[dow] THEN
							-- hour
							WHILE hr < 24 LOOP
								IF c_hrs[hr+1] THEN
									-- min
									WHILE min < 60 LOOP
										IF c_min[min] THEN
											--RAISE NOTICE 'NEXT:   %:%, %.%.%, %', hr, min, day, mon, yr, dow;
											RETURN (yr::text || '-' || mon::text || '-' || day
												|| ' ' || hr || ':' || min)::TIMESTAMPTZ;
										END IF;
										min := min + 1;
									END LOOP;
								END IF;
								hr  := hr + 1;
								min := 0;
							END LOOP;
						END IF;
						day := day + 1;
						dow := dow + 1;
						IF dow > 7 THEN
							dow := 1;
						END IF;
						hr  := 0;
						min := 0;
					END LOOP;
					mon := mon + 1;
					day := 1;
					hr  := 0;
					min := 0;
				END IF;
				mon := mon + 1;
				day := 1;
				hr  := 0;
				min := 0;
			END LOOP;
			yr  := yr + 1;
			mon := 1;
			day := 1;
			hr  := 0;
			min := 0;
		END LOOP;
		RAISE EXCEPTION 'BUG/FAIL: Cron time can''t be found in next 5 years.';
	END
$$ LANGUAGE plpgsql;


DROP TYPE IF EXISTS parsed_cron_t CASCADE;
CREATE TYPE parsed_cron_t AS
(
    min boolean[],
    hrs boolean[],
    day boolean[],
    mon boolean[],
    dow boolean[]
);

CREATE LANGUAGE plpythonu;
CREATE OR REPLACE FUNCTION parse_cron (cron_time TEXT) RETURNS parsed_cron_t
AS $$
	import re
	from itertools import repeat

	re_number = re.compile('\d+$')
	re_range  = re.compile('(?:(\d+)-(\d+)|\*)(?:/(\d+))?')

	SPECIALS = {
		"hourly"   : '0 * * * *',
		"daily"    : '0 0 * * *',
		"midnight" : '0 0 * * *',
		"weekly"   : '0 0 * * 0',
		"monthly"  : '0 0 1 * *',
		"yearly"   : '0 0 1 1 *',
		"annually" : '0 0 1 1 *',
	}
	MONTHS = [
		None,
		'jan', 'feb', 'mar', 'apr', 'may',
		'jun', 'jul', 'aug', 'sep', 'oct',
		'nov', 'dec',
	]
	WEEK = [
		None,
		'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun',
	]
	ZH_FIRST, ZH_LAST = range(2)
	field_opts = [
		(59, None,   None,     'min'),
		(23, None,   None,     'hrs'),
		(31, None,   ZH_FIRST, 'day'),
		(12, MONTHS, ZH_FIRST, 'mon'),
		(7,  WEEK,   ZH_LAST,  'dow'),
	]

	class CronTime:
		def __init__(self):
			self.parsed_fields = None
			self.parsed = False
		
		def _parse_field(self, field, max, names = None, zero_hack = None):
			if field == '*': # time is of the essence ;)
				return tuple(repeat(True, max + int(zero_hack == None)))
			bt = list(repeat(False, max + 1))
			for part in field.split(','):
				if re_number.match(part):
					# plain number
					n = int(part)
					if n < 0 or n > max:
						return None
					bt[n] = True
				else:
					m = re_range.match(part)
					if m:
						# range
						f_str = m.group(1)
						if f_str:
							f = int(f_str)
							t = int(m.group(2))
							if f < 0 or t > max:
								return None
						else:
							# got '*'
							f = int(zero_hack != None)
							t = max
						step_str = m.group(3)
						if step_str:
							step = int(step_str)
						else:
							step = 1
						to_set = range(f, t + 1, step)
						if not to_set:
							return None
						for n in to_set:
							bt[n] = True
					else:
						lpart = part.lower()
						if names and lpart in names:
							# name
							n = names.index(lpart)
							bt[n] = True
						else:
							# fail
							return None
			if zero_hack == ZH_FIRST:
				bt[1] = bt[1] or bt[0]
				bt = bt[1:]
			elif zero_hack == ZH_LAST:
				bt[-1] = bt[-1] or bt[0]
				bt = bt[1:]
			return tuple(bt)
		
		def parse(self, time_str):
			self.parsed = False
			if time_str[0] == '@':
				sp = time_str[1:]
				if sp in SPECIALS:
					self.time_str = SPECIALS[sp]
				else:
					return False
			else:
				self.time_str = time_str
			fields = re.split('\s', self.time_str)
			if len(fields) != 5:
				return None
			self.parsed_fields = []
			for i in range(5):
				max, names, zero_hack, _ = field_opts[i]
				fa = self._parse_field(fields[i], max, names, zero_hack)
				if fa:
					self.parsed_fields.append(fa)
				else:
					return False
			self.parsed = True
			return True
		
		def get_parsed_fields(self, cron_time):
			self.parse(cron_time)
			if self.parsed:
				return self.parsed_fields
			else:
				return (None, None, None, None, None)
			
	c = CronTime()
	return c.get_parsed_fields(cron_time)
$$ LANGUAGE plpythonu;

RESET ROLE;
