SET ROLE pgscheduler;
SET search_path TO pgscheduler;

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
		SET search_path TO pgscheduler;
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


CREATE OR REPLACE FUNCTION run_as(fun TEXT, role TEXT, prefix TEXT) RETURNS INTEGER AS $$
	DECLARE
		proxyf  TEXT;
		useless RECORD;
    BEGIN
		proxyf := prefix || md5(random()::text);
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
			RAISE WARNING 'UNDEFINED OBJECT: maybe ''%'' is invalid role?', fun;
			RETURN -2;
		WHEN others THEN
			return -42;
	END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION run_task(task_type TEXT, task_id INTEGER) RETURNS INTEGER AS $$
	DECLARE
		task   RECORD;
		r      INTEGER;
    BEGIN
        SET search_path TO pgscheduler;
		EXECUTE 'SELECT job, role FROM pgs_' || task_type || ' WHERE id = ' || task_id INTO task;
		IF task.job IS NULL THEN
			RAISE WARNING '% task to run not found in pgs_runner. Ignoring.', task_type;
			RETURN -3;
		END IF;

		r := run_as(task.job, task.role, 'pgs_proxy_');

		IF task_type = 'runner' THEN
			UPDATE pgs_runner SET last_finished = clock_timestamp() WHERE id = task_id ;
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
