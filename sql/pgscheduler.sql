CREATE SCHEMA pgscheduler;
GRANT ALL ON SCHEMA pgscheduler TO pgscheduler;
SET ROLE pgscheduler;
SET search_path TO pgscheduler;


CREATE OR REPLACE FUNCTION rrcheck(dr OID, rr OID) RETURNS BOOLEAN AS $$
	DECLARE
		pr OID;
	BEGIN
		FOR pr IN SELECT roleid FROM pg_auth_members WHERE member = dr LOOP
			IF pr = rr OR pgscheduler.rrcheck(pr, rr) THEN
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

		RETURN pgscheduler.rrcheck(dr.oid, rr.oid);
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tasks_change_tr() RETURNS trigger AS $$
	DECLARE
		check_role BOOLEAN;
    BEGIN
		check_role := FALSE;
		IF TG_OP = 'UPDATE' THEN
			IF OLD.id != NEW.id THEN
				RAISE EXCEPTION 'ID change is forbidden.';
			END IF;
			IF OLD.role != NEW.role THEN
				check_role := TRUE;
			END IF;
		ELSE
			check_role := TRUE;
		END IF;

		RAISE NOTICE '%: %, %', current_user, NEW.role, check_role;
		IF check_role AND NOT pgscheduler.role_check(current_user, NEW.role) THEN
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
		p parsed_cron_t;
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
BEFORE INSERT OR UPDATE ON pgs_cron
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
AFTER INSERT OR UPDATE ON pgs_at
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
AFTER INSERT OR UPDATE ON pgs_runner
    FOR EACH ROW EXECUTE PROCEDURE tasks_change_tr();

RESET ROLE;
