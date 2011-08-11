CREATE SCHEMA pgscheduler;
GRANT ALL ON SCHEMA pgscheduler TO pgscheduler;
SET ROLE pgscheduler;
SET search_path TO pgscheduler;


CREATE OR REPLACE FUNCTION tasks_change() RETURNS trigger AS $$
    BEGIN
        NOTIFY pgs_tasks_change;
		RETURN NULL;
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

    /* run retroactively like anacron? */
    retroactive BOOLEAN DEFAULT FALSE NOT NULL,

    minutes   TEXT NOT NULL DEFAULT '*',
    hours     TEXT NOT NULL DEFAULT '*',
    weekdays  TEXT NOT NULL DEFAULT '*',
    monthdays TEXT NOT NULL DEFAULT '*',
    months    TEXT NOT NULL DEFAULT '*'
) 
INHERITS (pgs_task);


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
    EXECUTE PROCEDURE tasks_change();

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
    EXECUTE PROCEDURE tasks_change();

RESET ROLE;
