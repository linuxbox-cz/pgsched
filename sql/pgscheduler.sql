CREATE SCHEMA pgscheduler;
GRANT ALL ON SCHEMA pgscheduler TO pgscheduler;
SET ROLE pgscheduler;
SET search_path TO pgscheduler;

/*
 * Let "task" be a (a series of?) job scheduled to run at a specific time.
 * This table has common columns for all the scheduling methods (tables).
 */
CREATE TABLE pgs_task (
    id         SERIAL PRIMARY KEY,
    job        TEXT NOT NULL, /* function name or a reference to jobs table? */
    created    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "desc"       TEXT,
    role       TEXT NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    valid_from TIMESTAMPTZ,
    valid_to   TIMESTAMPTZ
);

/*
 * Periodic cron style tasks.
 */
CREATE TABLE pgs_cron (

    /* run retroactively like anacron? */
    retroactive BOOLEAN DEFAULT FALSE NOT NULL,
    t_last_run  TIMESTAMPTZ,

    /* Universal, unambiguous, programmer friendly approach from pgagent.
     * How much better is it than saving cron-like strings and evaluating them
     * each time? I'm not quite sure... */
    /* Also, what about time zone? */
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
    t_at timestamptz NOT NULL,
    /* run retroactively (when daemon was down at the time etc.)? */
    retroactive boolean DEFAULT false NOT NULL
) 
INHERITS (pgs_task);

/*
 * Periodic job runner.
 */
CREATE TABLE pgs_runner (
	/* how long after the task finished shall we run it again? */
    period timestamp NOT NULL
) 
INHERITS (pgs_task);

RESET ROLE;
