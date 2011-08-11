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
	t TIMESTAMPTZ,
	retroactive BOOLEAN
);

CREATE OR REPLACE FUNCTION next_task() RETURNS task_t AS $$
	DECLARE
		ncron      RECORD;
		nat        RECORD;
		nrunner    RECORD;
		task       task_t;
		td         FLOAT;
		BEGIN
		SET search_path TO pgscheduler;
		SELECT INTO nat * FROM pgs_at
			WHERE enabled IS TRUE AND last_run IS NULL
			ORDER BY run_at ASC LIMIT 1;
		SELECT INTO task 0, 'at', nat.id, nat.job, nat.role, nat.run_at, nat.retroactive;   
		IF task.id IS NULL THEN
			task.wait := NULL;
			task."type" := NULL;
			RETURN task;
		END IF;
		-- date_part('epoch', nat.t_at)::int
		SELECT INTO td extract(epoch from task.t - NOW());
		IF td > 0 THEN
			task.wait := td;
		ELSE
			-- TODO: cron/at/runner cases
			-- TODO: isn't it too late? (retroactive)
			UPDATE pgs_at SET last_run = task.t WHERE id = task.id;
		END IF;
		RETURN task;
	END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION run_task(TEXT, INTEGER) RETURNS INTEGER AS $$
	DECLARE
	    task_type  ALIAS FOR $1;
		task_id    ALIAS FOR $2;
		task   RECORD;
    BEGIN
        SET search_path TO pgscheduler;
		CASE task_type
			WHEN 'at' THEN
				SELECT INTO task job, role FROM pgs_at WHERE id = task_id;
				IF task.job IS NULL THEN
					RAISE EXCEPTION 'AT task to run not found in pgs_at.';
				END IF;
				DELETE FROM pgs_at WHERE id = task_id;
			ELSE
				RAISE EXCEPTION 'Not implemented yet: %', task_type;
		END CASE;

		RAISE NOTICE 'RUNNING % task: %', task_type, task.job;
		IF task.role IS NOT NULL THEN
			EXECUTE 'SET ROLE ' || task.role ;
		END IF;
		EXECUTE 'SELECT ' || task.job || '()';
		RETURN 0;
	END
$$ LANGUAGE plpgsql;

RESET ROLE;
