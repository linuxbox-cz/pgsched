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
		BEGIN
		SET search_path TO pgscheduler;
		/* AT */
		SELECT INTO nat * FROM pgs_at
			WHERE enabled IS TRUE AND last_run IS NULL
			ORDER BY run_at ASC LIMIT 1;
		SELECT INTO task 0, 'at', nat.id, nat.job, nat.role, nat.run_at, nat.retroactive;   
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
		-- date_part('epoch', nat.t_at)::int
		SELECT INTO td extract(epoch from task.run_at - clock_timestamp());
		IF td > 0 THEN
			task.wait := td;
		ELSE
			-- TODO: isn't it too late? (retroactive)
			EXECUTE 'UPDATE pgs_' || task."type" || ' SET last_run = ''' || task.run_at || ''' WHERE id = ' || task.id ;
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
			WHEN 'runner' THEN
				SELECT INTO task job, role FROM pgs_runner WHERE id = task_id;
				IF task.job IS NULL THEN
					RAISE EXCEPTION 'RUNNER task to run not found in pgs_runner.';
				END IF;
			ELSE
				RAISE EXCEPTION 'Not implemented yet: %', task_type;
		END CASE;

		RAISE NOTICE 'RUNNING % task: %', task_type, task.job;
		IF task.role IS NOT NULL THEN
			EXECUTE 'SET ROLE ' || task.role ;
		END IF;
		EXECUTE 'SELECT ' || task.job || '()';

		IF task_type = 'runner' THEN
			UPDATE pgs_runner SET last_finished = clock_timestamp() WHERE id = task_id ;
		END IF;
			
		RETURN 0;
	END
$$ LANGUAGE plpgsql;

RESET ROLE;
