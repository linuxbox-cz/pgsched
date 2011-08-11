SET ROLE pgscheduler;
SET search_path TO pgscheduler;

DROP TYPE IF EXISTS task_t CASCADE;
CREATE TYPE task_t AS
(
    "type" text,
    id integer,
    job text,
    role text,
    t bigint,
    retroactive boolean
);

CREATE OR REPLACE FUNCTION next_task() RETURNS task_t AS $$
	DECLARE
		ncron   RECORD;
		nat     RECORD;
		nrunner RECORD;
		next_task   task_t;
    BEGIN
        SET search_path TO pgscheduler;
		SELECT INTO nat * FROM pgs_at
			WHERE enabled IS TRUE ORDER BY t_at ASC LIMIT 1;
		SELECT INTO next_task 'at', nat.id, nat.job, nat.role, date_part('epoch', nat.t_at)::bigint, nat.retroactive;   
		RETURN next_task;
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
			WHEN "at" THEN
				SELECT INTO task job, role FROM pgs_at
					WHERE id = task_id;
				RAISE NOTICE 'RUNNING %', task.job;
			ELSE
				RAISE EXCEPTION 'Not implemented yet: %', task_type;
		END CASE;
		RETURN 0;
	END
$$ LANGUAGE plpgsql;

RESET ROLE;
