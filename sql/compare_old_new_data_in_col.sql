-- Step 1: Insert records into the history table when there's a change in zone, team, or title
INSERT INTO staff_history (emp_id, zone, team, title, firstname, lastname, mobile, change_date, comments)
SELECT 
    sm.emp_id,
    sm.zone,
    sm.team,
    sm.title,
    sm.firstname,
    sm.lastname,
    sm.mobile,
    CURRENT_TIMESTAMP,  -- The current timestamp for when the change occurred
    -- Construct a JSONB object for old vs new values in the "comments" field
    jsonb_build_object(
        'changes', jsonb_build_object(
            'zone', jsonb_build_object('old', sm.zone, 'new', ss.zone),
            'team', jsonb_build_object('old', sm.team, 'new', ss.team),
            'title', jsonb_build_object('old', sm.title, 'new', ss.title)
        )
    ) AS comments
FROM staff_master sm
JOIN staff_staging ss ON sm.emp_id = ss.emp_id
WHERE (
    sm.zone != ss.zone OR  -- Only insert into history if zone is different
    sm.team != ss.team OR  -- Only insert into history if team is different
    sm.title != ss.title    -- Only insert into history if title is different
);

-- Step 2: Update the master table with the data from the staging table (regardless of changes)
UPDATE staff_master sm
SET
    sm.zone = ss.zone,
    sm.team = ss.team,
    sm.title = ss.title,
    sm.firstname = ss.firstname,
    sm.lastname = ss.lastname,
    sm.mobile = ss.mobile
FROM staff_staging ss
WHERE sm.emp_id = ss.emp_id;
