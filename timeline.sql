WITH cleaned AS (
    SELECT 
        location,
        DATE_TRUNC('hour', start_time) 
        + INTERVAL '10 min' * ROUND(EXTRACT(MINUTE FROM start_time) / 10.0) AS start_rounded,
        (end_time - start_time) AS duration_interval
    FROM jobs
    WHERE start_time >= CURRENT_DATE - INTERVAL '180 days'
      AND end_time IS NOT NULL AND end_time > start_time
),
usual_start AS (
    SELECT location, 
           start_rounded::time AS usual_start_time,
           ROW_NUMBER() OVER (PARTITION BY location ORDER BY COUNT(*) DESC) AS rn
    FROM cleaned
    GROUP BY location, start_rounded
),
usual_duration AS (
    SELECT location,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY end_time - start_time) AS median_duration
    FROM jobs
    WHERE start_time >= CURRENT_DATE - INTERVAL '180 days'
      AND end_time IS NOT NULL AND end_time > start_time
    GROUP BY location
)
SELECT 
    us.location,
    us.usual_start_time AS start_time,                     -- e.g. 06:10:00
    (us.usual_start_time + ud.median_duration)::time AS end_time,  -- calculated end
    ud.median_duration AS duration_interval
FROM usual_start us
JOIN usual_duration ud ON us.location = ud.location
WHERE us.rn = 1
ORDER BY start_time;
=================================================================================================================

WITH cleaned AS (
    SELECT 
        location,
        -- Rounded start time (nearest 10 min) → most common "usual" start
        DATE_TRUNC('hour', start_time) 
        + INTERVAL '10 min' * ROUND(EXTRACT(MINUTE FROM start_time) / 10.0) AS start_rounded,
        
        -- Duration as interval
        (end_time - start_time) AS duration_interval
    FROM jobs
    WHERE start_time IS NOT NULL
      AND end_time IS NOT NULL
      AND end_time > start_time
      AND start_time >= CURRENT_DATE - INTERVAL '180 days'
),
-- Most common rounded start time per location
usual_start AS (
    SELECT 
        location,
        start_rounded::time AS usual_start_time,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY COUNT(*) DESC) AS rn
    FROM cleaned
    GROUP BY location, start_rounded
),
-- Median duration per location (the real "usual" runtime)
usual_duration AS (
    SELECT 
        location,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY end_time - start_time
        ) AS median_duration_interval
    FROM jobs
    WHERE start_time >= CURRENT_DATE - INTERVAL '180 days'
      AND end_time IS NOT NULL
      AND end_time > start_time
    GROUP BY location
)
-- Final result with duration in "dd hh mm ss" format
SELECT 
    us.location,
    us.usual_start_time,
    -- Beautiful dd hh mm ss format (only shows units that are >0)
    TRIM(
        COALESCE(EXTRACT(DAY FROM ud.median_duration_interval)-1 || 'd ', '')
        || COALESCE(EXTRACT(HOUR FROM ud.median_duration_interval) || 'h ', '')
        || COALESCE(EXTRACT(MINUTE FROM ud.median_duration_interval) || 'm ', '')
        || EXTRACT(SECOND FROM ud.median_duration_interval) || 's'
    ) AS usual_duration
FROM usual_start us
JOIN usual_duration ud ON us.location = ud.location
WHERE us.rn = 1
ORDER BY us.usual_start_time ASC;
