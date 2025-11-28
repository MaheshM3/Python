WITH cleaned AS (
    SELECT 
        location,
        -- Round to nearest 10 minutes (change to '5 minutes' if you want tighter)
        DATE_TRUNC('hour', start_time) 
        + INTERVAL '10 min' * ROUND(EXTRACT(MINUTE FROM start_time) / 10.0) AS typical_time
    FROM jobs
    WHERE start_time >= CURRENT_DATE - INTERVAL '180 days'
      AND start_time IS NOT NULL
),
counts AS (
    SELECT 
        location,
        typical_time::time AS usual_start_time,
        COUNT(*) AS frequency
    FROM cleaned
    GROUP BY location, typical_time
),
ranked AS (
    SELECT 
        location,
        usual_start_time,
        frequency,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY frequency DESC, usual_start_time) AS rn
    FROM counts
)
SELECT 
    location,
    usual_start_time
FROM ranked
WHERE rn = 1
ORDER BY usual_start_time;   -- perfect chronological order