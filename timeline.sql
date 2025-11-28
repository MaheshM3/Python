WITH time_counts AS (
    SELECT 
        location,
        start_time::time AS start_time_only,
        COUNT(*) AS frequency
    FROM jobs
    WHERE start_time IS NOT NULL
      AND start_time >= CURRENT_DATE - INTERVAL '180 days'
    GROUP BY location, start_time::time
),
usual_times AS (
    SELECT 
        location,
        start_time_only AS usual_start_time,
        frequency,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY frequency DESC, start_time_only) AS rn
    FROM time_counts
)
SELECT 
    location,
    usual_start_time
FROM usual_times
WHERE rn = 1
ORDER BY 
    usual_start_time ASC,      -- primary: real chronological order
    location ASC;              -- secondary: only as tie-breaker (alphabetical)