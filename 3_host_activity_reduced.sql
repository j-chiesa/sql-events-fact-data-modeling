-- DDL for `host_activity_reduced` table
CREATE TABLE host_activity_reduced (
    month_start DATE,
    host TEXT,
    hit_array REAL[],
    unique_visitors_array REAL[],
    PRIMARY KEY(month_start, host)
);

-- Incremental query that loads `host_activity_reduced` day-by-day
INSERT INTO host_activity_reduced
WITH
    yesterday_array AS (
        -- Select records from the beginning of the month
        SELECT *
        FROM host_activity_reduced
        WHERE month_start = DATE('2023-01-01')
    ),
    daily_aggregate AS (
        -- Aggregate daily hits and unique visitors for each host
        SELECT 
            host,
            DATE(CAST(event_time AS TIMESTAMP)) AS date,
            COUNT(1) AS hit,
            COUNT(DISTINCT user_id) AS unique_visitors
        FROM events
        WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
            AND user_id IS NOT NULL
        GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
    )
    
-- Combine daily aggregate data with existing monthly data
SELECT 
    COALESCE(DATE_TRUNC('month', d.date), y.month_start) AS month_start,
    COALESCE(d.host, y.host) AS host,
    CASE
        WHEN y.hit_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(d.date - DATE(DATE_TRUNC('month', d.date)), 0)]) || ARRAY[COALESCE(d.hit, 0)]
        WHEN y.hit_array IS NOT NULL THEN y.hit_array || ARRAY[COALESCE(d.hit, 0)]
    END AS hit_array,
    CASE
        WHEN y.unique_visitors_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(d.date - DATE(DATE_TRUNC('month', d.date)), 0)]) || ARRAY[COALESCE(d.unique_visitors, 0)]
        WHEN y.unique_visitors_array IS NOT NULL THEN y.unique_visitors_array || ARRAY[COALESCE(d.unique_visitors, 0)]
    END AS unique_visitors_array
FROM daily_aggregate d
FULL OUTER JOIN yesterday_array y ON d.host = y.host
ON CONFLICT (month_start, host)
DO UPDATE SET hit_array = EXCLUDED.hit_array, unique_visitors_array = EXCLUDED.unique_visitors_array;
