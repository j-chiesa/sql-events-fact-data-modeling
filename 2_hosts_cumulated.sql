-- DDL for `hosts_cumulated` table
CREATE TABLE hosts_cumulated (
    host TEXT,
    date DATE,
    host_activity_datelist DATE[],
    PRIMARY KEY(host, date)
);

-- Incremental query to generate `host_activity_datelist`
INSERT INTO hosts_cumulated
WITH 
    yesterday AS (
        -- Select records from the previous day
        SELECT *
        FROM hosts_cumulated
        WHERE date = DATE('2023-01-30')
    ),
    today AS (
        -- Select records from the current day
        SELECT 
            host,
            DATE(CAST(event_time AS TIMESTAMP)) AS date_active
        FROM events
        WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
        GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
    )

-- Combine data from today and yesterday
SELECT
    COALESCE(t.host, y.host) AS host,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date,
    CASE 
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]  -- Initialize array if no previous dates
        WHEN t.date_active IS NULL THEN y.host_activity_datelist  -- Use previous array if no current activity
        ELSE ARRAY[t.date_active] || y.host_activity_datelist  -- Append current date to previous dates array
    END AS host_activity_datelist
FROM today t
FULL OUTER JOIN yesterday y ON t.host = y.host;
