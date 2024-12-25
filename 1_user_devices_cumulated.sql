-- DDL for `user_devices_cumulated` table
CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    device_id NUMERIC,
    browser_type TEXT,
    date DATE,
    device_activity_datelist DATE[],
    PRIMARY KEY (user_id, device_id, browser_type, date)
);

-- Insert cumulative data from events into `user_devices_cumulated`
INSERT INTO user_devices_cumulated
WITH
    yesterday AS (
        -- Select records from the previous day
        SELECT *
        FROM user_devices_cumulated
        WHERE date = DATE('2023-01-30')
    ),
    today AS (
        -- Select records from the current day
        SELECT 
            e.user_id AS user_id,
            e.device_id AS device_id,
            d.browser_type AS browser_type,
            DATE(CAST(e.event_time AS TIMESTAMP)) AS date_active
        FROM events e
        JOIN devices d ON e.device_id = d.device_id
        WHERE CAST(CAST(e.event_time AS TIMESTAMP) AS DATE) = DATE('2023-01-31')
            AND user_id IS NOT NULL
        GROUP BY e.user_id, e.device_id, d.browser_type, DATE(CAST(e.event_time AS TIMESTAMP))
    )

-- Combine data from today and yesterday
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.device_id, y.device_id) AS device_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.device_activity_datelist
        ELSE ARRAY[t.date_active] || y.device_activity_datelist
    END AS device_activity_datelist
FROM today t
FULL OUTER JOIN yesterday y ON t.user_id = y.user_id AND t.device_id = y.device_id;

-- Generate `datelist_int` for each user device
WITH 
    user_devices AS (
        -- Select records from `user_devices_cumulated` for a specific date
        SELECT *
        FROM user_devices_cumulated
        WHERE date = DATE('2023-01-31')
    ), 
    series AS (
        -- Generate a series of dates for the month of January 2023
        SELECT *
        FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
    )

-- Calculate the `datelist_int` for each date in the series
SELECT 
    user_id,
    device_id,
    browser_type,
    date,
    CASE
        WHEN device_activity_datelist @> ARRAY[DATE(series_date)] THEN CAST(POW(2, 32 - (date - DATE_TRUNC('month', series_date))) AS BIGINT)
        ELSE 0
    END AS datelist_int
FROM user_devices
CROSS JOIN series;