# SQL Events Fact Data Modeling

This repository demonstrates a data engineering solution focused on constructing cumulative and incremental queries to support fact data modeling. The project addresses complex requirements such as tracking user device activity, logging host activity, and generating monthly reduced fact tables.

## Problem / Opportunity

The task involves transforming and modeling datasets (devices and events) to support analytical requirements. Key objectives include creating tables and queries to handle:

1. Tracking users' active days by browser type.
2. Logging host activity across different dates.
3. Generating monthly reduced fact tables with hit counts and unique visitors.

These objectives ensure efficient analysis and reporting for large-scale event and device datasets.

## How I Identified the Problem

The need for a well-structured data model was identified while analyzing the events and devices datasets. The gaps included:

- Lack of cumulative tracking for user or host activity.
- Absence of efficient summarization for monthly reporting.
- Complexities in handling incremental updates for daily activity data.

The inefficiencies were apparent during schema and query design exploration.

## The Solution

The solution includes:

1. **Creating tables**:
   - `user_devices_cumulated`: Tracks cumulative user device activity by browser type.
   - `hosts_cumulated`: Logs cumulative host activity by date.
   - `host_activity_reduced`: A monthly fact table summarizing hits and unique visitors.

2. **Developing queries**:
   - Cumulative queries for device and host activity.
   - Incremental updates for host activity.
   - Monthly summarization for reduced fact tables.

3. **SQL Scripts**:
   - `1_user_devices_cumulated.sql`: Defines the table and generates cumulative user device activity.
   - `2_hosts_cumulated.sql`: Defines the table and logs cumulative host activity.
   - `3_host_activity_reduced.sql`: Defines the table and generates monthly summaries.

## How I Came Up With the Solution

The solution was devised based on:

1. **Best practices in fact table modeling**:
   - Emphasizing clarity, consistency, and scalability in schema design.
   - Designing cumulative and incremental queries to handle large datasets.

2. **Iterative problem-solving**:
   - Identifying dataset inefficiencies through query testing.
   - Refining schema and query logic to meet analytical needs.

3. **Data engineering principles**:
   - Using robust data structures like arrays and maps.
   - Applying incremental updates to optimize performance.

## The Process for Implementing It

1. **Analyze requirements**:
   - Reviewed dataset structure and analytical needs.
   - Identified key objectives for cumulative and incremental modeling.

2. **Design table schemas**:
   - Created normalized schemas for cumulative and fact tables.

3. **Develop queries**:
   - Wrote cumulative and incremental SQL scripts for user device and host activity.

4. **Test and validate**:
   - Ensured correctness of cumulative tracking and monthly summaries.

5. **Optimize and document**:
   - Improved query performance and provided clear script documentation.

---

### Files in This Repository

- **`1_user_devices_cumulated.sql`**: DDL and queries for cumulative user device activity.
- **`2_hosts_cumulated.sql`**: DDL and queries for cumulative host activity.
- **`3_host_activity_reduced.sql`**: DDL and queries for monthly reduced fact tables.

---

This project highlights SQL skills, database design, and incremental query implementation to solve data modeling challenges effectively.
