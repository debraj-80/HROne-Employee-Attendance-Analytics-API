# DECISIONS.md

1. **Indexes.** A unique `employees(emp_code)` index protects employee identity. A unique `attendance_logs(emp_code, date)` index enforces one record per employee per attendance date and makes employee/date lookups efficient. The date/employee compound index supports the required descending-date listing order; status/date/employee helps status-filtered lists; employee/punch-in supports finding the latest eligible punch-in for punch-out. Department and joined-date indexes support employee filtering and headcount queries.

2. **Punch-in race.** Both requests may reach the insert, but the unique `(emp_code, date)` index allows only one document. That request returns 201. The other insert raises `DuplicateKeyError`, which is translated to 409. No separate pre-check is relied on for correctness.

3. **Leaderboard ties.** MongoDB `$rank` uses total late minutes descending, so equal totals share a rank and the next rank is skipped (1, 2, 2, 4). The `rank <= limit` filter is applied after ranking, so all employees tied at the cutoff rank are returned; tied output rows are ordered by employee code.

4. **Employees without logs.** The department summary starts from employees who joined on or before the month's last day, then left-joins that employee's logs for the month. Employees with no matching logs still contribute one to headcount; employees who join later are excluded.

5. **At 100× scale.** I would measure real execution plans and latency first, then consider monthly pre-aggregated analytics, retention/archival policies, and workload isolation for expensive reports. I would keep the source attendance records authoritative and update summaries idempotently so corrections do not leave aggregates stale.
