# REVIEW.md

The defects below were checked against the starter implementation and corrected in the current `app/main.py`. The examples describe how each issue can be reproduced or detected.

| # | Where | What was wrong | How to notice | Fix |
|---|---|---|---|---|
| 1 | Timestamp parsing | Float-based epoch conversion can lose millisecond precision; seconds, strings, floats, booleans, and out-of-range values must be rejected. | Send `punched_at` as a float, numeric string, boolean, or seconds instead of milliseconds; test values near millisecond boundaries. | Strict `EpochMs` validation and integer `timedelta` conversion. |
| 2 | Timezone conversion | Naive BSON datetimes can be mistaken for local time and produce incorrect IST calculations or API timestamps. | Test punch-ins near UTC/IST midnight and datetimes with and without `tzinfo`. | Use UTC-aware MongoDB datetimes and explicit UTC/IST conversion helpers. |
| 3 | R1 attendance date | Overnight-shift punches before `shift_end` belong to the previous attendance date. | For a 22:00–06:00 shift, punch in at 02:00 IST and check the attendance date. | `determine_attendance_date` applies the overnight rule. Equal shift times are also treated as overnight per `shift_end <= shift_start`. |
| 4 | R2 grace period | Late status must be strictly more than 10 minutes after shift start; the reported minutes are measured from shift start. | For a 09:30 shift, compare 09:40:00, 09:40:01, and 10:15:59. | `compute_late_minutes` uses a strict `delta > GRACE` check and floors whole minutes from shift start. |
| 5 | R3 overtime | Overtime is whole minutes after shift end and only counts at 30 minutes or more. | Test 29:59 and 30:00 after shift end, including an overnight shift. | `compute_overtime_minutes` floors minutes and applies the 30-minute threshold. |
| 6 | R4/R8 rounding | Binary floating-point and Python's default rounding can disagree with half-up contract rounding. | Test durations/ratios at half-way boundaries. | Decimal arithmetic with `ROUND_HALF_UP` is used for work hours and API calculations. |
| 7 | R5 half-day | Half-day depends on the already-rounded work-hours value being below 4.50. | Test raw durations that round to 4.49 and 4.50. | `compute_half_day` compares the rounded stored value against `4.50`. |
| 8 | Punch-in concurrency | A check-then-insert pattern can allow duplicate records for the same employee and attendance date. | Send simultaneous punch-in requests for the same employee/date. | Unique indexes on `emp_code` and on `(emp_code, date)` arbitrate races; duplicate-key errors become 409. |
| 9 | Punch-out concurrency | Two simultaneous punch-outs must not both close the same record. | Send concurrent punch-out requests against one open record. | A conditional `find_one_and_update` succeeds for one request; the loser receives 409. |
| 10 | Regularization audit history | Replacing history loses previous corrections; concurrent edits can overwrite each other. | Apply two corrections in sequence or concurrently and inspect `history`. | The update condition checks the read version and `$push` appends one history entry atomically. |
| 11 | Date range validation | An inverted inclusive date range must be rejected rather than silently returning an empty page. | Request `date_from=2026-10-20&date_to=2026-10-01`. | Endpoints validate range order and return 422. Calendar strings are pattern- and date-validated. |
| 12 | Pagination | Unbounded page sizes can return too many records and hurt latency/memory. | Request `page_size=1000`, `page_size=0`, or `page=0`. | Page is at least 1 and page size is restricted to 1–100. Empty out-of-range pages avoid unnecessary cursor reads. |
| 13 | Department summary | Starting analytics from attendance logs omits employees who have no logs and can include people who joined after the month. | Include a no-log employee and a future joiner. | The pipeline starts from employees, filters `joined_on` by month end, and left-joins monthly attendance. |
| 14 | Department summary average | Averaging per-employee averages weights employees incorrectly; only presence-status records with stored work hours count. | Give employees different numbers of valid attendance records. | The pipeline sums hundredths of work hours and record counts, then computes one record-weighted mean. |
| 15 | Late leaderboard | Array positions are not competition ranks; limiting before ranking drops cutoff ties. | Create totals such as 100, 90, 90, 80 and test `limit=3`. | `$rank` ranks by total late minutes; filtering by rank occurs after ranking and final ties sort by `emp_code`. |
| 16 | Department trend | Returning only dates with logs misses weekends and missing weekdays; computing the series in Python violates the database-pipeline requirement. | Query a range with no logs on some days and include weekends. | The aggregation generates every calendar date, computes rates and a 7-row moving average in MongoDB. |
| 17 | Indexing and explain | List and analytics queries need appropriate indexes and the explain route must use the same filters/pipelines as the endpoint. | Run `/admin/explain/...` with representative filters and inspect `executionStats`. | Indexes are created idempotently at startup; shared query/pipeline builders feed both endpoints and explain. |

## Checked and retained

- Analytics consume stored derived fields rather than silently recalculating historical records.
- Attendance is addressed by `(emp_code, date)`; MongoDB `_id` is not exposed by the API.
- `ABSENT` and `LEAVE` are correction/seed statuses, not statuses created by punch-in.
- A weekday with no attendance record is distinct from an explicit `ABSENT` record.
