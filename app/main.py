"""Employee Attendance & Analytics API (FastAPI + MongoDB).

Everything lives in this one file, in this order:
  1. configuration and the MongoDB client
  2. time / business-rule helpers (R1 - R5)
  3. (de)serialisation helpers (epoch milliseconds <-> BSON datetimes)
  4. request models and validated parameter types
  5. query and aggregation-pipeline builders (shared by the endpoints AND /admin/explain)
  6. endpoints

Design rules that explain most of the code:
  * Instants are BSON datetimes in MongoDB and integer epoch milliseconds in the API; conversion happens only in the
    serialisation helpers.
  * Dates / months / shift times are plain strings, compared lexicographically (zero-padded, so that is correct).
  * Every write that can race is made atomic by the database: a unique index (punch-in, employee create) or a
    conditional find_one_and_update (punch-out, regularize).
  * Analytics read the *stored* derived values; they never recompute them from punch times.
"""
import json
import logging
import os
import threading
from calendar import monthrange
from contextlib import asynccontextmanager
from datetime import date as Date
from datetime import datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Dict, List, Literal, Optional, Tuple

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.exceptions import RequestValidationError
from pydantic import AfterValidator, BaseModel, Field, field_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# --------------------------------------------------------------------------------------------------------------------
# 1. Configuration
# --------------------------------------------------------------------------------------------------------------------

load_dotenv()  # never overrides variables that are already set in the real environment

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

log = logging.getLogger("attendance")

# tz_aware=True: PyMongo returns aware (UTC) datetimes instead of naive ones, so nothing is ever mistaken for local
# time. A short server-selection timeout keeps /health and start-up from hanging when MongoDB is down.
client = MongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MS_PER_DAY = 86_400_000

EPOCH_MS_MIN = 100_000_000_000  # anything smaller looks like seconds
EPOCH_MS_MAX = 4_102_444_800_000

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]  # R6
GRACE = timedelta(minutes=10)  # R2
MIN_OVERTIME_MINUTES = 30  # R3
HALF_DAY_BELOW = Decimal("4.50")  # R5
MAX_SHIFT_SPAN = timedelta(hours=24)
MAX_TREND_DAYS = 92

DATE_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
MONTH_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
HHMM_RE = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

_indexes_ready = False
_index_lock = threading.Lock()


def ensure_indexes() -> bool:
    """Create every index the endpoints need. Idempotent: re-creating an identical index is a no-op.

    Each index is created on its own so that one failure (for example a pre-existing index with the same keys but a
    different name) cannot stop the app from starting. Returns True when all of them are in place.
    """
    global _indexes_ready
    with _index_lock:
        specs = [
            # employees
            (db.employees, [("emp_code", ASCENDING)], {"unique": True}),  # identity + duplicate-create race
            (db.employees, [("department", ASCENDING), ("emp_code", ASCENDING)], {}),  # list by dept, trend/summary
            (db.employees, [("joined_on", ASCENDING)], {}),  # headcount (R9) without a department
            # attendance_logs
            (db.attendance_logs, [("emp_code", ASCENDING), ("date", ASCENDING)], {"unique": True}),  # one record/day
            (db.attendance_logs, [("date", DESCENDING), ("emp_code", ASCENDING)], {}),  # list order + month scans
            (db.attendance_logs, [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {}),
            (db.attendance_logs, [("emp_code", ASCENDING), ("punch_in", DESCENDING)], {}),  # punch-out lookup
        ]
        ok = True
        for collection, keys, options in specs:
            try:
                collection.create_index(keys, **options)
            except PyMongoError as exc:
                ok = False
                log.warning("index %s on %s not created: %s", keys, collection.name, exc)
        _indexes_ready = ok
        return ok


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except Exception as exc:  # MongoDB not reachable yet: /health keeps retrying
        log.warning("index creation deferred: %s", exc)
    yield


app = FastAPI(
    title="Employee Attendance & Analytics API",
    version="2.0.0",
    description="Backend for an HR attendance system built with FastAPI and MongoDB.",
    lifespan=lifespan,
)

# --------------------------------------------------------------------------------------------------------------------
# 2. Time and business-rule helpers
# --------------------------------------------------------------------------------------------------------------------


def as_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Treat a naive datetime as UTC (BSON dates are always UTC) and return an aware one."""
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def ms_to_datetime(ms: int) -> datetime:
    # Integer arithmetic only: float division would turn some millisecond values into the wrong microsecond.
    return EPOCH + timedelta(milliseconds=ms)


def datetime_to_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    return (as_utc(value) - EPOCH) // timedelta(milliseconds=1)


def truncate_seconds(value: datetime) -> datetime:
    """R1: instants are truncated to whole seconds before any calculation, and the truncated value is stored."""
    return value.replace(microsecond=0)


def now_ms_precision() -> datetime:
    """'Now' at the precision BSON can store (milliseconds), so what we return equals what we saved."""
    now = datetime.now(UTC)
    return now.replace(microsecond=(now.microsecond // 1000) * 1000)


def _hhmm(value: str) -> time:
    hours, minutes = value.split(":")
    return time(int(hours), int(minutes))


def is_overnight(shift_start: str, shift_end: str) -> bool:
    """R1: shift_end <= shift_start means the shift crosses midnight (22:00 - 06:00; equal times count too)."""
    return _hhmm(shift_end) <= _hhmm(shift_start)


def determine_attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    """R1: the record's date is the IST calendar date of the punch-in, except that on an overnight shift a punch-in
    earlier than shift_end belongs to the shift that started on the previous day."""
    local = as_utc(punch_in).astimezone(IST)
    day = local.date()
    if is_overnight(shift_start, shift_end) and local.time() < _hhmm(shift_end):
        day -= timedelta(days=1)
    return day.isoformat()


def _ist_moment(day: str, hhmm: str) -> datetime:
    return datetime.combine(Date.fromisoformat(day), _hhmm(hhmm), tzinfo=IST)


def compute_late_minutes(punch_in: datetime, att_date: str, shift_start: str) -> int:
    """R2: late only if STRICTLY more than 10:00 after shift_start; then whole minutes since shift_start (not since
    the end of the grace period). 09:40:00 -> 0, 09:40:01 -> 10, 10:15:59 -> 45 for a 09:30 shift."""
    delta = truncate_seconds(as_utc(punch_in)) - _ist_moment(att_date, shift_start)
    if delta > GRACE:
        return delta // timedelta(minutes=1)
    return 0


def compute_overtime_minutes(punch_out: datetime, att_date: str, shift_start: str, shift_end: str) -> int:
    """R3: whole minutes from shift_end to punch_out, counted only when at least 30. The end of an overnight shift
    falls on the next calendar day."""
    end = _ist_moment(att_date, shift_end)
    if is_overnight(shift_start, shift_end):
        end += timedelta(days=1)
    minutes = (truncate_seconds(as_utc(punch_out)) - end) // timedelta(minutes=1)
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def round_half_up(value: Any, places: int) -> float:
    """Half-up rounding (R4 / R8). Decimal arithmetic; Python's round() is banker's rounding on binary floats."""
    quantum = Decimal(1).scaleb(-places)
    return float(Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP))


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: (punch_out - punch_in) in seconds / 3600, rounded half-up to 2 decimals. Computed exactly with Decimal so
    that 4.505 hours is 4.51 and not whatever a float division happens to give."""
    seconds = int((truncate_seconds(as_utc(punch_out)) - truncate_seconds(as_utc(punch_in))).total_seconds())
    hours = Decimal(seconds) / Decimal(3600)
    return float(hours.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_half_day(work_hours: Optional[float]) -> bool:
    """R5: the ROUNDED work_hours is below 4.50."""
    return work_hours is not None and Decimal(str(work_hours)) < HALF_DAY_BELOW


def derive_fields(emp: Dict[str, Any], att_date: str, status: str, punch_in: Optional[datetime],
                  punch_out: Optional[datetime]) -> Dict[str, Any]:
    """The four derived fields for a record in its final state (DATA_MODEL.md, 'Stored derived values')."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, att_date, emp["shift_start"])
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    work_hours = compute_work_hours(punch_in, punch_out)
    return {
        "work_hours": work_hours,
        "late_minutes": late,
        "overtime_minutes": compute_overtime_minutes(punch_out, att_date, emp["shift_start"], emp["shift_end"]),
        "half_day": compute_half_day(work_hours),
    }


def month_bounds(month: str) -> Tuple[str, str]:
    """String bounds for `date` filters. The upper bound is not a real date for short months, which does not matter:
    it is only ever compared as a string, and no valid date of the month sorts above 'YYYY-MM-31'."""
    return f"{month}-01", f"{month}-31"


def month_last_day(month: str) -> str:
    year, mon = int(month[:4]), int(month[5:7])
    return Date(year, mon, monthrange(year, mon)[1]).isoformat()


def count_working_days(month: str, joined_on: Optional[str]) -> int:
    """R7: Monday-Friday, no holiday calendar, counted from joined_on for someone who joined during the month."""
    year, mon = int(month[:4]), int(month[5:7])
    first, last = Date(year, mon, 1), Date(year, mon, monthrange(year, mon)[1])
    start = first
    try:
        if joined_on:
            start = max(first, Date.fromisoformat(joined_on))
    except ValueError:
        pass
    if start > last:
        return 0
    total = (last - start).days + 1
    weeks, extra = divmod(total, 7)
    return weeks * 5 + sum(1 for i in range(extra) if (start + timedelta(days=i)).weekday() < 5)


# --------------------------------------------------------------------------------------------------------------------
# 3. Serialisation
# --------------------------------------------------------------------------------------------------------------------


def serialize_history(entries: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Convert history for the API. The from/to values of punch_in and punch_out are BSON dates inside a nested
    object, which is easy to forget; they become epoch milliseconds like every other instant."""
    result = []
    for entry in entries or []:
        changes: Dict[str, Any] = {}
        for field, change in (entry.get("changes") or {}).items():
            change = dict(change or {})
            if field in ("punch_in", "punch_out"):
                change = {k: datetime_to_ms(v) if isinstance(v, datetime) else v for k, v in change.items()}
            changes[field] = change
        result.append({"at": datetime_to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"),
                       "changes": changes})
    return result


def serialize_attendance(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Exactly the AttendanceRecord fields, never `_id`. Legacy records may lack history / half_day / the counters:
    missing means empty / false / 0."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": datetime_to_ms(doc.get("punch_in")),
        "punch_out": datetime_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": int(doc.get("late_minutes") or 0),
        "overtime_minutes": int(doc.get("overtime_minutes") or 0),
        "half_day": bool(doc.get("half_day", False)),
        "history": serialize_history(doc.get("history")),
    }


def serialize_employee(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": datetime_to_ms(doc.get("created_at")),
    }


def unprocessable(message: str, where: Tuple[str, ...] = ("body",)) -> None:
    """Raise a 422 in FastAPI's own validation-error shape ({"detail": [ ... ]}), as the contract describes."""
    raise RequestValidationError([{"type": "value_error", "loc": where, "msg": message, "input": None}])


# --------------------------------------------------------------------------------------------------------------------
# 4. Request models and validated parameter types
# --------------------------------------------------------------------------------------------------------------------


def _check_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return value
    try:
        Date.fromisoformat(value)
    except ValueError:
        raise ValueError("must be a real calendar date, YYYY-MM-DD")
    return value


def _check_month(value: Optional[str]) -> Optional[str]:
    if value is None:
        return value
    if int(value[:4]) < 1:
        raise ValueError("year must be at least 0001")
    return value


# Instants: strict ints only (floats, numeric strings and booleans are rejected), inside the contract's range.
EpochMs = Annotated[int, Field(strict=True, ge=EPOCH_MS_MIN, le=EPOCH_MS_MAX)]
StatusValue = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

DateQuery = Annotated[Optional[str], Query(pattern=DATE_RE), AfterValidator(_check_date)]
DateRequired = Annotated[str, Query(pattern=DATE_RE), AfterValidator(_check_date)]
DatePath = Annotated[str, Path(pattern=DATE_RE), AfterValidator(_check_date)]
MonthRequired = Annotated[str, Query(pattern=MONTH_RE), AfterValidator(_check_month)]
MonthOptional = Annotated[Optional[str], Query(pattern=MONTH_RE), AfterValidator(_check_month)]
PageParam = Annotated[int, Query(ge=1)]
PageSizeParam = Annotated[int, Query(ge=1, le=100)]  # R10


class EmployeeCreate(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=EMAIL_RE)
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HHMM_RE)
    shift_end: str = Field(default="18:30", pattern=HHMM_RE)
    joined_on: str = Field(pattern=DATE_RE)

    @field_validator("joined_on")
    @classmethod
    def _joined_on_is_a_date(cls, value: str) -> str:
        return _check_date(value)

    # Equal shift times are valid under R1: shift_end <= shift_start is overnight,
    # so equal times represent a 24-hour shift. Punch-out still enforces the 24-hour maximum.


class PunchInRequest(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeRequest(BaseModel):
    status: Optional[StatusValue] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


# --------------------------------------------------------------------------------------------------------------------
# 5. Queries and aggregation pipelines (shared by the endpoints and by /admin/explain)
# --------------------------------------------------------------------------------------------------------------------


def attendance_find_spec(emp_code: Optional[str], date_from: Optional[str], date_to: Optional[str],
                         status: Optional[str], page: int, page_size: int) -> Dict[str, Any]:
    """Filter, sort, skip and limit of GET /attendance, in one place so that explain runs the very same query."""
    query: Dict[str, Any] = {}
    if emp_code is not None:
        query["emp_code"] = emp_code
    if status is not None:
        query["status"] = status
    if date_from is not None or date_to is not None:
        query["date"] = {}
        if date_from is not None:
            query["date"]["$gte"] = date_from
        if date_to is not None:
            query["date"]["$lte"] = date_to
    # Required order: date descending, then emp_code ascending. With a single employee selected the second key is
    # constant, so it is dropped; that lets the (emp_code, date) index deliver the order without an in-memory sort.
    sort = {"date": DESCENDING} if emp_code is not None else {"date": DESCENDING, "emp_code": ASCENDING}
    return {"filter": query, "sort": sort, "skip": (page - 1) * page_size, "limit": page_size}


def _present_weight() -> Dict[str, Any]:
    """1 for a presence record, 0.5 for a presence half day, 0 otherwise (R5, R6)."""
    return {"$cond": [{"$in": ["$status", PRESENCE]}, {"$cond": [{"$eq": ["$half_day", True]}, 0.5, 1]}, 0]}


def _is_weekday(date_field: str = "$date") -> Dict[str, Any]:
    """True for Monday-Friday. `date` is a YYYY-MM-DD string; an unparsable value yields null, i.e. not a weekday."""
    parsed = {"$dateFromString": {"dateString": date_field, "onError": None}}
    return {"$in": [{"$isoDayOfWeek": parsed}, [1, 2, 3, 4, 5]]}


def _count_if(condition: Dict[str, Any]) -> Dict[str, Any]:
    return {"$sum": {"$cond": [condition, 1, 0]}}


def _is_late() -> Dict[str, Any]:
    return {"$gt": ["$late_minutes", 0]}


def _half_up_expr(expr: Any, places: int) -> Dict[str, Any]:
    """Half-up rounding of a non-negative number inside the database ($round is round-half-even). The tiny nudge makes
    exact ties (0.03125 -> 0.0313) round up and cannot move any non-tie across a boundary: for the ratios and means
    involved the nearest non-tie is more than 1e-4 away."""
    scale = 10 ** places
    return {"$divide": [{"$floor": {"$add": [{"$multiply": [expr, scale]}, 0.500000001]}}, scale]}


def employee_monthly_pipeline(emp_code: str, month: str) -> List[Dict[str, Any]]:
    lo, hi = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": lo, "$lte": hi}}},
        {"$group": {
            "_id": None,
            # R7: only Monday-Friday records count towards present_days ...
            "present_days": {"$sum": {"$cond": [_is_weekday(), _present_weight(), 0]}},
            "leave_days": _count_if({"$eq": ["$status", "LEAVE"]}),
            # ... but weekend records still add to the late and overtime totals.
            "late_count": _count_if(_is_late()),
            "total_late_minutes": {"$sum": {"$cond": [_is_late(), "$late_minutes", 0]}},
            "total_overtime_minutes": {"$sum": "$overtime_minutes"},
        }},
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> List[Dict[str, Any]]:
    """Employees drive the pipeline, so people with no logs still count towards headcount (R9) and people who join
    after the month do not. Each employee's month is reduced to one small document inside $lookup; the second $group
    then folds employees into departments."""
    lo, hi = month_bounds(month)
    employee_filter: Dict[str, Any] = {"joined_on": {"$lte": month_last_day(month)}}
    if department is not None:
        employee_filter["department"] = department
    has_hours = {"$and": [{"$in": ["$status", PRESENCE]}, {"$isNumber": "$work_hours"}]}
    per_employee = [
        {"$match": {"date": {"$gte": lo, "$lte": hi}}},
        {"$group": {
            "_id": None,
            "present_days": {"$sum": {"$cond": [_is_weekday(), _present_weight(), 0]}},
            # avg_work_hours is a mean over RECORDS: carry the sum (in exact hundredths of an hour) and the count.
            "wh_hundredths": {"$sum": {"$cond": [has_hours, {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}, 0]}},
            "wh_count": _count_if(has_hours),
            "late_count": _count_if(_is_late()),
            "total_late_minutes": {"$sum": {"$cond": [_is_late(), "$late_minutes", 0]}},
            "leave_count": _count_if({"$eq": ["$status", "LEAVE"]}),
            "on_duty_count": _count_if({"$eq": ["$status", "ON_DUTY"]}),
        }},
    ]
    return [
        {"$match": employee_filter},
        {"$lookup": {"from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
                     "pipeline": per_employee, "as": "agg"}},
        {"$unwind": {"path": "$agg", "preserveNullAndEmptyArrays": True}},  # keep employees without any log
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": "$agg.present_days"},
            "wh_hundredths": {"$sum": "$agg.wh_hundredths"},
            "wh_count": {"$sum": "$agg.wh_count"},
            "late_count": {"$sum": "$agg.late_count"},
            "total_late_minutes": {"$sum": "$agg.total_late_minutes"},
            "leave_count": {"$sum": "$agg.leave_count"},
            "on_duty_count": {"$sum": "$agg.on_duty_count"},
        }},
        {"$sort": {"_id": ASCENDING}},
    ]


def late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> List[Dict[str, Any]]:
    lo, hi = month_bounds(month)
    pipeline: List[Dict[str, Any]] = [
        {"$match": {"date": {"$gte": lo, "$lte": hi}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs whose employee does not exist are dropped here
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        # Standard competition ranking: ties share a rank and the next rank is skipped (1, 2, 2, 4). The rank is
        # computed by the database over the whole ranked set; limit is applied AFTER it, on the rank.
        {"$setWindowFields": {"sortBy": {"total_late_minutes": DESCENDING}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total_late_minutes": DESCENDING, "_id": ASCENDING}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name", "department": "$emp.department",
                      "total_late_minutes": 1, "late_count": 1}},
    ]
    return pipeline


def trend_inputs(department: str, date_from: str, date_to: str) -> Tuple[List[str], int, List[str]]:
    """Validate the range, then read what the pipeline needs about the department's employees: their codes, how many
    had joined by `from` (R9), and the join dates that fall inside the range."""
    first, last = Date.fromisoformat(date_from), Date.fromisoformat(date_to)
    if last < first:
        unprocessable("'to' must not be before 'from'", ("query", "to"))
    if (last - first).days + 1 > MAX_TREND_DAYS:
        unprocessable(f"the range may not exceed {MAX_TREND_DAYS} days", ("query", "to"))
    employees = list(db.employees.find({"department": department}, {"_id": 0, "emp_code": 1, "joined_on": 1}))
    if not employees:
        raise HTTPException(status_code=404, detail="Department not found")
    codes = [e["emp_code"] for e in employees]
    joined = [e["joined_on"] for e in employees if isinstance(e.get("joined_on"), str)]
    base = sum(1 for j in joined if j <= date_from)
    joins = sorted(j for j in joined if date_from < j <= date_to)
    return codes, base, joins


def department_trend_pipeline(codes: List[str], base_headcount: int, joins: List[str], date_from: str,
                              date_to: str) -> List[Dict[str, Any]]:
    """One output row per calendar day, built inside the database.

    The $match is the only stage that reads attendance_logs (it uses an index). $facet then turns "the matching
    records" into exactly one document even when nothing matched, and $range generates the calendar from that single
    document, so weekends and days without any record still get a row. $setWindowFields adds the 7-row moving average.
    """
    first = Date.fromisoformat(date_from)
    n_days = (Date.fromisoformat(date_to) - first).days + 1
    start = datetime.combine(first, time.min, tzinfo=UTC)
    present_per_day = {"$sum": {"$cond": [{"$in": ["$status", PRESENCE]},
                                          {"$cond": [{"$eq": ["$half_day", True]}, 0.5, 1]}, 0]}}
    row = {
        "$let": {"vars": {"dt": {"$add": [start, {"$multiply": ["$$i", MS_PER_DAY]}]}}, "in": {
            "$let": {"vars": {"ds": {"$dateToString": {"format": "%Y-%m-%d", "date": "$$dt"}}}, "in": {
                "$let": {"vars": {"stat": {"$arrayElemAt": [
                    {"$filter": {"input": "$days", "as": "x", "cond": {"$eq": ["$$x._id", "$$ds"]}}}, 0]}}, "in": {
                    "date": "$$ds",
                    "is_working_day": {"$in": [{"$isoDayOfWeek": "$$dt"}, [1, 2, 3, 4, 5]]},
                    "headcount": {"$add": [base_headcount, {"$size": {"$filter": {
                        "input": {"$literal": joins}, "as": "j", "cond": {"$lte": ["$$j", "$$ds"]}}}}]},
                    "present_count": {"$ifNull": ["$$stat.present_count", 0]},
                    "late_count": {"$ifNull": ["$$stat.late_count", 0]},
                }}}}}}}
    return [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": date_from, "$lte": date_to}}},
        {"$facet": {"days": [{"$group": {"_id": "$date", "present_count": present_per_day,
                                         "late_count": _count_if(_is_late())}}]}},
        {"$project": {"rows": {"$map": {"input": {"$range": [0, n_days]}, "as": "i", "in": row}}}},
        {"$unwind": "$rows"},
        {"$replaceRoot": {"newRoot": "$rows"}},
        # Rate only on working days with someone on the books; a working day without records is 0.0, not null.
        {"$set": {"attendance_rate": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            _half_up_expr({"$divide": ["$present_count", "$headcount"]}, 4), None]}}},
        # Current row plus up to 6 preceding rows of the requested range; $avg skips null rates and is null when
        # the window holds none.
        {"$setWindowFields": {"sortBy": {"date": ASCENDING},
                              "output": {"ma": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}}}},
        {"$set": {"moving_avg_7d": {"$cond": [{"$isNumber": "$ma"}, _half_up_expr("$ma", 4), None]}}},
        {"$project": {"_id": 0, "date": 1, "is_working_day": 1, "headcount": 1, "present_count": 1, "late_count": 1,
                      "attendance_rate": 1, "moving_avg_7d": 1}},
        {"$sort": {"date": ASCENDING}},
    ]


# --------------------------------------------------------------------------------------------------------------------
# 6. Endpoints: system and employees
# --------------------------------------------------------------------------------------------------------------------


@app.get("/health", tags=["system"])
def health():
    try:
        db.command("ping")
    except PyMongoError:
        raise HTTPException(status_code=503, detail="MongoDB is not reachable")
    if not _indexes_ready:  # start-up could not reach MongoDB: finish the job now
        ensure_indexes()
    return {"status": "ok"}


@app.post("/employees", status_code=201, tags=["employees"])
def create_employee(payload: EmployeeCreate):
    doc = payload.model_dump()
    doc["created_at"] = now_ms_precision()
    try:
        # No "does it exist?" pre-check: it would race. The unique index on emp_code decides.
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="emp_code already exists")
    return serialize_employee(doc)


@app.get("/employees", tags=["employees"])
def list_employees(department: Optional[str] = None, page: PageParam = 1, page_size: PageSizeParam = 20):
    query: Dict[str, Any] = {} if department is None else {"department": department}
    total = db.employees.count_documents(query)
    skip = (page - 1) * page_size
    items: List[Dict[str, Any]] = []
    if skip < total:
        cursor = db.employees.find(query).sort("emp_code", ASCENDING).skip(skip).limit(page_size)
        items = [serialize_employee(d) for d in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------------------------------------------------
# 6. Endpoints: attendance
# --------------------------------------------------------------------------------------------------------------------


@app.post("/attendance/punch-in", status_code=201, tags=["attendance"])
def punch_in(payload: PunchInRequest):
    emp = db.employees.find_one({"emp_code": payload.emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="Employee not found")
    punched_at = ms_to_datetime(payload.punched_at) if payload.punched_at is not None else datetime.now(UTC)
    punched_at = truncate_seconds(punched_at)
    att_date = determine_attendance_date(punched_at, emp["shift_start"], emp["shift_end"])
    record = {
        "emp_code": payload.emp_code,
        "date": att_date,
        "status": payload.status,
        "punch_in": punched_at,
        "punch_out": None,
        **derive_fields(emp, att_date, payload.status, punched_at, None),
        "history": [],
    }
    try:
        # Two simultaneous punch-ins: the unique (emp_code, date) index lets exactly one insert succeed.
        db.attendance_logs.insert_one(record)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Already punched in for this attendance date")
    return serialize_attendance(record)


@app.post("/attendance/punch-out", tags=["attendance"])
def punch_out(payload: PunchOutRequest):
    emp = db.employees.find_one({"emp_code": payload.emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="Employee not found")
    punched_at = truncate_seconds(
        ms_to_datetime(payload.punched_at) if payload.punched_at is not None else datetime.now(UTC))

    # "The employee's most recent record whose punch_in is at or before punched_at." Deliberately NOT filtered on
    # punch_out being empty: if that record is already closed the answer is 409, not "close an older open one".
    record = db.attendance_logs.find_one({"emp_code": payload.emp_code, "punch_in": {"$lte": punched_at}},
                                         sort=[("punch_in", DESCENDING)])
    if record is None:
        raise HTTPException(status_code=404, detail="No punch-in found for this employee")
    if record.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="That record is already punched out")
    punch_in_at = as_utc(record["punch_in"])
    if punched_at <= truncate_seconds(punch_in_at):
        unprocessable("punched_at must be after the punch-in", ("body", "punched_at"))
    if punched_at - truncate_seconds(punch_in_at) > MAX_SHIFT_SPAN:
        unprocessable("punched_at is more than 24 hours after the punch-in", ("body", "punched_at"))

    derived = derive_fields(emp, record["date"], record["status"], punch_in_at, punched_at)
    # The filter repeats what we read (punch_out still empty, punch_in unchanged), so of two simultaneous punch-outs
    # exactly one matches; the other gets None back and 409.
    updated = db.attendance_logs.find_one_and_update(
        {"emp_code": record["emp_code"], "date": record["date"], "punch_out": None, "punch_in": record["punch_in"]},
        {"$set": {"punch_out": punched_at, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="That record is already punched out")
    return serialize_attendance(updated)


@app.get("/attendance", tags=["attendance"])
def list_attendance(emp_code: Optional[str] = None, date_from: DateQuery = None, date_to: DateQuery = None,
                    status: Optional[StatusValue] = None, page: PageParam = 1, page_size: PageSizeParam = 20):
    if date_from is not None and date_to is not None and date_from > date_to:
        unprocessable("date_from must not be after date_to", ("query", "date_from"))
    spec = attendance_find_spec(emp_code, date_from, date_to, status, page, page_size)
    total = db.attendance_logs.count_documents(spec["filter"])
    items: List[Dict[str, Any]] = []
    if spec["skip"] < total:  # also keeps an absurdly large page number away from the server
        cursor = db.attendance_logs.find(spec["filter"], sort=list(spec["sort"].items()), skip=spec["skip"],
                                         limit=spec["limit"])
        items = [serialize_attendance(d) for d in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}", tags=["attendance"])
def regularize_attendance(emp_code: str, date: DatePath, payload: RegularizeRequest):
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="Employee not found")
    record = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if record is None:
        raise HTTPException(status_code=404, detail="No attendance record for that date")

    old_status = record["status"]
    old_in, old_out = as_utc(record.get("punch_in")), as_utc(record.get("punch_out"))
    new_status = payload.status if payload.status is not None else old_status

    if new_status in ("ABSENT", "LEAVE"):
        if payload.punch_in is not None or payload.punch_out is not None:
            unprocessable("ABSENT and LEAVE clear the punch times; do not send punch_in or punch_out with them")
        new_in = new_out = None
    else:
        new_in = truncate_seconds(ms_to_datetime(payload.punch_in)) if payload.punch_in is not None else old_in
        if new_in is None:
            unprocessable("a presence status requires a punch_in", ("body", "punch_in"))
        if determine_attendance_date(new_in, emp["shift_start"], emp["shift_end"]) != date:
            unprocessable("punch_in must stay on the record's attendance date", ("body", "punch_in"))
        new_out = truncate_seconds(ms_to_datetime(payload.punch_out)) if payload.punch_out is not None else old_out
        if new_out is not None:
            if new_out <= truncate_seconds(new_in):
                unprocessable("punch_out must be after punch_in", ("body", "punch_out"))
            if new_out - truncate_seconds(new_in) > MAX_SHIFT_SPAN:
                unprocessable("punch_out must be within 24 hours of punch_in", ("body", "punch_out"))

    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
           **derive_fields(emp, date, new_status, new_in, new_out)}
    old = {"status": old_status, "punch_in": old_in, "punch_out": old_out, "work_hours": record.get("work_hours"),
           "late_minutes": record.get("late_minutes") or 0, "overtime_minutes": record.get("overtime_minutes") or 0,
           "half_day": bool(record.get("half_day", False))}
    changes = {f: {"from": old[f], "to": new[f]} for f in old if old[f] != new[f]}
    if not changes:
        unprocessable("this request changes nothing")

    history = record.get("history")
    # Optimistic concurrency: the update only applies if the record is still exactly as we read it (same history
    # length, same status and punch times). Otherwise a concurrent writer got there first: 409 instead of silently
    # overwriting its change or recording a history entry whose "from" values are stale.
    unchanged_since_read = {
        "emp_code": emp_code, "date": date, "status": old_status,
        "punch_in": record.get("punch_in"), "punch_out": record.get("punch_out"),
        "history": {"$size": len(history)} if isinstance(history, list) else None,
    }
    entry = {"at": now_ms_precision(), "by": payload.regularized_by, "reason": payload.reason, "changes": changes}
    updated = db.attendance_logs.find_one_and_update(
        unchanged_since_read,
        {"$set": {**new}, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="The record was modified concurrently; reload it and retry")
    return serialize_attendance(updated)


# --------------------------------------------------------------------------------------------------------------------
# 6. Endpoints: analytics
# --------------------------------------------------------------------------------------------------------------------


@app.get("/analytics/employees/{emp_code}/monthly", tags=["analytics"])
def employee_monthly(emp_code: str, month: MonthRequired):
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="Employee not found")
    rows = list(db.attendance_logs.aggregate(employee_monthly_pipeline(emp_code, month)))
    data = rows[0] if rows else {}
    present_days = float(data.get("present_days") or 0)
    working_days = count_working_days(month, emp.get("joined_on"))
    attendance_pct = None
    if working_days > 0:
        attendance_pct = round_half_up(Decimal(str(present_days)) / Decimal(working_days) * 100, 2)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": round_half_up(present_days, 2),
        "leave_days": int(data.get("leave_days") or 0),
        "late_count": int(data.get("late_count") or 0),
        "total_late_minutes": int(data.get("total_late_minutes") or 0),
        "total_overtime_minutes": int(data.get("total_overtime_minutes") or 0),
        "attendance_pct": attendance_pct,
    }


@app.get("/analytics/departments/summary", tags=["analytics"])
def department_summary(month: MonthRequired, department: Optional[str] = None):
    items = []
    for row in db.employees.aggregate(department_summary_pipeline(month, department)):
        hours_count = int(row["wh_count"])
        average = None
        if hours_count > 0:  # mean over records, exact: sum of hundredths / records / 100, rounded half-up once
            mean = Decimal(int(round(row["wh_hundredths"]))) / Decimal(hours_count) / Decimal(100)
            average = float(mean.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        items.append({
            "department": row["_id"],
            "headcount": int(row["headcount"]),
            "present_days": round_half_up(row["present_days"] or 0, 2),
            "avg_work_hours": average,
            "late_count": int(row["late_count"]),
            "total_late_minutes": int(row["total_late_minutes"]),
            "leave_count": int(row["leave_count"]),
            "on_duty_count": int(row["on_duty_count"]),
        })
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late", tags=["analytics"])
def late_leaderboard(month: MonthRequired, limit: Annotated[int, Query(ge=1, le=50)] = 10,
                     department: Optional[str] = None):
    items = [{"rank": int(r["rank"]), "emp_code": r["emp_code"], "name": r["name"], "department": r["department"],
              "total_late_minutes": int(r["total_late_minutes"]), "late_count": int(r["late_count"])}
             for r in db.attendance_logs.aggregate(late_leaderboard_pipeline(month, limit, department))]
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend", tags=["analytics"])
def department_trend(department: str, from_: Annotated[str, Query(alias="from", pattern=DATE_RE),
                                                       AfterValidator(_check_date)],
                     to: DateRequired):
    codes, base, joins = trend_inputs(department, from_, to)
    rows = db.attendance_logs.aggregate(department_trend_pipeline(codes, base, joins, from_, to))
    items = [{"date": r["date"], "is_working_day": bool(r["is_working_day"]), "headcount": int(r["headcount"]),
              "present_count": float(r["present_count"]), "late_count": int(r["late_count"]),
              "attendance_rate": r["attendance_rate"], "moving_avg_7d": r["moving_avg_7d"]} for r in rows]
    return {"department": department, "items": items}


# --------------------------------------------------------------------------------------------------------------------
# 6. Endpoints: admin / explain
# --------------------------------------------------------------------------------------------------------------------

ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard",
                        "department_trend"]


def _explain(command: Dict[str, Any]) -> Dict[str, Any]:
    """Run `explain` with executionStats verbosity and make the raw result JSON-safe (it can hold Timestamps etc.)."""
    result = db.command({"explain": command, "verbosity": "executionStats"})
    return json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))


def _require(**values: Any) -> None:
    missing = [name for name, value in values.items() if value is None]
    if missing:
        unprocessable("missing required query parameter(s): " + ", ".join(missing), ("query", missing[0]))


@app.get("/admin/explain/{endpoint}", tags=["admin"])
def explain_endpoint(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: MonthOptional = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: DateQuery = None,
    date_to: DateQuery = None,
    status: Optional[StatusValue] = None,
    from_: Annotated[Optional[str], Query(alias="from", pattern=DATE_RE), AfterValidator(_check_date)] = None,
    to: DateQuery = None,
    page: PageParam = 1,
    page_size: PageSizeParam = 20,
):
    """Explain the main query of an endpoint, built by the same functions the endpoint itself calls."""
    if endpoint == "attendance_list":
        if date_from is not None and date_to is not None and date_from > date_to:
            unprocessable("date_from must not be after date_to", ("query", "date_from"))
        spec = attendance_find_spec(emp_code, date_from, date_to, status, page, page_size)
        explain = _explain({"find": "attendance_logs", "filter": spec["filter"], "sort": spec["sort"],
                            "skip": spec["skip"], "limit": spec["limit"]})
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": explain}

    if endpoint == "employee_monthly":
        _require(emp_code=emp_code, month=month)
        pipeline, collection = employee_monthly_pipeline(emp_code, month), "attendance_logs"
    elif endpoint == "department_summary":
        _require(month=month)
        pipeline, collection = department_summary_pipeline(month, department), "employees"
    elif endpoint == "late_leaderboard":
        _require(month=month)
        pipeline, collection = late_leaderboard_pipeline(month, limit, department), "attendance_logs"
    else:  # department_trend
        _require(department=department, **{"from": from_}, to=to)
        codes, base, joins = trend_inputs(department, from_, to)
        pipeline, collection = department_trend_pipeline(codes, base, joins, from_, to), "attendance_logs"

    explain = _explain({"aggregate": collection, "pipeline": pipeline, "cursor": {}})
    return {"endpoint": endpoint, "collection": collection, "explain": explain}
