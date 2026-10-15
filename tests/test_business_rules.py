"""Focused unit tests for attendance business-rule helpers.

Run after installing requirements with: python -m pytest -q
"""
from datetime import datetime, timezone

from app.main import (
    compute_half_day,
    compute_late_minutes,
    compute_overtime_minutes,
    compute_work_hours,
    determine_attendance_date,
)


def utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_overnight_punch_before_shift_end_belongs_to_previous_day():
    # 02:00 IST on Oct 15 is 20:30 UTC on Oct 14.
    assert determine_attendance_date(utc("2026-10-14T20:30:00Z"), "22:00", "06:00") == "2026-10-14"


def test_day_shift_attendance_date_uses_ist_calendar_date():
    assert determine_attendance_date(utc("2026-10-14T04:00:00Z"), "09:30", "18:30") == "2026-10-14"


def test_late_grace_boundary_is_strict():
    assert compute_late_minutes(utc("2026-10-14T04:10:00Z"), "2026-10-14", "09:30") == 0
    assert compute_late_minutes(utc("2026-10-14T04:10:01Z"), "2026-10-14", "09:30") == 10


def test_late_minutes_are_measured_from_shift_start():
    assert compute_late_minutes(utc("2026-10-14T04:45:59Z"), "2026-10-14", "09:30") == 45


def test_overtime_requires_at_least_30_whole_minutes():
    # Shift ends at 18:30 IST = 13:00 UTC.
    assert compute_overtime_minutes(utc("2026-10-14T13:29:59Z"), "2026-10-14", "09:30", "18:30") == 0
    assert compute_overtime_minutes(utc("2026-10-14T13:30:00Z"), "2026-10-14", "09:30", "18:30") == 30


def test_overnight_overtime_uses_next_calendar_day_end():
    # 06:30 IST on Oct 15 = 01:00 UTC on Oct 15.
    assert compute_overtime_minutes(utc("2026-10-15T01:00:00Z"), "2026-10-14", "22:00", "06:00") == 30


def test_work_hours_round_half_up_to_two_decimals():
    start = utc("2026-10-14T00:00:00Z")
    end = utc("2026-10-14T04:30:18Z")  # 4.505 hours
    assert compute_work_hours(start, end) == 4.51


def test_half_day_uses_rounded_work_hours():
    assert compute_half_day(4.49) is True
    assert compute_half_day(4.50) is False
