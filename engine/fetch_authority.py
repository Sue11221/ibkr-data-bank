"""Offline, immutable request horizons from the reviewed session schedule.

This module authorizes no transport by itself. A1 integration must bind its
decisions to the actual send sites and the two-sided durable request ledger.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Mapping
import warnings
from zoneinfo import ZoneInfo


AUTHORITY_VERSION = "fetch-authority-v1"
TABLE_PATH = Path(__file__).resolve().with_name("session_schedule_table.json")
NY = ZoneInfo("America/New_York")
UTC = timezone.utc
DAILY_MARGIN = timedelta(minutes=20)
INTRADAY_MARGIN = timedelta(seconds=60)
REGULAR_CLOSE = time(16)
BAR_SIZES = MappingProxyType({
    "1s": ("1 secs", 1), "5s": ("5 secs", 5), "10s": ("10 secs", 10),
    "30s": ("30 secs", 30), "1m": ("1 min", 60), "2m": ("2 mins", 120),
    "5m": ("5 mins", 300), "15m": ("15 mins", 900),
    "30m": ("30 mins", 1800), "1h": ("1 hour", 3600), "1d": ("1 day", 86400),
})
KINDS = MappingProxyType({
    "": "TRADES", "iv": "OPTION_IMPLIED_VOLATILITY",
    "hvol": "HISTORICAL_VOLATILITY", "bidask": "BID_ASK",
})
SESSION_WINDOWS = MappingProxyType({
    "rth": (True, time(9, 30), time(16)),
    "pre": (False, time(4), time(9, 30)),
    "post": (False, time(16), time(20)),
})
TOKENS = tuple(base + (f"-{kind}" if kind else "")
               + (f"-{session}" if session != "rth" else "")
               for base in BAR_SIZES for kind in KINDS for session in SESSION_WINDOWS)
_TOKEN_RE = re.compile(
    r"(1s|5s|10s|30s|1m|2m|5m|15m|30m|1h|1d)(?:-(iv|hvol|bidask))?(?:-(pre|post))?\Z")
_DIGEST_RE = re.compile(r"[0-9a-f]{64}\Z")


class AuthorityError(ValueError):
    """The calendar or requested time cannot be safely authorized."""


class ResponseQuarantined(ValueError):
    """A guarded provider response was rejected and must not be retried."""


class CalendarUnsupported(AuthorityError):
    """The requested date/session is outside proven calendar coverage."""


def canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("utf-8")


def digest_value(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise AuthorityError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def strict_json(raw: bytes | str):
    try:
        return json.loads(raw, object_pairs_hook=_unique_object,
                          parse_constant=lambda value: (_ for _ in ()).throw(
                              AuthorityError(f"non-finite JSON value: {value}")))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise AuthorityError(f"invalid JSON: {exc}") from exc


def aware_ny(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise AuthorityError("an aware datetime is required")
    # Round-trip detects a nonexistent NY wall time rather than normalizing it
    # silently across a DST gap. UTC/other aware inputs are explicitly converted.
    result = value.astimezone(UTC).astimezone(NY)
    if getattr(value.tzinfo, "key", None) == "America/New_York":
        if value.replace(tzinfo=None) != result.replace(tzinfo=None):
            raise AuthorityError("nonexistent New York wall time")
    return result


def parse_day(value: str) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise AuthorityError("a canonical ISO date is required")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise AuthorityError("invalid calendar date") from exc


def parse_token(token: str) -> tuple[str, str, str]:
    match = _TOKEN_RE.fullmatch(token) if isinstance(token, str) else None
    if match is None:
        raise AuthorityError(f"noncanonical interval token: {token!r}")
    return match[1], match[2] or "", match[3] or "rth"


def _parse_time(value: str) -> time:
    if not isinstance(value, str) or not re.fullmatch(r"\d{2}:\d{2}", value):
        raise AuthorityError("canonical HH:MM session time required")
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise AuthorityError("invalid session time") from exc


@dataclass(frozen=True)
class SessionRow:
    day: date
    status: str
    open: time | None
    close: time | None

    @property
    def is_open(self) -> bool:
        return self.status in {"open_regular", "open_early_close"}


@dataclass(frozen=True)
class ScheduleAuthority:
    first_date: date
    valid_through: date
    table_digest: str
    consensus_digest: str
    fingerprint: str
    rows: Mapping[date, SessionRow]

    @classmethod
    def load(cls, path: Path = TABLE_PATH) -> "ScheduleAuthority":
        return cls.from_table(strict_json(Path(path).read_bytes()))

    @classmethod
    def from_table(cls, table: dict) -> "ScheduleAuthority":
        if not isinstance(table, dict) or set(table) != {"header", "rows", "schema_version"}:
            raise AuthorityError("schedule root schema mismatch")
        if type(table["schema_version"]) is not int or table["schema_version"] != 1:
            raise AuthorityError("unsupported schedule version")
        header = table["header"]
        if not isinstance(header, dict):
            raise AuthorityError("schedule header missing")
        required_header = {
            "authority", "coverage", "family", "generated_as_of", "generator_source_commit",
            "provenance", "row_count", "settlement", "status_counts", "timezone", "update_rule",
        }
        if set(header) != required_header:
            raise AuthorityError("schedule header schema mismatch")
        if (header["family"] != "xnys_regular" or header["timezone"] != "America/New_York"
                or not isinstance(header["authority"], str) or not header["authority"]
                or not isinstance(header["update_rule"], str) or not header["update_rule"]):
            raise AuthorityError("unsupported schedule authority/family/timezone/update rule")
        coverage = header["coverage"]
        if not isinstance(coverage, dict) or set(coverage) != {
                "first_date", "valid_through", "required_generation_runway_days"}:
            raise AuthorityError("schedule coverage schema mismatch")
        first, last = parse_day(coverage["first_date"]), parse_day(coverage["valid_through"])
        if first != date(1980, 1, 2) or last < first:
            raise AuthorityError("schedule differs from proven historical rule floor")
        generated = parse_day(header["generated_as_of"])
        if (type(coverage["required_generation_runway_days"]) is not int
                or coverage["required_generation_runway_days"] != 180
                or (last - generated).days < 180):
            raise AuthorityError("schedule generation runway below policy")
        if not isinstance(header["generator_source_commit"], str) or not re.fullmatch(
                r"[0-9a-f]{40}", header["generator_source_commit"]):
            raise AuthorityError("generator-source commit missing")
        provenance = header["provenance"]
        if not isinstance(provenance, dict) or set(provenance) != {
                "appendix_j_input_sha256", "consensus_dataset_sha256", "evidence_snapshot_sha256",
                "rule_set_sha256", "special_closure_list_sha256",
                "special_closure_sidecar_snapshot_sha256"}:
            raise AuthorityError("schedule provenance schema mismatch")
        if any(not isinstance(value, str) or not _DIGEST_RE.fullmatch(value)
               for value in provenance.values()):
            raise AuthorityError("invalid provenance digest")
        settlement = header["settlement"]
        if settlement != {
                "daily_settle_floor_close_et": "16:00", "daily_settle_margin_minutes": 20,
                "regular_close_et": "16:00"} or type(settlement["daily_settle_margin_minutes"]) is not int:
            raise AuthorityError("daily settlement policy differs from reviewed floor")
        raw_rows = table["rows"]
        if not isinstance(raw_rows, list):
            raise AuthorityError("schedule rows must be a list")
        rows: dict[date, SessionRow] = {}
        counts: Counter[str] = Counter()
        for raw in raw_rows:
            if not isinstance(raw, dict) or set(raw) != {
                    "date", "family", "status", "open_et", "close_et"}:
                raise AuthorityError("schedule row schema mismatch")
            day = parse_day(raw["date"])
            if day in rows:
                raise AuthorityError("duplicate raw (family,date) key")
            if raw["family"] != "xnys_regular":
                raise AuthorityError("unknown row family")
            status = raw["status"]
            if status not in {"open_regular", "open_early_close", "closed_holiday", "closed_weekend"}:
                raise AuthorityError("unknown row status")
            if status.startswith("open_"):
                opening, closing = _parse_time(raw["open_et"]), _parse_time(raw["close_et"])
                if opening >= closing:
                    raise AuthorityError("session open must precede close")
            else:
                opening = closing = None
                if raw["open_et"] is not None or raw["close_et"] is not None:
                    raise AuthorityError("closed row carries session times")
            rows[day] = SessionRow(day, status, opening, closing)
            counts[status] += 1
        expected = {first + timedelta(days=index) for index in range((last - first).days + 1)}
        if set(rows) != expected:
            raise AuthorityError("schedule calendar-date set is incomplete or out of coverage")
        if (type(header["row_count"]) is not int or header["row_count"] != len(rows)
                or header["status_counts"] != dict(counts)):
            raise AuthorityError("schedule row/status counts disagree")
        table_digest = digest_value(table)
        consensus = provenance["consensus_dataset_sha256"]
        policy = {
            "version": AUTHORITY_VERSION, "table": table_digest, "consensus": consensus,
            "bar_sizes": dict(BAR_SIZES), "kinds": dict(KINDS),
            "windows": {key: [use, start.isoformat(), end.isoformat()]
                        for key, (use, start, end) in SESSION_WINDOWS.items()},
            "daily_floor": REGULAR_CLOSE.isoformat(),
            "daily_margin_seconds": DAILY_MARGIN.total_seconds(),
            "intraday_margin_seconds": INTRADAY_MARGIN.total_seconds(),
        }
        return cls(first, last, table_digest, consensus, digest_value(policy),
                   MappingProxyType(rows))

    def check_clock(self, now: datetime) -> datetime:
        now = aware_ny(now)
        if not self.first_date <= now.date() <= self.valid_through:
            raise CalendarUnsupported("run clock outside schedule coverage")
        runway = (self.valid_through - now.date()).days
        if runway < 14:
            raise CalendarUnsupported("schedule runway below 14 days; reviewed extension required")
        if runway < 60:
            warnings.warn(f"schedule runway only {runway} days", RuntimeWarning, stacklevel=2)
        return now

    def row(self, day: date) -> SessionRow:
        if type(day) is not date or day not in self.rows:
            raise CalendarUnsupported("date outside schedule coverage")
        return self.rows[day]

    def window(self, token: str, day: date) -> tuple[datetime, datetime] | None:
        base, _, session = parse_token(token)
        row = self.row(day)
        if not row.is_open:
            return None
        if session != "rth" and (base == "1d" or row.status == "open_early_close"):
            raise CalendarUnsupported("extended daily/early-close window has no proven provenance")
        opening, closing = (row.open, row.close) if session == "rth" else SESSION_WINDOWS[session][1:]
        return datetime.combine(day, opening, NY), datetime.combine(day, closing, NY)

    def daily_settled_close(self, day: date) -> datetime:
        row = self.row(day)
        if not row.is_open:
            raise CalendarUnsupported("closed date has no daily settlement")
        return datetime.combine(day, max(row.close, REGULAR_CLOSE), NY) + DAILY_MARGIN

    def horizon(self, token: str, now: datetime) -> datetime | None:
        now = aware_ny(now)
        base, _, session = parse_token(token)
        if base == "1d" and session != "rth":
            return None
        day = now.date()
        self.row(day)  # Never guess a current date outside coverage.
        while day >= self.first_date:
            try:
                window = self.window(token, day)
            except CalendarUnsupported:
                window = None
            if window is not None:
                opening, closing = window
                if base == "1d":
                    if now >= self.daily_settled_close(day):
                        return closing
                else:
                    settled_until = now - INTRADAY_MARGIN
                    if settled_until >= closing:
                        return closing
                    seconds = BAR_SIZES[base][1]
                    periods = int((settled_until - opening).total_seconds() // seconds)
                    if periods > 0:
                        return opening + timedelta(seconds=periods * seconds)
            day -= timedelta(days=1)
        return None

    def accepts_label(self, token: str, stamp: datetime, now: datetime) -> bool:
        stamp, now = aware_ny(stamp), aware_ny(now)
        base, _, _ = parse_token(token)
        window = self.window(token, stamp.date())
        if window is None:
            return False
        if base == "1d":
            return stamp.time() == time() and now >= self.daily_settled_close(stamp.date())
        opening, closing = window
        seconds = BAR_SIZES[base][1]
        if not opening <= stamp < closing or stamp.microsecond:
            return False
        if int((stamp - opening).total_seconds()) % seconds:
            return False
        period_end = min(stamp + timedelta(seconds=seconds), closing)
        return now >= period_end + INTRADAY_MARGIN
