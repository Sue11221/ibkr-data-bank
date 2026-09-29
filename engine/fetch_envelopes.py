"""Strict IB/HTTP request variants and window-preserving horizon decisions."""

from __future__ import annotations

import calendar
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import re
from typing import Mapping

from fetch_authority import (AuthorityError, BAR_SIZES, CalendarUnsupported, KINDS,
                             SESSION_WINDOWS, ScheduleAuthority, UTC, aware_ny, parse_token)
from fetch_governors import endpoint_policy


class EnvelopeError(AuthorityError):
    pass


def instant(value: object) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as exc:
            raise EnvelopeError("invalid ISO request time") from exc
    result = aware_ny(value)
    if result.microsecond:
        raise EnvelopeError("request bounds must be whole seconds")
    return result


def carrier_start(end: datetime, duration: str) -> datetime:
    """Decode the explicit calendar-unit carrier; S is elapsed UTC seconds."""
    end = instant(end)
    match = re.fullmatch(r"([1-9][0-9]{0,9}) ([SDWMY])", duration) if isinstance(duration, str) else None
    if match is None:
        raise EnvelopeError("unsupported duration; expand Max to explicit covered bounds")
    count, unit = int(match[1]), match[2]
    try:
        if unit == "S":
            return (end.astimezone(UTC) - timedelta(seconds=count)).astimezone(end.tzinfo)
        if unit in {"D", "W"}:
            return instant(end - timedelta(days=count * (7 if unit == "W" else 1)))
        months = end.year * 12 + end.month - 1 - count * (12 if unit == "Y" else 1)
        year, month = divmod(months, 12)
        return instant(end.replace(year=year, month=month + 1,
                                   day=min(end.day, calendar.monthrange(year, month + 1)[1])))
    except (OverflowError, ValueError) as exc:
        raise EnvelopeError("duration outside representable datetime bounds") from exc


def encode_carrier(start: datetime, end: datetime) -> str:
    """Encode a containing carrier; intended bounds filter extra leading rows.

    Seconds are limited to 28,800; days to 365. Longer spans use calendar years,
    rounded outward, not oversized seconds/days. This does not promise provider
    retention or availability for a large historical request.
    """
    start, end = instant(start), instant(end)
    seconds = int((end.astimezone(UTC) - start.astimezone(UTC)).total_seconds())
    if seconds <= 0:
        raise EnvelopeError("clamped duration is not representable")
    if seconds <= 28_800:
        return f"{seconds} S"
    days = max(1, (end.date() - start.date()).days)
    if carrier_start(end, f"{days} D") > start:
        days += 1
    if days > 365:
        years = max(1, end.year - start.year)
        if carrier_start(end, f"{years} Y") > start:
            years += 1
        duration = f"{years} Y"
        if carrier_start(end, duration) > start:
            raise EnvelopeError("year carrier does not contain intended start")
        return duration
    duration = f"{days} D"
    if carrier_start(end, duration) > start:
        raise EnvelopeError("clamped carrier does not contain intended start")
    return duration


@dataclass(frozen=True)
class Envelope:
    """Validated, detached snapshot; instantiate using parse_envelope."""

    variant: str
    symbol: str
    con_id: int
    token: str | None = None
    what_to_show: str | None = None
    use_rth: bool | None = None
    bar_size: str | None = None
    raw_end: datetime | None = None
    duration: str | None = None
    intended_start: datetime | None = None
    intended_end: datetime | None = None
    method: str | None = None
    endpoint: str | None = None
    subject: str | None = None

    def wire(self) -> dict:
        if self.variant in {"http-series", "http-metadata"}:
            result = {"variant": self.variant, "endpoint": self.endpoint,
                      "subject": self.subject}
            if self.variant == "http-series":
                result.update(token=self.token, intended_start=self.intended_start.isoformat(),
                              intended_end=self.intended_end.isoformat())
            return result
        result = {"variant": self.variant, "symbol": self.symbol, "con_id": self.con_id}
        if self.variant in {"ibkr-bars", "ibkr-bars-unfiltered", "ibkr-head"}:
            result.update(what_to_show=self.what_to_show, use_rth=self.use_rth)
        if self.variant in {"ibkr-bars", "ibkr-bars-unfiltered"}:
            result.update(token=self.token, bar_size=self.bar_size,
                          raw_end=self.raw_end.isoformat(), duration=self.duration,
                          intended_start=self.intended_start.isoformat(),
                          intended_end=self.intended_end.isoformat())
        if self.variant == "ibkr-metadata":
            result["method"] = self.method
        return result


def parse_envelope(raw: Mapping) -> Envelope:
    if not isinstance(raw, Mapping):
        raise EnvelopeError("envelope must be an object")
    if isinstance(raw.get("variant"), str) and raw.get("variant") in {"http-series", "http-metadata"}:
        return _parse_http(raw)
    common = {"variant", "symbol", "con_id"}
    fields = {
        "ibkr-bars": common | {"token", "what_to_show", "use_rth", "bar_size", "raw_end",
                               "duration", "intended_start", "intended_end"},
        "ibkr-bars-unfiltered": common | {"token", "what_to_show", "use_rth", "bar_size",
                                          "raw_end", "duration", "intended_start", "intended_end"},
        "ibkr-head": common | {"what_to_show", "use_rth"},
        "ibkr-metadata": common | {"method"},
    }
    variant = raw.get("variant")
    if not isinstance(variant, str) or variant not in fields or set(raw) != fields[variant]:
        raise EnvelopeError("envelope variant/shape mismatch (no null padding)")
    symbol, con_id = raw["symbol"], raw["con_id"]
    if not isinstance(symbol, str) or not symbol.strip() or symbol != symbol.strip():
        raise EnvelopeError("symbol identity missing or noncanonical")
    if type(con_id) is not int or con_id < (0 if variant == "ibkr-metadata" else 1):
        raise EnvelopeError("contract identity missing")
    if variant == "ibkr-metadata":
        if not isinstance(raw["method"], str) or raw["method"] not in {"qualification", "company_name", "symbol_search"}:
            raise EnvelopeError("unknown IB metadata method")
        if raw["method"] == "company_name" and con_id < 1:
            raise EnvelopeError("company name requires contract identity")
        if raw["method"] == "symbol_search" and con_id != 0:
            raise EnvelopeError("symbol search has no contract identity")
        return Envelope(variant, symbol, con_id, method=raw["method"])
    if raw["what_to_show"] not in KINDS.values() or type(raw["use_rth"]) is not bool:
        raise EnvelopeError("invalid IB data kind/session fields")
    if variant == "ibkr-head":
        return Envelope(variant, symbol, con_id, what_to_show=raw["what_to_show"],
                        use_rth=raw["use_rth"])
    base, kind, session = parse_token(raw["token"])
    if (raw["what_to_show"] != KINDS[kind] or raw["bar_size"] != BAR_SIZES[base][0]):
        raise EnvelopeError("token and IB fields disagree")
    if variant == "ibkr-bars-unfiltered":
        if raw["token"] != "1m-iv" or raw["use_rth"] is not False:
            raise EnvelopeError("unfiltered diagnostic requires 1m-iv with useRTH=False")
    elif raw["use_rth"] != SESSION_WINDOWS[session][0]:
        raise EnvelopeError("token and IB fields disagree")
    start, end, raw_end = (instant(raw[key]) for key in
                           ("intended_start", "intended_end", "raw_end"))
    if not start < end or raw_end < end or carrier_start(raw_end, raw["duration"]) > start:
        raise EnvelopeError("raw carrier does not contain the intended range")
    return Envelope(variant, symbol, con_id, token=raw["token"],
                    what_to_show=raw["what_to_show"], use_rth=raw["use_rth"],
                    bar_size=raw["bar_size"], raw_end=raw_end, duration=raw["duration"],
                    intended_start=start, intended_end=end)


@dataclass(frozen=True)
class Decision:
    state: str
    reason: str
    requested: Envelope
    effective: Envelope | None
    horizon: datetime | None


def _parse_http(raw):
    variant = raw["variant"]
    fields = {"variant", "endpoint", "subject"}
    if variant == "http-series":
        fields |= {"token", "intended_start", "intended_end"}
    if set(raw) != fields:
        raise EnvelopeError("HTTP envelope variant/shape mismatch")
    endpoint, _ = endpoint_policy(raw["endpoint"])
    if endpoint.variant != variant:
        raise EnvelopeError("HTTP endpoint/variant mismatch")
    subject = raw["subject"]
    if (not isinstance(subject, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", subject)):
        raise EnvelopeError("invalid HTTP subject")
    if variant == "http-metadata":
        return Envelope(variant, "", 0, endpoint=raw["endpoint"], subject=subject)
    if raw["token"] != "1d":
        raise EnvelopeError("HTTP series requires canonical trades daily token")
    start, end = instant(raw["intended_start"]), instant(raw["intended_end"])
    if not start < end:
        raise EnvelopeError("invalid HTTP intended range")
    return Envelope(variant, "", 0, token="1d", intended_start=start,
                    intended_end=end, endpoint=raw["endpoint"], subject=subject)


def _http_decide(request, authority, horizons, captured_now):
    if request.variant == "http-metadata":
        return Decision("allowed", "named horizon-exempt HTTP metadata", request, request, None)
    endpoint, _ = endpoint_policy(request.endpoint)
    horizon = horizons.get(request.token)
    try:
        authority.row(request.intended_start.date())
        authority.row(request.intended_end.date())
        if not endpoint.ranged:
            now = aware_ny(captured_now)
            window = authority.window("1d", now.date())
            if window is not None and now < authority.daily_settled_close(now.date()):
                return Decision("refused", "range-less HTTP requires daily settlement",
                                request, None, horizon)
        day, found = request.intended_start.date(), False
        while day <= request.intended_end.date():
            window = authority.window("1d", day)
            if window is not None and horizon is not None and window[1] <= min(horizon, request.intended_end):
                found = True
                break
            day += timedelta(days=1)
    except CalendarUnsupported as exc:
        return Decision("calendar_unsupported", str(exc), request, None, horizon)
    if not found or horizon is None or horizon <= request.intended_start:
        return Decision("refused", "no settled HTTP dates in intended range", request, None, horizon)
    if request.intended_end <= horizon:
        return Decision("allowed", "HTTP range within captured horizon", request, request, horizon)
    return Decision("clamped", "HTTP end clamped; intended start preserved", request,
                    replace(request, intended_end=horizon), horizon)


def decide(request: Envelope, authority: ScheduleAuthority,
           horizons: Mapping[str, datetime | None], *, captured_now=None) -> Decision:
    # Revalidate even a manually constructed dataclass; no caller bypass.
    request = parse_envelope(request.wire())
    if request.variant.startswith("http-"):
        return _http_decide(request, authority, horizons, captured_now)
    if request.variant not in {"ibkr-bars", "ibkr-bars-unfiltered"}:
        return Decision("allowed", "ledger-only horizon-exempt metadata", request, request, None)
    horizon = horizons.get(request.token)
    try:
        authority.row(request.intended_start.date())
        authority.row(request.intended_end.date())
        # The intended range cannot be narrowed to hide an unsupported date.
        # A clamped carrier is re-encoded below; its effective bounds must also
        # remain in coverage before it can be delivered to a transport.
        base, _, session = parse_token(request.token)
        if base == "1d" and session != "rth":
            raise CalendarUnsupported("daily extended session is unsupported")
        day = request.intended_start.date()
        open_found = False
        while day <= request.intended_end.date():
            window = authority.window(request.token, day)
            if window is not None:
                if base == "1d":
                    open_found = True
                elif window[0] < request.intended_end and window[1] > request.intended_start:
                    open_found = True
            day += timedelta(days=1)
    except CalendarUnsupported as exc:
        return Decision("calendar_unsupported", str(exc), request, None, horizon)
    if not open_found or horizon is None or horizon <= request.intended_start:
        return Decision("refused", "no settled session in intended range", request, None, horizon)
    if request.variant == "ibkr-bars-unfiltered":
        # The raw carrier may contain extra rows, but only one exact, settled
        # prior-day RTH session is the intended diagnostic range.
        window = authority.window(request.token, request.intended_end.date())
        if (window is None or request.intended_start != window[0]
                or request.intended_end != window[1]
                or horizon < window[1] or captured_now is None
                or request.intended_end.date() >= aware_ny(captured_now).date()):
            return Decision("refused", "unfiltered diagnostic requires one prior settled RTH session",
                            request, None, horizon)
    effective_end = min(request.intended_end, horizon)
    # Daily midnight labels do not make an unsettled current session eligible:
    # a same-day intended range must reach that day's actual session close.
    if base == "1d" and effective_end.date() < request.intended_start.date():
        return Decision("refused", "daily session is not settled", request, None, horizon)
    if request.raw_end <= horizon:
        if request.duration.endswith(" S") and int(request.duration[:-2]) > 28_800:
            return Decision("refused", "seconds carrier exceeds 28800 S", request, None, horizon)
        if request.duration.endswith(" D") and int(request.duration[:-2]) > 365:
            return Decision("refused", "days carrier exceeds 365 D; use years", request, None, horizon)
        try:
            authority.row(carrier_start(request.raw_end, request.duration).date())
        except CalendarUnsupported as exc:
            return Decision("calendar_unsupported", str(exc), request, None, horizon)
        return Decision("allowed", "carrier within captured horizon", request, request, horizon)
    try:
        effective = replace(request, intended_end=effective_end, raw_end=effective_end,
                            duration=encode_carrier(request.intended_start, effective_end))
        effective = parse_envelope(effective.wire())
        authority.row(carrier_start(effective.raw_end, effective.duration).date())
    except AuthorityError as exc:
        return Decision("refused", str(exc), request, None, horizon)
    return Decision("clamped", "end clamped; intended start preserved", request, effective, horizon)
