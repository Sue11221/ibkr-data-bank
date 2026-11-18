"""Offline A1-2 contracts, durability faults, concurrency and inverse mutations."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import copy
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, time, timedelta, timezone
import inspect
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
from unittest.mock import patch
import warnings

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import check_kit
import fetch_authority as authority_module
from fetch_authority import (AuthorityError, BAR_SIZES, CalendarUnsupported, KINDS, NY,
                             ScheduleAuthority, SESSION_WINDOWS, TOKENS, aware_ny,
                             canonical_bytes, parse_token, strict_json)
import fetch_envelopes as envelopes
from fetch_envelopes import parse_envelope, carrier_start, decide
import fetch_ledger as ledger_module
from fetch_ledger import FetchLedger, LedgerError, inspect_ledger
import fetch_run_context as run
from fetch_run_context import FetchRunContext, RequestRefused

KIT = check_kit.CheckKit()
check = KIT.check


def rejects(name, function, exception=Exception, contains=""):
    try:
        function()
    except exception as exc:
        check(name, contains in str(exc), str(exc))
    except BaseException as exc:
        check(name, False, f"wrong error: {exc!r}")
    else:
        check(name, False, "unexpected success")


def ny(value):
    return datetime.fromisoformat(value).replace(tzinfo=NY)


TABLE = strict_json(authority_module.TABLE_PATH.read_bytes())
AUTH = ScheduleAuthority.from_table(TABLE)
NOW = ny("2026-07-08T16:20:00")
BAR_PRODUCER = "ibkr.gap_fill.session_fetch"


def bars(token="1d", start="2026-07-08T00:00:00", end="2026-07-08T16:00:00",
         raw_end=None, duration="1 D"):
    base, kind, session = parse_token(token)
    return dict(variant="ibkr-bars", symbol="TEST", con_id=123, token=token,
                what_to_show=KINDS[kind], use_rth=SESSION_WINDOWS[session][0],
                bar_size=BAR_SIZES[base][0], intended_start=ny(start).isoformat(),
                intended_end=ny(end).isoformat(), raw_end=ny(raw_end or end).isoformat(),
                duration=duration)


def context(directory, now=NOW, **kwargs):
    return FetchRunContext.create(directory, authority=kwargs.pop("authority", AUTH),
                                  clock=lambda: now, **kwargs)


def execute(ctx, envelope=None, transport=None, producer=BAR_PRODUCER, **kwargs):
    request = ctx.worker("worker1").request(producer, envelope or bars())
    return request.execute(transport or (lambda effective: []), acquire_turn=lambda: 0.0, **kwargs)


def false_early():
    table = copy.deepcopy(TABLE)
    row = next(row for row in table["rows"] if row["date"] == "2026-07-08")
    row.update(status="open_early_close", close_et="13:00")
    table["header"]["status_counts"]["open_regular"] -= 1
    table["header"]["status_counts"]["open_early_close"] += 1
    return ScheduleAuthority.from_table(table)


KIT.section("authority, complete dates and fixed policy")
check("committed table exact date denominator", len(AUTH.rows) == 17897
      and AUTH.first_date == date(1980, 1, 2) and AUTH.valid_through == date(2028, 12, 31))
check("all eleven bases and 132 canonical tokens", len(BAR_SIZES) == 11 and len(TOKENS) == 132)
rejects("immutable row mapping", lambda: AUTH.rows.__setitem__(date(2026, 7, 8), None), AttributeError)
rejects("immutable row values", lambda: setattr(AUTH.row(date(2026, 7, 8)), "close", time(13)), FrozenInstanceError)
for mutation in ("duplicate-plus-gap", "missing", "outside", "null-close", "policy", "provenance"):
    bad = copy.deepcopy(TABLE)
    if mutation == "duplicate-plus-gap":
        bad["rows"][1] = copy.deepcopy(bad["rows"][0])
    elif mutation == "missing":
        bad["rows"].pop(0)
    elif mutation == "outside":
        bad["rows"][0]["date"] = "1979-12-31"
    elif mutation == "null-close":
        bad["rows"][0]["close_et"] = None
    elif mutation == "policy":
        bad["header"]["settlement"]["daily_settle_margin_minutes"] = 0
    else:
        bad["header"]["provenance"]["consensus_dataset_sha256"] = "missing"
    rejects("table refuses " + mutation, lambda: ScheduleAuthority.from_table(bad), AuthorityError)
rejects("duplicate JSON key", lambda: strict_json('{"a":1,"a":2}'), AuthorityError)
rejects("nonfinite JSON", lambda: strict_json('{"a":NaN}'), AuthorityError)
for value in (datetime(2026, 1, 1), ny("2026-03-08T02:30:00")):
    rejects("naive/nonexistent wall time " + str(value), lambda: aware_ny(value), AuthorityError)
check("UTC converted explicitly", aware_ny(datetime(2026, 7, 8, 20, 20, tzinfo=timezone.utc)) == NOW)
for day in (date(1979, 12, 31), date(2029, 1, 1)):
    rejects("outside coverage " + str(day), lambda: AUTH.row(day), CalendarUnsupported)
rejects("runway 13 days fails", lambda: AUTH.check_clock(ny("2028-12-18T12:00:00")), CalendarUnsupported)
with warnings.catch_warnings(record=True) as observed_warnings:
    warnings.simplefilter("always")
    AUTH.check_clock(ny("2028-12-17T12:00:00"))
    AUTH.check_clock(ny("2028-11-02T12:00:00"))
    AUTH.check_clock(ny("2028-11-01T12:00:00"))
check("14 and 59 days warn, 60 does not", len(observed_warnings) == 2)
for day in (date(2012, 11, 23), date(2025, 12, 24)):
    check("historical early close " + str(day), AUTH.row(day).close == time(13))
    check("genuine early daily floor " + str(day), AUTH.daily_settled_close(day).time() == time(16, 20))
check("historical table open retained", AUTH.row(date(1980, 1, 2)).open == time(10))
for day in (date(2026, 7, 3), date(2026, 7, 4), date(2001, 9, 11)):
    check("closure/weekend has no session " + str(day), AUTH.window("1m", day) is None)
FALSE_EARLY = false_early()
check("false early changes fingerprint", FALSE_EARLY.fingerprint != AUTH.fingerprint)
check("false early daily floor remains 16:20", FALSE_EARLY.daily_settled_close(date(2026, 7, 8)).time() == time(16, 20))

KIT.section("G1 boundary denominator: every applicable token, before/equal/after")
for day in (date(2026, 7, 8), date(2025, 12, 24), date(2026, 3, 9), date(2026, 11, 2)):
    for token in TOKENS:
        base, _, session = parse_token(token)
        if session != "rth" and (base == "1d" or AUTH.row(day).status == "open_early_close"):
            rejects(f"unsupported {day} {token}", lambda: AUTH.window(token, day), CalendarUnsupported)
            continue
        opening, closing = AUTH.window(token, day)
        stamp = datetime.combine(day, time(), NY) if base == "1d" else opening
        boundary = (AUTH.daily_settled_close(day) if base == "1d" else
                    opening + timedelta(seconds=BAR_SIZES[base][1] + 60))
        for offset, expected in ((-1, False), (0, True), (1, True)):
            check(f"label {day} {token} boundary {offset}",
                  AUTH.accepts_label(token, stamp, boundary + timedelta(seconds=offset)) == expected)
        expected_horizon = closing if base == "1d" else opening + timedelta(seconds=BAR_SIZES[base][1])
        check(f"horizon {day} {token} equality", AUTH.horizon(token, boundary) == expected_horizon)
        check(f"horizon {day} {token} pre-boundary", AUTH.horizon(token, boundary - timedelta(seconds=1)) < expected_horizon)
        if base != "1d":
            check(f"session end {day} {token}", AUTH.horizon(token, closing + timedelta(seconds=60)) == closing)
            check(f"end label excluded {day} {token}", not AUTH.accepts_label(token, closing, closing + timedelta(days=1)))

KIT.section("strict variants and range-preserving clamp")
valid = bars()
check("bars wire round trip", parse_envelope(valid).wire() == valid)
head = dict(variant="ibkr-head", symbol="TEST", con_id=123, what_to_show="TRADES", use_rth=True)
metadata = dict(variant="ibkr-metadata", symbol="TEST", con_id=0, method="qualification")
check("head shape", parse_envelope(head).wire() == head)
check("qualification shape", parse_envelope(metadata).wire() == metadata)
bad_shapes = [dict(head, intended_start=None), dict(metadata, token=None),
              dict(valid, use_rth=False), dict(valid, what_to_show="BID_ASK"),
              dict(valid, bar_size="30 secs"), dict(valid, con_id=True),
              dict(valid, token="1d-rth"), dict(valid, duration="Max"),
              dict(valid, duration="0 D"), dict(valid, intended_start=valid["intended_end"]),
              dict(valid, duration="1 S"), dict(valid, raw_end="2026-07-08T16:00:00"),
              dict(valid, raw_end="2026-07-08T15:59:59-04:00"),
              dict(metadata, method="company_name")]
for index, bad in enumerate(bad_shapes):
    rejects(f"invalid envelope {index}", lambda: parse_envelope(bad), AuthorityError)
check("month carrier handles February", carrier_start(ny("2024-03-31T16:00:00"), "1 M") == ny("2024-02-29T16:00:00"))
check("year carrier leap day", carrier_start(ny("2024-02-29T16:00:00"), "1 Y") == ny("2023-02-28T16:00:00"))
check("30-year probe decodes", carrier_start(NOW, "30 Y") == ny("1996-07-08T16:20:00"))
for start, end in (("2026-03-07T16:00:00", "2026-03-09T16:00:00"),
                   ("2026-10-31T09:30:00", "2026-11-02T16:00:00")):
    duration = envelopes.encode_carrier(ny(start), ny(end))
    check("DST start contained " + start, carrier_start(ny(end), duration) <= ny(start))
    check("DST carrier avoids oversized seconds " + start,
          not duration.endswith(" S") or int(duration[:-2]) <= 28800)
with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch))
    request = parse_envelope(bars(end="2026-07-08T23:59:00", duration="2 M"))
    decision = decide(request, AUTH, ctx.horizons)
    check("daily end clamped to close", decision.state == "clamped" and decision.effective.raw_end == ny("2026-07-08T16:00:00"))
    check("clamp preserves explicit start exactly", decision.effective.intended_start == request.intended_start
          and carrier_start(decision.effective.raw_end, decision.effective.duration) <= request.intended_start)
    for start in ("1980-12-02T00:00:00", "1996-07-08T00:00:00"):
        old = parse_envelope(bars(start=start, end="2026-07-08T23:59:00", duration="60 Y"))
        old_decision = decide(old, AUTH, ctx.horizons)
        check("covered Max/old-head start preserved " + start,
              old_decision.state == "clamped" and old_decision.effective.intended_start == ny(start))
    edge = parse_envelope(bars(start="1980-01-02T00:00:00", end="2026-07-08T23:59:00", duration="60 Y"))
    check("rounded carrier outside first covered day refuses",
          decide(edge, AUTH, ctx.horizons).state == "refused")
    seconds_carrier = parse_envelope(bars(start="2026-07-07T00:00:00",
        end="2026-07-08T16:00:00", duration="144000 S"))
    check("otherwise allowed oversized seconds carrier refuses",
          decide(seconds_carrier, AUTH, ctx.horizons).state == "refused")
    check("28800 seconds uses bounded S carrier", envelopes.encode_carrier(
        ny("2026-07-08T08:00:00"), ny("2026-07-08T16:00:00")) == "28800 S")
    check("28801 seconds uses containing calendar carrier", envelopes.encode_carrier(
        ny("2026-07-08T07:59:59"), ny("2026-07-08T16:00:00")) == "1 D")
    check("month-end nonmatching wall times contained", carrier_start(
        ny("2024-03-31T16:00:00"), envelopes.encode_carrier(
            ny("2024-02-29T00:00:00"), ny("2024-03-31T16:00:00"))) <= ny("2024-02-29T00:00:00"))
    check("365-day carrier stays in days", envelopes.encode_carrier(
        ny("2025-07-08T16:00:00"), ny("2026-07-08T16:00:00")) == "365 D")
    check("366-day carrier uses containing years", envelopes.encode_carrier(
        ny("2025-07-07T16:00:00"), ny("2026-07-08T16:00:00")) == "2 Y")
    long_days = parse_envelope(bars(start="2025-07-07T16:00:00",
        end="2026-07-08T16:00:00", duration="366 D"))
    check("otherwise allowed oversized day carrier refuses",
          decide(long_days, AUTH, ctx.horizons).state == "refused")
    for start, end in (("1979-12-31T00:00:00", "2026-07-08T16:00:00"),
                       ("2026-07-08T00:00:00", "2029-01-01T16:00:00")):
        request = parse_envelope(bars(start=start, end=end, duration="60 Y"))
        check("intended range outside calendar refuses " + start,
              decide(request, AUTH, ctx.horizons).state == "calendar_unsupported")
    uncovered_carrier = parse_envelope(bars(duration="60 Y"))
    check("unclamped raw carrier before floor refuses", decide(uncovered_carrier, AUTH, ctx.horizons).state == "calendar_unsupported")
    ctx.ledger.close()

KIT.section("single clock, stable worker horizons, producer binding")
with tempfile.TemporaryDirectory() as scratch:
    calls = []
    ctx = FetchRunContext.create(Path(scratch), authority=AUTH,
                                clock=lambda: calls.append(1) or ny("2026-07-08T16:19:59"))
    workers = [ctx.worker(f"worker{index}") for index in range(8)]
    check("clock exactly once, all workers same context", len(calls) == 1 and all(worker.context is ctx for worker in workers))
    rejects("horizons immutable", lambda: ctx.horizons.__setitem__("1d", NOW), AttributeError)
    mutable_horizons = dict(ctx.horizons)
    detached = replace(ctx, horizons=mutable_horizons)
    mutable_horizons["1d"] = NOW
    check("direct construction detaches horizon mapping", detached.horizons["1d"] == ctx.horizons["1d"])
    rejects("direct incomplete horizon mapping fails", lambda: replace(ctx, horizons={}), AuthorityError)
    rejects("captured clock immutable", lambda: setattr(ctx, "captured_now", NOW), FrozenInstanceError)
    check("daily horizon still yesterday mid-run", ctx.horizons["1d"].date() == date(2026, 7, 7))
    with patch.object(ScheduleAuthority, "horizon", side_effect=AssertionError("mid-run recomputation")):
        for index in range(2):
            rejects(f"request/retry uses frozen horizon {index}", lambda: execute(ctx), RequestRefused)
    second = context(Path(scratch))
    check("new operation new clock/horizon/file", second.operation_id != ctx.operation_id
          and second.horizons["1d"].date() == date(2026, 7, 8) and second.ledger.path != ctx.ledger.path)
    for producer in ("ibkr.metadata.company_name", "ibkr.choke.liveib_bars", "unknown"):
        rejects("unknown/A2 producer " + producer, lambda: ctx.worker("w").request(producer, bars()), RequestRefused)
    rejects("variant mismatch", lambda: ctx.worker("w").request(BAR_PRODUCER, head), RequestRefused)
    inventory = strict_json((ROOT / "fetch_send_inventory.json").read_bytes())
    allowed = {site["producer_id"]: site["transport"] for site in inventory["sites"]
               if site["disposition"] == "a1_context_required" or
               (site["disposition"] == "guarded_choke_point" and site["transport"] == "ibkr-metadata")}
    check("A1 producer allowlist exact reviewed inventory", dict(run.A1_PRODUCERS) == allowed)
    forged = replace(second.worker("w").request(BAR_PRODUCER, bars()), producer_id="unknown")
    rejects("execute rechecks producer binding", lambda: forged.execute(lambda e: [], acquire_turn=lambda: 0), RequestRefused)
    forged_context = replace(second, operation_id="another-operation")
    rejects("execute rechecks context/ledger binding", lambda: execute(forged_context), RequestRefused)
    ctx.ledger.close()
    second.ledger.close()

KIT.section("physical attempt ordering, filtering, metadata, retries")
with tempfile.TemporaryDirectory() as scratch:
    trace = []
    ctx = context(Path(scratch), fault=lambda event, stage: trace.append((event, stage)))
    rows = [{"timestamp": ny(day + "T00:00:00"), "close": index}
            for index, day in enumerate(("2026-07-07", "2026-07-08", "2026-07-09", "2026-07-04"))]
    delivered = []
    def transport(envelope):
        trace.append(("transport", "call"))
        delivered.append(envelope)
        return rows
    request = ctx.worker("w").request(BAR_PRODUCER, bars(end="2026-07-08T23:59:00"))
    accepted = request.execute(transport, acquire_turn=lambda: trace.append(("pacer", "turn")) or 0.2)
    check("only intended settled day accepted", len(accepted) == 1 and accepted[0]["close"] == 1)
    check("caller response detached", accepted[0] is not rows[1])
    check("only effective envelope delivered", delivered[0].raw_end == ny("2026-07-08T16:00:00"))
    check("pacer before decision before send before result", trace == [
        ("pacer", "turn"), ("decision", "append"), ("decision", "flush"), ("decision", "fsync"),
        ("transport", "call"), ("result", "append"), ("result", "flush"), ("result", "fsync")])
    # Retry one logical request, each timeout/error/empty physically recorded.
    rejects("timeout propagates after result", lambda: request.execute(
        lambda e: (_ for _ in ()).throw(TimeoutError("fixture")), acquire_turn=lambda: 0), TimeoutError)
    rejects("transport error propagates after result", lambda: request.execute(
        lambda e: (_ for _ in ()).throw(ValueError("fixture")), acquire_turn=lambda: 0), ValueError)
    check("empty retry completes", request.execute(lambda e: [], acquire_turn=lambda: 0) == [])
    head_result = execute(ctx, head, lambda e: "1980-12-02", "ibkr.gap_fill.head_timestamp")
    metadata_result = execute(ctx, metadata, lambda e: {"id": 123}, "ibkr.choke.qualify")
    check("head and qualification ledger-only", head_result == "1980-12-02" and metadata_result == {"id": 123})
    ctx.seal()
    evidence = inspect_ledger(ctx.ledger.path)
    decisions = [event for event in evidence["events"] if event["event"] == "decision"]
    results = [event for event in evidence["events"] if event["event"] == "result"]
    check("all retries unique and same logical group", len({event["attempt_id"] for event in decisions}) == 6
          and len({event["logical_id"] for event in decisions[:4]}) == 1)
    first = results[0]["payload"]
    check("dual digests and row counts", first["observed"]["count"] == 4 and first["accepted"]["count"] == 1
          and first["dropped"] == 3 and first["observed"]["digest"] != first["accepted"]["digest"])
    check("timeout/error/empty outcomes", [event["payload"]["outcome"] for event in results[1:4]] == ["timeout", "error", "empty"])
    check("seal verifies and no unmatched", evidence["verified"] and not evidence["outcome_unknown"])
    rejects("sealed writer refuses new work", lambda: execute(ctx), LedgerError)
    ctx.ledger.close()

KIT.section("daily refusal, corrupted close and intraday settlement")
for authority in (AUTH, FALSE_EARLY):
    for producer, variant in run.A1_PRODUCERS.items():
        if variant != "ibkr-bars":
            continue
        with tempfile.TemporaryDirectory() as scratch:
            ctx = context(Path(scratch), ny("2026-07-08T13:20:00"), authority=authority)
            sends = []
            rejects("13:20 daily zero-send " + producer + authority.fingerprint[:5],
                    lambda: execute(ctx, transport=lambda e: sends.append(e), producer=producer), RequestRefused)
            ctx.seal()
            evidence = inspect_ledger(ctx.ledger.path)
            check("refusal pair complete " + producer + authority.fingerprint[:5], len(evidence["events"]) == 3 and not sends)
            ctx.ledger.close()
with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch), ny("2026-07-08T09:32:00"))
    rows = [{"timestamp": ny(f"2026-07-08T{stamp}"), "value": index}
            for index, stamp in enumerate(("09:29:00", "09:30:00", "09:30:30", "09:31:00", "09:32:00"))]
    output = execute(ctx, bars("1m", "2026-07-08T09:30:00", "2026-07-08T09:35:00"), lambda e: rows)
    check("intraday drops prefix, misaligned and unsettled rows", len(output) == 1 and output[0]["value"] == 1)
    partial = execute(ctx, bars("1m", "2026-07-08T09:30:00", "2026-07-08T09:30:30"), lambda e: rows)
    check("bar extending past intended end not accepted", partial == [])
    ctx.seal()
    ctx.ledger.close()

KIT.section("two-sided append/flush/fsync failures and seal receipt")
for event in ("decision", "result", "seal", "receipt"):
    for stage in (("append", "flush", "fsync", "publish") if event == "receipt" else ("append", "flush", "fsync")):
        with tempfile.TemporaryDirectory() as scratch:
            def fault(kind, point):
                if (kind, point) == (event, stage):
                    raise OSError("injected " + event + " " + stage)
            ctx = context(Path(scratch), fault=fault)
            sent, consumed = [], []
            def attempt():
                value = execute(ctx, transport=lambda e: sent.append(e) or [{"timestamp": NOW.replace(hour=0, minute=0), "close": 1}])
                consumed.extend(value)
                if event in {"seal", "receipt"}:
                    ctx.seal()
            rejects(f"{event}/{stage} stops", attempt, LedgerError)
            check(f"{event}/{stage} write failure poisons run", bool(ctx.ledger.failure) and not ctx.ledger.verified)
            check(f"{event}/{stage} prevents unsafe send/use", (not sent and not consumed) if event == "decision"
                  else (len(sent) == 1 and not consumed) if event == "result" else len(consumed) == 1)
            rejects(f"{event}/{stage} later attempt fails", lambda: execute(ctx), LedgerError)
            # close may flush bytes after injected failure; a seal alone still must not verify.
            ctx.ledger.close()
            rejects(f"{event}/{stage} disk cannot verify", lambda: inspect_ledger(ctx.ledger.path), LedgerError)
            if event == "result":
                check(f"{event}/{stage} quarantine exists", len(list(Path(scratch).glob("*.quarantine.json"))) == 1)
            if event == "result" and stage == "append":
                check("unmatched result is outcome_unknown", len(inspect_ledger(ctx.ledger.path, require_seal=False)["outcome_unknown"]) == 1)

KIT.section("real fsync calls, malformed responses and concurrent append")
for event, call_number in (("decision", 1), ("result", 2), ("seal", 3), ("receipt", 4)):
    with tempfile.TemporaryDirectory() as scratch:
        ctx = context(Path(scratch))
        real_fsync = ledger_module.os.fsync
        calls, sends, consumed = [], [], []
        def failing_fsync(fd):
            calls.append(fd)
            if len(calls) == call_number:
                raise OSError("real fsync seam failure")
            return real_fsync(fd)
        def attempt_with_real_failure():
            consumed.extend(execute(ctx, transport=lambda e: sends.append(e) or [
                {"timestamp": NOW.replace(hour=0, minute=0), "close": 1}]))
            ctx.seal()
        with patch.object(ledger_module.os, "fsync", side_effect=failing_fsync):
            rejects("actual fsync failure " + event, attempt_with_real_failure, LedgerError)
        check("actual fsync prevents unsafe use " + event,
              (not sends and not consumed) if event == "decision" else
              (len(sends) == 1 and not consumed) if event == "result" else bool(ctx.ledger.failure))
        ctx.ledger.close()
        rejects("actual fsync cannot verify " + event, lambda: inspect_ledger(ctx.ledger.path), LedgerError)

class BrokenFile:
    def __init__(self, original, mode):
        self.original, self.mode = original, mode
    def write(self, data):
        if self.mode == "short":
            return self.original.write(data[:7])
        if self.mode == "append":
            raise OSError("actual file write failure")
        return self.original.write(data)
    def flush(self):
        if self.mode == "flush":
            raise OSError("actual file flush failure")
        return self.original.flush()
    def fileno(self):
        return self.original.fileno()
    def close(self):
        return self.original.close()

for event in ("decision", "result", "seal"):
    for mode in ("short", "append", "flush"):
        with tempfile.TemporaryDirectory() as scratch:
            ctx = context(Path(scratch))
            consumed, sends = [], []
            original_file = ctx.ledger._file
            def transport(e):
                sends.append(e)
                if event == "result":
                    ctx.ledger._file = BrokenFile(original_file, mode)
                return [{"timestamp": NOW.replace(hour=0, minute=0), "close": 1}]
            def attempt_with_broken_file():
                if event == "decision":
                    ctx.ledger._file = BrokenFile(original_file, mode)
                consumed.extend(execute(ctx, transport=transport))
                if event == "seal":
                    ctx.ledger._file = BrokenFile(original_file, mode)
                    ctx.seal()
            rejects(f"actual file {event}/{mode} fails", attempt_with_broken_file, LedgerError)
            check(f"actual file {event}/{mode} blocks unsafe use",
                  (not sends and not consumed) if event == "decision" else
                  (len(sends) == 1 and not consumed) if event == "result" else bool(ctx.ledger.failure))
            ctx.ledger.close()
            rejects(f"actual file {event}/{mode} cannot verify", lambda: inspect_ledger(ctx.ledger.path), LedgerError)

with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch), fault=lambda kind, stage: (_ for _ in ()).throw(OSError("result failure"))
                  if (kind, stage) == ("result", "append") else None)
    with patch.object(Path, "open", side_effect=OSError("quarantine unavailable")):
        rejects("quarantine failure cannot permit consumption", lambda: execute(ctx,
            transport=lambda e: [{"timestamp": NOW.replace(hour=0, minute=0), "close": 1}]), LedgerError)
    check("quarantine failure retains run failure", bool(ctx.ledger.failure))
    ctx.ledger.close()

with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch))
    real_fsync = ledger_module.os.fsync
    with patch.object(ledger_module.os, "fsync", wraps=real_fsync) as sync:
        execute(ctx)
        ctx.seal()
        check("real decision/result/seal/receipt fsync calls", sync.call_count == 4)
    ctx.ledger.close()
with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch))
    for bad in ([{"close": 1}], [{"timestamp": NOW, "close": float("nan")}],
                [{"timestamp": datetime(2026, 7, 8), "close": 1}]):
        rejects("bad response halts consumption " + str(bad), lambda: execute(ctx, transport=lambda e: bad), (ValueError, AuthorityError))
    ctx.seal()
    check("normalization failures pair-complete", len(inspect_ledger(ctx.ledger.path)["events"]) == 7)
    ctx.ledger.close()
with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch))
    barrier = threading.Barrier(8)
    def worker(index):
        request = ctx.worker(f"w{index}").request(BAR_PRODUCER, bars())
        barrier.wait(timeout=10)
        for _ in range(8):
            request.execute(lambda e: [], acquire_turn=lambda: 0)
        return request.worker.context
    with ThreadPoolExecutor(max_workers=8) as pool:
        same = list(pool.map(worker, range(8)))
    ctx.seal()
    evidence = inspect_ledger(ctx.ledger.path)
    check("64 concurrent pairs plus seal parseable", len(evidence["events"]) == 129 and evidence["verified"])
    check("workers retained exact shared context", all(value is ctx for value in same))
    ctx.ledger.close()

KIT.section("parser corruption, duplicate IDs, unmatched and seal identity")
with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch))
    execute(ctx)
    ctx.seal()
    ctx.ledger.close()
    original = ctx.ledger.path.read_bytes()
    records = [strict_json(line) for line in original.splitlines()]
    for mode in ("truncated", "duplicate-key", "sequence", "duplicate-decision", "duplicate-result",
                 "wrong-operation", "wrong-attempt", "after-seal", "hash", "missing-result", "wrong-schema"):
        changed = copy.deepcopy(records)
        if mode == "sequence":
            changed[1]["seq"] = 4
        elif mode == "duplicate-decision":
            changed[1] = dict(changed[0], seq=2)
        elif mode == "duplicate-result":
            changed.insert(2, dict(changed[1], seq=3))
        elif mode == "wrong-operation":
            changed[1]["operation_id"] = "wrong"
        elif mode == "wrong-attempt":
            changed[1]["attempt_id"] = "wrong"
        elif mode == "after-seal":
            changed.append(dict(changed[1], seq=4))
        elif mode == "hash":
            changed[0]["payload"]["pacer_wait_seconds"] = 99
        elif mode == "missing-result":
            changed.pop(1)
        elif mode == "wrong-schema":
            changed[0]["schema_version"] = True
        raw = b"".join(canonical_bytes(event) + b"\n" for event in changed)
        if mode == "truncated":
            raw = raw[:-3]
        elif mode == "duplicate-key":
            raw = raw.replace(b'"seq":1', b'"seq":1,"seq":1', 1)
        target = Path(scratch) / (mode + ".jsonl")
        target.write_bytes(raw)
        rejects("parser fails " + mode, lambda: inspect_ledger(target), LedgerError)
    # Exercise pair checking independently of a seal hash mismatch.
    for mode in ("duplicate-decision", "duplicate-result", "changed-logical"):
        decision, result = records[:2]
        sequence = [decision, result, dict(decision, seq=3), dict(result, seq=4)]
        if mode == "duplicate-result":
            sequence = [decision, result, dict(result, seq=3)]
        elif mode == "changed-logical":
            sequence[2] = copy.deepcopy(sequence[2])
            sequence[2]["attempt_id"] = "new-attempt"
            sequence[2]["payload"]["requested"]["symbol"] = "CHANGED"
        target = Path(scratch) / (mode + "-unsealed.jsonl")
        target.write_bytes(b"".join(canonical_bytes(event) + b"\n" for event in sequence))
        rejects("unsealed structural check " + mode, lambda: inspect_ledger(target, require_seal=False), LedgerError)
    target = Path(scratch) / "unmatched.jsonl"
    target.write_bytes(canonical_bytes(records[0]) + b"\n")
    check("unmatched explicitly surfaced", inspect_ledger(target, require_seal=False)["outcome_unknown"] == [records[0]["attempt_id"]])
    rejects("unmatched cannot verify", lambda: inspect_ledger(target), LedgerError)
    target = Path(scratch) / "missing-receipt.jsonl"
    target.write_bytes(original)
    rejects("complete seal without receipt cannot verify", lambda: inspect_ledger(target), LedgerError)
    bad_receipt = strict_json(ctx.ledger.receipt_path.read_bytes())
    bad_receipt["event_count"] += 1
    target.with_suffix(".seal.json").write_bytes(canonical_bytes(bad_receipt))
    rejects("wrong seal receipt cannot verify", lambda: inspect_ledger(target), LedgerError)

with tempfile.TemporaryDirectory() as scratch:
    writer = FetchLedger(Path(scratch), "operation")
    ids = dict(operation_id="operation", worker_id="w", producer_id="p", logical_id="l", attempt_id="a")
    writer.decision(ids, {"requested": {"variant": "fixture"}})
    rejects("writer duplicate decision fails", lambda: writer.decision(ids, {"requested": {}}), LedgerError)
    rejects("writer unmatched seal fails", writer.seal, LedgerError)
    rejects("writer mismatched result fails", lambda: writer.result(dict(ids, worker_id="wrong"), {}), LedgerError)
    writer.result(ids, {"outcome": "empty"})
    rejects("writer duplicate result fails", lambda: writer.result(ids, {}), LedgerError)
    rejects("writer retry cannot change logical request", lambda: writer.decision(
        dict(ids, attempt_id="b"), {"requested": {"variant": "changed"}}), LedgerError)
    writer.seal()
    check("rejected structural mutations never append bytes", len(inspect_ledger(writer.path)["events"]) == 3)
    writer.close()

KIT.section("interrupted physical attempt stays explicitly unverified (O3)")
for interruption in (KeyboardInterrupt, SystemExit):
    with tempfile.TemporaryDirectory() as scratch:
        ctx = context(Path(scratch))
        try:
            sends = []
            def interrupted_transport(envelope):
                sends.append(envelope)
                raise interruption("fixture interrupted after decision")
            rejects("interrupt propagates " + interruption.__name__,
                    lambda: execute(ctx, transport=interrupted_transport), interruption)
            partial = inspect_ledger(ctx.ledger.path, require_seal=False)
            check("interrupt leaves one outcome_unknown " + interruption.__name__,
                  len(sends) == 1 and len(partial["outcome_unknown"]) == 1
                  and not partial["verified"])
            rejects("interrupted run cannot seal " + interruption.__name__, ctx.seal, LedgerError)
            check("interrupted run has no receipt " + interruption.__name__,
                  not ctx.ledger.receipt_path.exists())
        finally:
            ctx.ledger.close()

source = inspect.getsource(envelopes.parse_envelope)
needle = "raw_end < end or "
check("F-A12-1 inverse containment source anchored", source.count(needle) == 1)
namespace = dict(vars(envelopes))
exec(compile(source.replace(needle, ""), "<containment-mutant>", "exec"), namespace)
short_end = dict(valid, raw_end="2026-07-08T15:59:59-04:00")
check("inverse F-A12-1 removal violates containment oracle",
      namespace["parse_envelope"](short_end).raw_end < parse_envelope(valid).intended_end)

KIT.section("copy portability and inverse guard probes")
with tempfile.TemporaryDirectory() as scratch:
    relocated = Path(scratch) / "copied engine"
    relocated.mkdir()
    for name in ("fetch_authority.py", "fetch_envelopes.py", "fetch_ledger.py", "fetch_run_context.py", "fetch_governors.py", "fetch_operations.py", "session_schedule_table.json"):
        shutil.copy2(ROOT / name, relocated / name)
    probe = subprocess.run([sys.executable, "-c", "from pathlib import Path; from datetime import datetime; "
        "from fetch_authority import ScheduleAuthority,NY; from fetch_run_context import FetchRunContext; "
        "a=ScheduleAuthority.load(); c=FetchRunContext.create(Path('logs'),authority=a,clock=lambda:datetime(2026,7,8,16,20,tzinfo=NY)); "
        "c.seal(); c.ledger.close(); print(len(a.rows))"], cwd=relocated, capture_output=True, text=True, timeout=30)
    check("plain copied modules resolve local schedule and ledger", probe.returncode == 0 and probe.stdout.strip() == "17897", " ".join(probe.stderr.splitlines()))

# In-memory mutations execute actual shipped method bodies with one guard
# removed. Oracles below require the mutant to violate the positive contract.
source = inspect.getsource(ScheduleAuthority.daily_settled_close)
mutant = source.replace("max(row.close, REGULAR_CLOSE)", "row.close")
namespace = dict(vars(authority_module))
exec(compile(textwrap.dedent(mutant), "<daily-floor-mutant>", "exec"), namespace)
with patch.object(ScheduleAuthority, "daily_settled_close", namespace["daily_settled_close"]):
    check("inverse daily floor bypass turns oracle RED", FALSE_EARLY.daily_settled_close(date(2026, 7, 8)).time() != time(16, 20))
with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch), ny("2026-07-08T09:32:00"))
    rows = [{"timestamp": ny("2026-07-08T09:32:00"), "close": 1}]
    with patch.object(run, "filter_rows", lambda observed, envelope, context: observed):
        value = execute(ctx, bars("30s", "2026-07-08T09:30:00", "2026-07-08T09:35:00"), lambda e: rows)
        check("inverse 30s filter bypass turns oracle RED", len(value) != 0)
    ctx.ledger.close()
with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch), fault=lambda kind, stage: (_ for _ in ()).throw(OSError("fixture"))
                  if (kind, stage) == ("decision", "append") else None)
    sent = []
    with patch.object(ctx.ledger, "decision", lambda *args: None), patch.object(ctx.ledger, "result", lambda *args: None):
        execute(ctx, transport=lambda e: sent.append(e) or [])
    check("inverse decision bypass turns zero-send oracle RED", bool(sent))
    ctx.ledger.close()
with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch), fault=lambda kind, stage: (_ for _ in ()).throw(OSError("fixture"))
                  if (kind, stage) == ("result", "append") else None)
    consumed = []
    with patch.object(ctx.ledger, "result", lambda *args: None):
        consumed.extend(execute(ctx, transport=lambda e: [{"timestamp": NOW.replace(hour=0, minute=0), "close": 1}]))
    check("inverse result bypass turns no-consumption oracle RED", bool(consumed))
    ctx.ledger.close()

with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch), ny("2026-07-08T16:19:59"))
    future = {token: AUTH.horizon(token, NOW) for token in TOKENS}
    real_decide = run.decide
    try:
        with patch.object(run, "decide", lambda envelope, authority, horizons, **kwargs:
                          real_decide(envelope, authority, future, **kwargs)):
            sends = []
            execute(ctx, transport=lambda e: sends.append(e) or [])
        check("inverse mid-run horizon recomputation turns refusal oracle RED", bool(sends))
    finally:
        ctx.ledger.close()

with tempfile.TemporaryDirectory() as scratch:
    ctx = context(Path(scratch))
    source = inspect.getsource(FetchLedger._append)
    needle = "os.fsync(self._file.fileno())"
    check("fsync inverse mutation source anchored", source.count(needle) == 1)
    namespace = dict(vars(ledger_module))
    exec(compile(textwrap.dedent(source.replace(needle, "pass")), "<fsync-mutant>", "exec"), namespace)
    sends = []
    with patch.object(FetchLedger, "_append", namespace["_append"]), patch.object(
            ledger_module.os, "fsync", side_effect=OSError("fsync failure")):
        execute(ctx, transport=lambda e: sends.append(e) or [])
    check("inverse actual fsync removal turns zero-send oracle RED", bool(sends))
    ctx.ledger.close()

sys.exit(KIT.finish())
