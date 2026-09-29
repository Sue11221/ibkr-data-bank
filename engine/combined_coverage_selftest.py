"""Offline coverage/schema checks for full-history combined evidence."""

from datetime import date, datetime, timedelta

import stock_validate as sv


checks = []


def check(name, value):
    checks.append(bool(value))
    print(f"[{'PASS' if value else 'FAIL'}] {name}")


days = []
day = date(2024, 1, 2)
while len(days) < 80:
    if day.weekday() < 5:
        days.append(day)
    day += timedelta(days=1)

base = (100.0, 101.0, 99.0, 100.5, 1000.0)
off = (110.0, 111.0, 109.0, 110.5, 1000.0)
bars = [(datetime(d.year, d.month, d.day, 9, 30), *base) for d in days]
external = {d.isoformat(): base for d in days}
internal = {d: base for d in days}

clean = sv.combined_crosscheck(
    "unused", "AAA", read_fn=lambda *_args: bars,
    ext_ref=external, int_ref=internal)
coverage = clean.get("reference_coverage") or {}
check("current dual references publish explicit coverage",
      clean["status"] == "ok"
      and coverage["external"]["requested_range"] == sv.FULL_HISTORY_RANGE
      and coverage["internal"]["requested_range"] is None
      and coverage["external"]["head_ok"] is True
      and coverage["internal"]["head_ok"] is True)

shallow_external = {d.isoformat(): base for d in days[40:]}
shallow = sv.combined_crosscheck(
    "unused", "AAA", read_fn=lambda *_args: bars,
    ext_ref=shallow_external, int_ref=internal)
check("clean shallow reference demotes the verdict to inconclusive",
      shallow["status"] == "inconclusive"
      and shallow["reference_coverage"]["external"]["head_gap_days"]
      > sv.FULL_HISTORY_HEAD_TOLERANCE_DAYS)

flag_day = days[55]
shallow_external[flag_day.isoformat()] = off
flagged_internal = dict(internal)
flagged_internal[flag_day] = off
flagged = sv.combined_crosscheck(
    "unused", "AAA", read_fn=lambda *_args: bars,
    ext_ref=shallow_external, int_ref=flagged_internal)
check("persistent disagreement outranks shallow coverage",
      flagged["status"] == "flagged"
      and flagged["persistent"] == [flag_day.isoformat()])

missing = sv.combined_crosscheck(
    "unused", "AAA", read_fn=lambda *_args: bars,
    ext_ref={}, int_ref=internal)
check("missing reference is explicit and inconclusive",
      missing["status"] == "inconclusive"
      and missing["reference_coverage"]["external"] is None)

check("combined schema was bumped with full-history token centralized",
      sv.COMBINED_FLAGS_SCHEMA_VERSION == 3
      and sv.FULL_HISTORY_RANGE == "Max")

print(f"combined coverage: {sum(checks)}/{len(checks)} passed")
raise SystemExit(0 if all(checks) else 1)
