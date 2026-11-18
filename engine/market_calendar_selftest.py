"""Self-tests for market_calendar.py (NYSE half-day early-close calendar).

Standalone, no framework, no network: python engine/market_calendar_selftest.py
Every check prints PASS/FAIL; exit code 1 on any failure.
"""

import sys
from datetime import date, datetime, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import market_calendar as mc   # noqa: E402

FAILS = []
N = [0]


def check(label, cond, detail=""):
    N[0] += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {label}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(label)


print("=== [1] known half-days are early closes ===========================")
KNOWN_HALF_DAYS = [
    date(2023, 7, 3),    # July 3 (Mon)
    date(2023, 11, 24),  # Black Friday
    date(2024, 7, 3),    # July 3 (Wed)
    date(2024, 11, 29),  # Black Friday
    date(2024, 12, 24),  # Christmas Eve (Tue)
    date(2025, 11, 28),  # Black Friday
    date(2025, 12, 24),  # Christmas Eve (Wed)
    date(2026, 11, 27),  # Black Friday
    date(2026, 12, 24),  # Christmas Eve (Thu)
]
for d in KNOWN_HALF_DAYS:
    check(f"is_early_close({d}) is True", mc.is_early_close(d) is True)

check("every EARLY_CLOSE_DATES entry is a weekday (Mon-Fri)",
      all(d.weekday() < 5 for d in mc.EARLY_CLOSE_DATES),
      str(sorted(d for d in mc.EARLY_CLOSE_DATES if d.weekday() >= 5)))

print("=== [2] normal trading days are NOT early closes ===================")
check("normal day 2024-06-17 -> False", mc.is_early_close(date(2024, 6, 17)) is False)
check("normal day 2025-03-12 -> False", mc.is_early_close(date(2025, 3, 12)) is False)
check("2023-12-22 was a normal full session, not an observed early close",
      mc.is_early_close(date(2023, 12, 22)) is False)
# Dec 24 2022 fell on a Saturday: no session, must not be in the set.
check("weekend 2022-12-24 (Sat) not in set",
      date(2022, 12, 24) not in mc.EARLY_CLOSE_DATES
      and mc.is_early_close(date(2022, 12, 24)) is False)
# July 3 2026 falls on a Friday = observed July-4 holiday (full close, no half-day).
check("2026-07-03 (observed holiday) not an early close",
      mc.is_early_close(date(2026, 7, 3)) is False)

print("=== [3] datetime input is compared by date =========================")
check("datetime on a half-day -> True",
      mc.is_early_close(datetime(2024, 12, 24, 11, 5)) is True)
check("datetime on a normal day -> False",
      mc.is_early_close(datetime(2024, 6, 17, 15, 59)) is False)
try:
    mc.is_early_close("2024-12-24")
    check("non-date input rejected", False)
except TypeError:
    check("non-date input rejected", True)

print("=== [4] close-time helpers =========================================")
check("early_close_time() == 13:00", mc.early_close_time() == time(13, 0))
check("regular_close_time() == 16:00", mc.regular_close_time() == time(16, 0))
check("expected_close on a half-day == 13:00",
      mc.expected_close(date(2024, 12, 24)) == time(13, 0))
check("expected_close on a normal day == 16:00",
      mc.expected_close(date(2024, 6, 17)) == time(16, 0))
check("expected_close on 2023-12-22 == 16:00",
      mc.expected_close(date(2023, 12, 22)) == time(16, 0))
check("expected_close accepts datetime",
      mc.expected_close(datetime(2025, 11, 28, 10, 0)) == time(13, 0))

print("=== [5] full-closure holidays (computed) ===========================")
HOLIDAYS_2024 = [
    date(2024, 1, 1),    # New Year's Day (Mon)
    date(2024, 1, 15),   # MLK (3rd Mon Jan)
    date(2024, 2, 19),   # Washington's Birthday (3rd Mon Feb)
    date(2024, 3, 29),   # Good Friday (Easter Mar 31)
    date(2024, 5, 27),   # Memorial Day (last Mon May)
    date(2024, 6, 19),   # Juneteenth (Wed)
    date(2024, 7, 4),    # Independence Day (Thu)
    date(2024, 9, 2),    # Labor Day (1st Mon Sep)
    date(2024, 11, 28),  # Thanksgiving (4th Thu Nov)
    date(2024, 12, 25),  # Christmas (Wed)
]
for d in HOLIDAYS_2024:
    check(f"is_holiday({d}) is True", mc.is_holiday(d) is True)
    check(f"is_trading_day({d}) is False", mc.is_trading_day(d) is False)
# weekend observance shifts
check("New Year 2023: Sun Jan 1 -> observed Mon Jan 2",
      mc.is_holiday(date(2023, 1, 2)) and not mc.is_holiday(date(2023, 1, 1)))
check("New Year 2022: Sat Jan 1 NOT observed (no makeup)",
      not mc.is_holiday(date(2022, 1, 1))
      and mc.is_trading_day(date(2021, 12, 31)))
check("July 4 2021 (Sun) -> observed Mon Jul 5",
      mc.is_holiday(date(2021, 7, 5)) and not mc.is_holiday(date(2021, 7, 4)))
check("Christmas 2021 (Sat) -> observed Fri Dec 24",
      mc.is_holiday(date(2021, 12, 24)))
# Juneteenth only an NYSE holiday from 2022
check("Juneteenth NOT a holiday in 2021 (pre-NYSE)",
      not mc.is_holiday(date(2021, 6, 19)))
check("Juneteenth 2022 (Sun) -> observed Mon Jun 20",
      mc.is_holiday(date(2022, 6, 20)))
check("2025-01-09 National Day of Mourning is a full closure",
      mc.is_holiday(date(2025, 1, 9))
      and not mc.is_trading_day(date(2025, 1, 9)))
check("2012-10-29 & 30 Hurricane Sandy are full closures",
      not mc.is_trading_day(date(2012, 10, 29))
      and not mc.is_trading_day(date(2012, 10, 30)))
check("2018-12-05 National Day of Mourning (G.H.W. Bush) is a full closure",
      mc.is_holiday(date(2018, 12, 5))
      and not mc.is_trading_day(date(2018, 12, 5)))
# trading-day basics
check("normal Mon 2024-06-17 is a trading day",
      mc.is_trading_day(date(2024, 6, 17)) is True)
check("Saturday 2024-06-15 is NOT a trading day",
      mc.is_trading_day(date(2024, 6, 15)) is False)
check("a HALF-day IS still a trading day (2024-12-24)",
      mc.is_trading_day(date(2024, 12, 24)) is True
      and not mc.is_holiday(date(2024, 12, 24)))
check("is_trading_day accepts datetime",
      mc.is_trading_day(datetime(2024, 6, 17, 10, 0)) is True)

print()
print(f"{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    for f_ in FAILS:
        print(f"  FAILED: {f_}")
    sys.exit(1)
print("ALL PASS")
