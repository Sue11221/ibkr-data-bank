"""Headless tests for the InputGuard orchestration (block tracking, ESC abort,
watchdog auto-unblock) with the Windows calls injected.  No real BlockInput.
    python engine/tws_inputguard_selftest.py"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tws_inputguard import InputGuard

_PASS = [0]
_FAIL = [0]


def check(cond, name):
    if cond:
        _PASS[0] += 1
    else:
        _FAIL[0] += 1
        print("  FAIL:", name)


def test_block_tracking():
    calls = []
    g = InputGuard(block_fn=lambda f: calls.append(f) or True,
                   esc_fn=lambda: False, install_hook=False)
    check(not g.is_blocked(), "starts unblocked")
    g.block()
    check(g.is_blocked() and calls == [True], "block() freezes + tracks")
    g.unblock()
    check(not g.is_blocked() and calls == [True, False], "unblock() releases")


def test_block_refused_not_tracked():
    # BlockInput refused (no elevation) -> block_fn returns False -> not tracked
    g = InputGuard(block_fn=lambda f: False, esc_fn=lambda: False,
                   install_hook=False)
    g.block()
    check(not g.is_blocked(), "a refused block is not tracked (no false watchdog)")


def test_esc_aborts_owner_releases():
    # ESC fires on the poller thread (a NON-owner). Windows BlockInput(FALSE) is
    # a no-op from any thread but the one that blocked, so the poller must NOT
    # fake a release — it sets abort, and the OWNING thread (here, main) does the
    # real release. (Bug-hunt finding #2.)
    pressed = {"v": False}
    released = []
    g = InputGuard(block_fn=lambda f: (released.append(f) if not f else None)
                   or True, esc_fn=lambda: pressed["v"], install_hook=False)
    g.start()
    g.block()                                   # main thread is the block OWNER
    check(not g.aborted(), "no abort before ESC")
    pressed["v"] = True
    time.sleep(0.15)
    check(g.aborted(), "ESC (poller thread) sets abort")
    check(False not in released,
          "ESC from a non-owner thread does NOT fake a release")
    check(g.is_blocked(), "input stays blocked until the owner releases")
    g.unblock()                                 # owner releases for real
    check(False in released, "the owning thread's unblock() releases input")
    g.stop()


def test_watchdog_aborts_and_stays_armed():
    # The watchdog runs on its own (non-owner) thread, so it can't physically
    # release. It must set abort AND keep itself armed (NOT clear _blocked_since)
    # so it stays effective until the owner releases. The old code self-disarmed
    # on its no-op cross-thread unblock. (Bug-hunt finding #3.)
    released = []
    g = InputGuard(block_fn=lambda f: (released.append(f) if not f else None)
                   or True, esc_fn=lambda: False, install_hook=False,
                   max_block_s=0.2)
    g.start()
    g.block()                                   # main thread owner
    time.sleep(0.6)
    check(g.aborted(), "watchdog aborts after max_block_s")
    check(g.is_blocked(),
          "watchdog does NOT self-disarm — stays armed (is_blocked True)")
    check(False not in released, "watchdog (non-owner) does not fake a release")
    g.unblock()                                 # owner releases
    check(not g.is_blocked() and False in released,
          "owner unblock after the watchdog fired releases + disarms")
    g.stop()


def test_is_physical_esc():
    # PHYSICAL ESC (flags=0) aborts; INJECTED ESC (LLKHF_INJECTED=0x10) — what
    # the restart's own pyautogui.press('escape') produces — must NOT, or the
    # restart self-aborts. Found by the live 4-port test.
    check(InputGuard._is_physical_esc(0x1B, 0x00) is True,
          "physical ESC (flags=0) -> abort")
    check(InputGuard._is_physical_esc(0x1B, 0x10) is False,
          "INJECTED ESC (flags=LLKHF_INJECTED) -> ignored (no self-abort)")
    check(InputGuard._is_physical_esc(0x1B, 0x11) is False,
          "injected+extended ESC -> ignored")
    check(InputGuard._is_physical_esc(0x41, 0x00) is False,
          "a non-ESC physical key -> no abort")


def test_poller_defers_to_active_hook():
    # When the LL hook is active it is the authoritative ESC source (it can read
    # LLKHF_INJECTED); the GetAsyncKeyState poller, which CANNOT distinguish
    # injected from physical, must defer and never fire — else the restart's
    # injected ESC presses would abort the run.
    import threading
    fired = []
    g = InputGuard(esc_fn=lambda: (fired.append(1) or True), install_hook=True)
    g._hook = 0x1234                         # simulate an installed hook
    g._stop.clear(); g.abort.clear()
    th = threading.Thread(target=g._esc_poller, daemon=True)
    th.start()
    time.sleep(0.2)
    g._stop.set(); th.join(timeout=1)
    check(not g.aborted() and not fired,
          "poller defers to active hook (never consults esc_fn / sets abort)")


def test_cross_thread_unblock_is_noop():
    # The heart of the bug: block() on thread A, unblock() from thread B must NOT
    # call BlockInput(FALSE) (Windows ignores it) and must NOT clear the blocked
    # state — it only requests abort. Only thread A can release. (Finding #2.)
    import threading
    released = []
    g = InputGuard(block_fn=lambda f: (released.append(f) if not f else None)
                   or True, esc_fn=lambda: False, install_hook=False)
    blocked_evt = threading.Event()
    release_evt = threading.Event()

    def worker():
        g.block()                               # WORKER is the owner
        blocked_evt.set()
        release_evt.wait(2)                     # park until main says go
        g.unblock()                             # owner releases for real

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    blocked_evt.wait(2)
    check(g.is_blocked(), "worker holds the block")
    g.unblock()                                 # MAIN is NOT the owner
    check(False not in released,
          "a non-owner unblock() does NOT release (cross-thread no-op)")
    check(g.aborted(), "a non-owner unblock() requests abort instead")
    check(g.is_blocked(), "block state preserved until the OWNER releases")
    release_evt.set()
    t.join(timeout=2)
    check(not g.is_blocked() and False in released,
          "the owner thread's unblock() performed the real release")


def test_stop_unblocks():
    released = []
    g = InputGuard(block_fn=lambda f: (released.append(f) if not f else None)
                   or True, esc_fn=lambda: False, install_hook=False)
    g.start()
    g.block()
    g.stop()
    check(not g.is_blocked() and False in released,
          "stop() always unblocks (finally-safety)")


def main():
    for t in [v for k, v in sorted(globals().items())
              if k.startswith("test_")]:
        t()
    total = _PASS[0] + _FAIL[0]
    print(f"\ntws_inputguard_selftest: {_PASS[0]}/{total} passed, "
          f"{_FAIL[0]} failed")
    return 1 if _FAIL[0] else 0


if __name__ == "__main__":
    sys.exit(main())
