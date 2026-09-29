"""Windows input guard for the GUI-automation restart.

Optionally FREEZE the user's mouse + keyboard during the relaunch drive (so they
can't accidentally interfere), with three layered safety guarantees so the user
can never get locked out:

  * ESC abort — a low-level keyboard hook AND a GetAsyncKeyState poll both watch
    for ESC; either one sets the abort flag. The OWNING worker thread then
    releases input at its next abort checkpoint (see below).
  * WATCHDOG — if input stays blocked longer than max_block_s, it sets the abort
    flag so the worker releases ASAP. So a stuck step can never keep input
    frozen indefinitely as long as the worker stays responsive.
  * Ctrl+Alt+Del — Windows never lets BlockInput swallow the secure-attention
    sequence, so that is always the ultimate escape (and the real failsafe if
    the worker is wedged inside a blocking call).

CRITICAL WINDOWS REALITY: BlockInput(FALSE) only takes effect from the SAME
thread that called BlockInput(TRUE); a release attempt from any other thread is
a silent no-op. So block()/unblock() are OWNER-thread operations: the worker
that calls block() is the only thread that can physically release. The ESC
poller, the keyboard hook and the watchdog therefore NEVER call BlockInput
themselves — they only request_abort(), and the worker unblocks itself at the
next checkpoint (or in its finally). For that to be responsive the worker's
long steps must poll abort_check; see relaunch_one / _drive_login.

BlockInput may be refused without elevation; if so, block() is a no-op (no harm,
just no protection). Windows-only. The ctypes calls are isolated behind small
functions / injectable callables so the orchestration logic is unit-testable
headlessly.
"""
import threading
import time

VK_ESCAPE = 0x1B


def _user32():
    import ctypes
    return ctypes.WinDLL("user32", use_last_error=True)


def block_input(flag):
    """BlockInput(flag): freeze (True) / release (False) the physical mouse and
    keyboard. Returns True iff the call succeeded (it is refused without the
    right integrity level — then there is simply no block)."""
    try:
        return bool(_user32().BlockInput(bool(flag)))
    except Exception:  # noqa: BLE001
        return False


def esc_pressed():
    """True if ESC is physically down right now (GetAsyncKeyState)."""
    try:
        return bool(_user32().GetAsyncKeyState(VK_ESCAPE) & 0x8000)
    except Exception:  # noqa: BLE001
        return False


def is_elevated():
    """True if the process runs at an integrity level that lets BlockInput
    actually freeze input (i.e. 'Run as administrator'). When False, block() is
    a no-op and the restart proceeds with minimize + ESC only."""
    try:
        import ctypes
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:  # noqa: BLE001
        return False


def minimize_all():
    """Show desktop — minimize every window for a clean drive."""
    _shell("MinimizeAll")


def restore_all():
    """Undo the show-desktop."""
    _shell("UndoMinimizeALL")


def _shell(method):
    import subprocess
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command",
             f"(New-Object -ComObject Shell.Application).{method}()"],
            capture_output=True, timeout=10)
    except Exception:  # noqa: BLE001
        pass


class InputGuard:
    """A block session with ESC abort + watchdog. Drive it like:

        g = InputGuard(on_progress=log, max_block_s=180)
        g.start()
        try:
            for port in ports:
                if g.aborted():
                    break
                g.block()
                try:
                    relaunch_one(..., abort_check=g.aborted)
                finally:
                    g.unblock()
        finally:
            g.stop()

    block_fn / esc_fn are injectable for tests; install_hook=False skips the
    real low-level keyboard hook (also for tests)."""

    def __init__(self, on_progress=None, max_block_s=180,
                 block_fn=block_input, esc_fn=esc_pressed, install_hook=True):
        self._log = on_progress or (lambda m: None)
        self.max_block_s = max_block_s
        self._block_fn = block_fn
        self._esc_fn = esc_fn
        self._install_hook = install_hook
        self.abort = threading.Event()
        self._stop = threading.Event()
        self._blocked_since = None
        self._owner_tid = None          # the thread that holds BlockInput(TRUE)
        self._lock = threading.Lock()
        self._threads = []
        self._hook = None
        self._proc = None
        self._warned_block = False
        self._watchdog_fired = False

    def aborted(self):
        return self.abort.is_set()

    def is_blocked(self):
        with self._lock:
            return self._blocked_since is not None

    def block(self):
        """Freeze input. MUST be called by the worker thread that will also
        unblock — it becomes the block OWNER (only it can physically release)."""
        with self._lock:
            if self._block_fn(True):
                self._blocked_since = time.monotonic()
                self._owner_tid = threading.get_ident()
                if not self._warned_block:
                    self._log("input frozen — ESC aborts; Ctrl+Alt+Del is the "
                              "failsafe")
            elif not self._warned_block:
                self._log("input-block unavailable (run as Administrator for "
                          "the hard freeze) — minimize + ESC only")
            self._warned_block = True

    def unblock(self):
        """Release the freeze. Effective ONLY from the owning thread (the one
        that called block()) — Windows ignores BlockInput(FALSE) from any other
        thread. A non-owner caller therefore does NOT fake a release: it
        requests an abort (so the owner releases at its next checkpoint) and
        leaves the watchdog armed."""
        tid = threading.get_ident()
        with self._lock:
            if self._blocked_since is None:
                return
            if self._owner_tid is not None and tid != self._owner_tid:
                self.abort.set()        # ask the owner to release; don't lie
                return
            self._block_fn(False)
            self._blocked_since = None
            self._owner_tid = None

    # ESC must come from the USER, not from the restart's own GUI automation:
    # tws_api.configure / open_config call pyautogui.press("escape") to dismiss
    # menus, and those injected presses carry the LLKHF_INJECTED flag. The LL
    # hook can see that flag (GetAsyncKeyState cannot), so PHYSICAL ESC = abort,
    # injected ESC = ignored.
    LLKHF_INJECTED = 0x10

    @classmethod
    def _is_physical_esc(cls, vk, flags):
        return vk == VK_ESCAPE and not (flags & cls.LLKHF_INJECTED)

    def request_abort(self, reason=None):
        """Signal the run to abort (any thread). Does NOT touch BlockInput — the
        owning worker thread performs the real release at its next abort check.
        Logs `reason` once (on the transition to aborted)."""
        first = not self.abort.is_set()
        self.abort.set()
        if reason and first:
            self._log(reason)

    def _watchdog(self):
        while not self._stop.is_set():
            with self._lock:
                bs = self._blocked_since
            if bs is not None and (time.monotonic() - bs) > self.max_block_s:
                # Do NOT clear _blocked_since here (cross-thread BlockInput(FALSE)
                # is a no-op, and clearing it would DISARM this watchdog while
                # input is still really frozen). Just keep the abort set; the
                # owning worker releases at its next checkpoint.
                if not self._watchdog_fired:
                    self._log("watchdog: max block time exceeded — aborting; "
                              "worker will release input at its next step "
                              "(Ctrl+Alt+Del is the failsafe)")
                    self._watchdog_fired = True
                self.abort.set()
            self._stop.wait(0.5)

    def _esc_poller(self):
        while not self._stop.is_set():
            # GetAsyncKeyState can't distinguish the restart's OWN injected ESC
            # presses from a real user ESC, so when the LL hook is active (it
            # CAN, via LLKHF_INJECTED) the poller defers to it and never fires —
            # otherwise the restart's pyautogui.press("escape") would abort the
            # run. The poller is only the fallback when no hook is installed.
            if self._install_hook and self._hook is not None:
                self._stop.wait(0.05)
                continue
            try:
                hit = self._esc_fn()
            except Exception:  # noqa: BLE001
                hit = False
            if hit:
                self.request_abort("ESC pressed — aborting (input releases at "
                                   "the next step)")
            self._stop.wait(0.03)

    def _hook_loop(self):
        # WH_KEYBOARD_LL so ESC is caught even when BlockInput is active.
        import ctypes
        from ctypes import wintypes
        user32 = _user32()
        WH_KEYBOARD_LL, WM_KEYDOWN, WM_SYSKEYDOWN = 13, 0x0100, 0x0104
        lresult = ctypes.c_ssize_t
        hookproc = ctypes.CFUNCTYPE(lresult, ctypes.c_int, wintypes.WPARAM,
                                    wintypes.LPARAM)
        # WITHOUT explicit argtypes ctypes defaults each arg to a 32-bit c_int,
        # so the 64-bit LPARAM (a pointer to KBDLLHOOKSTRUCT) overflows on EVERY
        # key event -> "OverflowError: int too long to convert" raised in the
        # callback on every keystroke, flooding stderr and leaving the hook chain
        # un-forwarded (which disrupts GUI automation like the Alt+F menu step).
        # Pin the real Win32 widths so the call is clean.
        user32.CallNextHookEx.restype = lresult
        user32.CallNextHookEx.argtypes = [wintypes.HHOOK, ctypes.c_int,
                                          wintypes.WPARAM, wintypes.LPARAM]
        user32.SetWindowsHookExW.restype = wintypes.HHOOK
        user32.SetWindowsHookExW.argtypes = [ctypes.c_int, hookproc,
                                             wintypes.HINSTANCE, wintypes.DWORD]
        user32.UnhookWindowsHookEx.restype = wintypes.BOOL
        user32.UnhookWindowsHookEx.argtypes = [wintypes.HHOOK]

        def proc(ncode, wparam, lparam):
            try:
                if ncode >= 0 and wparam in (WM_KEYDOWN, WM_SYSKEYDOWN):
                    # KBDLLHOOKSTRUCT: [0]=vkCode, [1]=scanCode, [2]=flags
                    kb = ctypes.cast(lparam, ctypes.POINTER(wintypes.DWORD))
                    if self._is_physical_esc(kb[0], kb[2]):
                        # PHYSICAL ESC only (not the restart's injected presses);
                        # hook thread is NOT the block owner — only signal, the
                        # worker releases at its next checkpoint.
                        self.abort.set()
            except Exception:  # noqa: BLE001
                pass
            # forward down the chain with the real hook handle (clean argtypes
            # above prevent the LPARAM overflow that used to raise here).
            return user32.CallNextHookEx(self._hook, ncode, wparam, lparam)

        self._proc = hookproc(proc)
        try:
            self._hook = user32.SetWindowsHookExW(WH_KEYBOARD_LL, self._proc,
                                                  None, 0)
        except Exception:  # noqa: BLE001
            self._hook = None
        msg = wintypes.MSG()
        while not self._stop.is_set():
            try:
                user32.PeekMessageW(ctypes.byref(msg), None, 0, 0, 1)
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.005)
        if self._hook:
            try:
                user32.UnhookWindowsHookEx(self._hook)
            except Exception:  # noqa: BLE001
                pass
            self._hook = None

    def start(self):
        self._stop.clear()
        self.abort.clear()
        self._watchdog_fired = False
        self._threads = [threading.Thread(target=self._watchdog, daemon=True),
                         threading.Thread(target=self._esc_poller, daemon=True)]
        if self._install_hook:
            self._threads.append(
                threading.Thread(target=self._hook_loop, daemon=True))
        for t in self._threads:
            t.start()

    def stop(self):
        self.unblock()
        self._stop.set()
        for t in self._threads:
            t.join(timeout=2)
        self._threads = []
