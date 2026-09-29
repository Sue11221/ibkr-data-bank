"""Headless tests for sp500.py live-fetch — NO network (injected opener)."""
import sys, tempfile, shutil
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parent))
import sp500

_P = [0]; _F = [0]
def check(c, n):
    if c: _P[0] += 1
    else: _F[0] += 1; print("  FAIL:", n)

class _Resp:
    def __init__(self, text): self._b = text.encode("utf-8")
    def read(self): return self._b

def opener_for(mapping):
    def _op(req, timeout=None):
        url = req.full_url
        for key, text in mapping.items():
            if key in url:
                if text is None:
                    raise OSError(f"simulated down: {key}")
                return _Resp(text)
        raise OSError(f"no mock for {url}")
    return _op

CSV = ("Symbol,Security,GICS Sector\n"
       + "\n".join(f"{s},Co {s},Tech"
                   for s in ["MMM", "AAPL", "BRK.B", "BF.B", "COIN"]
                   + [f"T{i}" for i in range(500)]))

def test_canon():
    check(sp500._canon("brk.b") == "BRK-B" and sp500._canon(" BF.B ") == "BF-B",
          "_canon upper + dots->dashes")

def test_validate():
    try:
        sp500._validate(["AAPL", "MSFT"]); ok = False
    except sp500.Sp500FetchError:
        ok = True
    check(ok, "_validate rejects an implausibly short list")
    big = sp500._validate(["AAPL", "AAPL", "BRK-B"] + [f"T{i}" for i in range(500)])
    check(len(big) == len(set(big)), "_validate dedupes")

@patch("fetch_ibkr_bridge.refuse_a2")  # Retained A2 parser; opener is a local fixture.
def test_datahub_parse(_hold):
    syms, src = sp500.fetch_current_sp500(
        _opener=opener_for({"githubusercontent": CSV}))
    check(src == "datahub" and "BRK-B" in syms and "BF-B" in syms and "COIN" in syms,
          "datahub CSV parsed + canonicalized (BRK.B->BRK-B)")
    check(400 <= len(syms) <= 600, "datahub list plausible size")

@patch("fetch_ibkr_bridge.refuse_a2")
def test_wikipedia_fallback(_hold):
    wiki = ('<table id="constituents">'
            + "".join(f"<tr><td><a href=x>{s}</a></td><td>Co</td></tr>"
                      for s in ["MMM", "AAPL", "BRK.B"] + [f"W{i}" for i in range(500)])
            + "</table>")
    syms, src = sp500.fetch_current_sp500(
        _opener=opener_for({"githubusercontent": None, "wikipedia": wiki}))
    check(src == "wikipedia" and "BRK-B" in syms,
          "falls back to wikipedia when datahub is down")

@patch("fetch_ibkr_bridge.refuse_a2")
def test_current_cache_and_fallback(_hold):
    base = tempfile.mkdtemp(prefix="sp500_")
    cache = str(Path(base) / "sp500_current.json")
    try:
        t, src, asof = sp500.current_sp500(
            cache_path=cache, _opener=opener_for({"githubusercontent": CSV}),
            _today="2026-06-24")
        check(src == "datahub" and asof == "2026-06-24" and Path(cache).exists(),
              "current_sp500 live success caches to disk")
        t2, src2, asof2 = sp500.current_sp500(
            cache_path=cache, _opener=opener_for({"none": None}))
        check(src2 == "cache" and asof2 == "2026-06-24" and t2 == t,
              "current_sp500 offline -> cache")
        t3, src3, _ = sp500.current_sp500(
            cache_path=str(Path(base) / "absent.json"),
            _opener=opener_for({"none": None}))
        check(src3 == "bundled" and t3 == sp500.SP500,
              "current_sp500 offline + no cache -> bundled snapshot")
    finally:
        shutil.rmtree(base, ignore_errors=True)

def test_a1_hold():
    from fetch_run_context import RequestRefused
    calls = []
    try:
        sp500._http("https://example.com", 1, lambda *a, **k: calls.append(a))
    except RequestRefused:
        check(not calls, "A1 hold refuses even an injected opener")
    else:
        check(False, "A1 hold must refuse")


def main():
    for t in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        t()
    tot = _P[0] + _F[0]
    print(f"\nsp500_selftest: {_P[0]}/{tot} passed, {_F[0]} failed")
    return 1 if _F[0] else 0

if __name__ == "__main__":
    sys.exit(main())
