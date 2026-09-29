"""S&P 500 constituents for the presets — a bundled SNAPSHOT (`SP500`) PLUS a
live fetch (`current_sp500`) so a preset can pull TODAY's index.

The index changes a few times a year, so `current_sp500()` fetches the live list
(datahub CSV -> Wikipedia), caches it, and falls back to the bundled `SP500`
snapshot when offline. Symbols everywhere are in the IBKR/storage CANONICAL
spelling (dots -> dashes: BRK.B -> BRK-B, BF.B -> BF-B), so membership tests
compare apples to apples.
"""

import json
import re
import urllib.request
from datetime import date
from pathlib import Path

SP500 = [
    "MMM", "AOS", "ABT", "ABBV", "ACN", "ADBE", "AMD", "AES", "AFL", "A",
    "APD", "ABNB", "AKAM", "ALB", "ARE", "ALGN", "ALLE", "LNT", "ALL", "GOOGL",
    "GOOG", "MO", "AMZN", "AMCR", "AEE", "AEP", "AXP", "AIG", "AMT", "AWK",
    "AMP", "AME", "AMGN", "APH", "ADI", "ANSS", "AON", "APA", "AAPL", "AMAT",
    "APTV", "ACGL", "ADM", "ANET", "AJG", "AIZ", "T", "ATO", "ADSK", "ADP",
    "AZO", "AVB", "AVY", "AXON", "BKR", "BALL", "BAC", "BAX", "BDX", "BRK-B",
    "BBY", "TECH", "BIIB", "BLK", "BX", "BK", "BA", "BKNG", "BWA", "BSX",
    "BMY", "AVGO", "BR", "BRO", "BF-B", "BLDR", "BG", "BXP", "CHRW", "CDNS",
    "CZR", "CPT", "CPB", "COF", "CAH", "KMX", "CCL", "CARR", "CTLT", "CAT",
    "CBOE", "CBRE", "CDW", "CE", "COR", "CNC", "CNP", "CF", "CHRD", "CRL",
    "SCHW", "CHTR", "CVX", "CMG", "CB", "CHD", "CI", "CINF", "CTAS", "CSCO",
    "C", "CFG", "CLX", "CME", "CMS", "KO", "CTSH", "CL", "CMCSA", "CAG",
    "COP", "ED", "STZ", "CEG", "COO", "CPRT", "GLW", "CPAY", "CTVA", "CSGP",
    "COST", "CTRA", "CRWD", "CCI", "CSX", "CMI", "CVS", "DHR", "DRI", "DVA",
    "DAY", "DECK", "DE", "DELL", "DAL", "DVN", "DXCM", "FANG", "DLR", "DFS",
    "DG", "DLTR", "D", "DPZ", "DOV", "DOW", "DHI", "DTE", "DUK", "DD",
    "EMN", "ETN", "EBAY", "ECL", "EIX", "EW", "EA", "ELV", "EMR", "ENPH",
    "ETR", "EOG", "EPAM", "EQT", "EFX", "EQIX", "EQR", "ERIE", "ESS", "EL",
    "EG", "EVRG", "ES", "EXC", "EXPE", "EXPD", "EXR", "XOM", "FFIV", "FDS",
    "FICO", "FAST", "FRT", "FDX", "FIS", "FITB", "FSLR", "FE", "FI", "FMC",
    "F", "FTNT", "FTV", "FOXA", "FOX", "BEN", "FCX", "GRMN", "IT", "GE",
    "GEHC", "GEV", "GEN", "GNRC", "GD", "GIS", "GM", "GPC", "GILD", "GPN",
    "GL", "GDDY", "GS", "HAL", "HIG", "HAS", "HCA", "DOC", "HSIC", "HSY",
    "HES", "HPE", "HLT", "HOLX", "HD", "HON", "HRL", "HST", "HWM", "HPQ",
    "HUBB", "HUM", "HBAN", "HII", "IBM", "IEX", "IDXX", "ITW", "INCY", "IR",
    "PODD", "INTC", "ICE", "IFF", "IP", "IPG", "INTU", "ISRG", "IVZ", "INVH",
    "IQV", "IRM", "JBHT", "JBL", "JKHY", "J", "JNJ", "JCI", "JPM", "JNPR",
    "K", "KVUE", "KDP", "KEY", "KEYS", "KMB", "KIM", "KMI", "KKR", "KLAC",
    "KHC", "KR", "LHX", "LH", "LRCX", "LW", "LVS", "LDOS", "LEN", "LLY",
    "LIN", "LYV", "LKQ", "LMT", "L", "LOW", "LULU", "LYB", "MTB", "MRO",
    "MPC", "MKTX", "MAR", "MMC", "MLM", "MAS", "MA", "MTCH", "MKC", "MCD",
    "MCK", "MDT", "MRK", "META", "MET", "MTD", "MGM", "MCHP", "MU", "MSFT",
    "MAA", "MRNA", "MHK", "MOH", "TAP", "MDLZ", "MPWR", "MNST", "MCO", "MS",
    "MOS", "MSI", "MSCI", "NDAQ", "NTAP", "NFLX", "NEM", "NWSA", "NWS", "NEE",
    "NKE", "NI", "NDSN", "NSC", "NTRS", "NOC", "NCLH", "NRG", "NUE", "NVDA",
    "NVR", "NXPI", "ORLY", "OXY", "ODFL", "OMC", "ON", "OKE", "ORCL", "OTIS",
    "PCAR", "PKG", "PLTR", "PANW", "PARA", "PH", "PAYX", "PAYC", "PYPL", "PNR",
    "PEP", "PFE", "PCG", "PM", "PSX", "PNW", "PNC", "POOL", "PPG", "PPL",
    "PFG", "PG", "PGR", "PLD", "PRU", "PEG", "PTC", "PSA", "PHM", "QRVO",
    "PWR", "QCOM", "DGX", "RL", "RJF", "RTX", "O", "REG", "REGN", "RF",
    "RSG", "RMD", "RVTY", "ROK", "ROL", "ROP", "ROST", "RCL", "SPGI", "CRM",
    "SBAC", "SLB", "STX", "SRE", "NOW", "SHW", "SPG", "SWKS", "SJM", "SW",
    "SNA", "SOLV", "SO", "LUV", "SWK", "SBUX", "STT", "STLD", "STE", "SYK",
    "SMCI", "SYF", "SNPS", "SYY", "TMUS", "TROW", "TTWO", "TPR", "TRGP", "TGT",
    "TEL", "TDY", "TFX", "TER", "TSLA", "TXN", "TXT", "TMO", "TJX", "TSCO",
    "TT", "TDG", "TRV", "TRMB", "TFC", "TYL", "TSN", "USB", "UBER", "UDR",
    "ULTA", "UNP", "UAL", "UPS", "URI", "UNH", "UHS", "VLO", "VTR", "VLTO",
    "VRSN", "VRSK", "VZ", "VRTX", "VTRS", "VICI", "V", "VST", "VMC", "WRB",
    "GWW", "WAB", "WBA", "WMT", "DIS", "WBD", "WM", "WAT", "WEC", "WFC",
    "WELL", "WST", "WDC", "WY", "WMB", "WTW", "WYNN", "XEL", "XYL", "YUM",
    "ZBRA", "ZBH", "ZTS",
]


def sp500_set():
    """The bundled constituents as a set of canonical symbols (deduped)."""
    return set(SP500)


# --- live (current) constituents -------------------------------------------

_DATAHUB = ("https://raw.githubusercontent.com/datasets/"
            "s-and-p-500-companies/{branch}/data/constituents.csv")
_WIKI = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_UA = {"User-Agent": "Mozilla/5.0 (data-bank-tool; sp500 constituents)"}


class Sp500FetchError(Exception):
    """No online source yielded a plausible current constituent list."""


def _canon(t):
    """Storage-canonical spelling: upper, dots -> dashes (BRK.B -> BRK-B)."""
    return t.strip().upper().replace(".", "-")


def _http(url, timeout, opener):
    from fetch_ibkr_bridge import refuse_a2
    refuse_a2()
    req = urllib.request.Request(url, headers=_UA)
    return (opener or urllib.request.urlopen)(req, timeout=timeout) \
        .read().decode("utf-8", "replace")


def _validate(syms):
    """Keep ticker-shaped symbols, dedupe, and sanity-check the count looks like
    a real index (so a broken parse falls back instead of returning garbage)."""
    keep = [s for s in syms if re.fullmatch(r"[A-Z][A-Z0-9-]{0,6}", s or "")]
    out = sorted(dict.fromkeys(keep))
    if not (400 <= len(out) <= 600):
        raise Sp500FetchError(f"got {len(out)} symbols — not a plausible S&P 500")
    return out


def _from_datahub(timeout, opener):
    last = None
    for branch in ("main", "master"):
        try:
            raw = _http(_DATAHUB.format(branch=branch), timeout, opener)
            rows = [ln for ln in raw.splitlines() if ln.strip()]
            return _validate([_canon(r.split(",")[0]) for r in rows[1:]])
        except Sp500FetchError:
            raise
        except Exception as exc:  # noqa: BLE001 — try the other branch
            last = exc
    raise Sp500FetchError(f"datahub unreachable ({last})")


def _from_wikipedia(timeout, opener):
    html = _http(_WIKI, timeout, opener)
    tbl = html.split('id="constituents"', 1)[-1].split("</table>", 1)[0]
    syms = re.findall(r'<td>\s*<a[^>]*>([A-Z][A-Z0-9.\-]{0,6})</a>', tbl)
    return _validate([_canon(s) for s in syms])


def fetch_current_sp500(timeout=15, _opener=None):
    """Fetch the CURRENT constituents (canonical tickers) online: datahub CSV
    first, Wikipedia second. Returns (sorted_tickers, source). Raises
    Sp500FetchError if neither yields a plausible list. `_opener` is a test seam."""
    errs = []
    for name, fn in (("datahub", _from_datahub), ("wikipedia", _from_wikipedia)):
        try:
            return fn(timeout, _opener), name
        except Exception as exc:  # noqa: BLE001 — try the next source
            errs.append(f"{name}: {exc}")
    raise Sp500FetchError("; ".join(errs))


def current_sp500(cache_path=None, timeout=15, _opener=None, _today=None):
    """Best-effort CURRENT list: live fetch -> on-disk cache -> bundled snapshot.
    Returns (tickers, source, asof) where source is
    'datahub'|'wikipedia'|'cache'|'bundled' and asof is an ISO date or None. A
    successful live fetch is cached to `cache_path` (best-effort) so a later
    offline click still gets a recent list before falling back to the snapshot."""
    try:
        tickers, src = fetch_current_sp500(timeout=timeout, _opener=_opener)
        asof = _today or date.today().isoformat()
        if cache_path:
            try:
                Path(cache_path).write_text(
                    json.dumps({"asof": asof, "source": src, "tickers": tickers}),
                    encoding="utf-8")
            except OSError:
                pass
        return tickers, src, asof
    except Exception:  # noqa: BLE001 — offline / source down: degrade gracefully
        if cache_path:
            try:
                d = json.loads(Path(cache_path).read_text(encoding="utf-8"))
                t = d.get("tickers")
                if isinstance(t, list) and 400 <= len(t) <= 600:
                    return sorted(dict.fromkeys(t)), "cache", d.get("asof")
            except (OSError, ValueError):
                pass
        return list(SP500), "bundled", None
