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
import hashlib
import math
import os
import sys
import threading
import time
from datetime import date
from urllib.error import HTTPError, URLError
from pathlib import Path

from fetch_authority import (AuthorityError, ResponseQuarantined, canonical_bytes,
                             digest_value, strict_json)
from fetch_ledger import LedgerError, inspect_ledger
from fetch_run_context import RequestCancelled, RequestRefused
import fetch_operations as fops

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


_HTTP_MISSING = object()


def _http(worker=_HTTP_MISSING, producer=_HTTP_MISSING, branch=_HTTP_MISSING,
          timeout=_HTTP_MISSING, opener=_HTTP_MISSING, cancel=_HTTP_MISSING,
          provenance=_HTTP_MISSING):
    if (any(value is _HTTP_MISSING for value in
            (worker, producer, branch, timeout, opener, cancel, provenance))
            or type(cancel) is not _MembershipCheck or type(provenance) is not dict):
        raise RequestRefused('membership helper requires explicit owned check and capture')
    from fetch_http_membership import guarded_membership_attempt
    return guarded_membership_attempt(worker, producer, branch, opener,
        timeout=timeout, cancel=cancel, _provenance=provenance,
        _root_deadline=cancel.deadline)


def _validate(syms):
    """Keep ticker-shaped symbols, dedupe, and sanity-check the count looks like
    a real index (so a broken parse falls back instead of returning garbage)."""
    keep = [s for s in syms if re.fullmatch(r"[A-Z][A-Z0-9-]{0,6}", s or "")]
    out = sorted(dict.fromkeys(keep))
    if not (400 <= len(out) <= 600):
        raise Sp500FetchError(f"got {len(out)} symbols — not a plausible S&P 500")
    return out


def parse_datahub(raw):
    """Pure CSV parser; runtime calls it inside the durable logical executor."""
    if type(raw) is not str:
        raise Sp500FetchError('CSV input is not text')
    rows = [line for line in raw.splitlines() if line.strip()]
    return _validate([_canon(row.split(',')[0]) for row in rows[1:]])


def parse_wikipedia(html):
    """Pure historical constituents-table parser, with the same plausibility bar."""
    if type(html) is not str:
        raise Sp500FetchError('HTML input is not text')
    table = html.split('id="constituents"', 1)[-1].split('</table>', 1)[0]
    symbols = re.findall(r'<td>\s*<a[^>]*>([A-Z][A-Z0-9.\-]{0,6})</a>', table)
    return _validate([_canon(symbol) for symbol in symbols])


def _provider_failure(error):
    """Only ordinary provider failures may choose another source or offline data."""
    if isinstance(error, (AuthorityError, LedgerError, RequestCancelled, ResponseQuarantined)):
        raise error
    if isinstance(error, HTTPError):
        return type(error.code) is int and 400 <= error.code <= 599
    if isinstance(error, URLError):
        reason = error.reason
        if isinstance(reason, (AuthorityError, LedgerError, RequestCancelled, ResponseQuarantined)):
            raise reason from error
        return isinstance(reason, (OSError, TimeoutError))
    return isinstance(error, (Sp500FetchError, TimeoutError, OSError))


def _from_datahub(worker, timeout, opener, check, provenance):
    for branch in ("main", "master"):
        check.check()
        try:
            return _http(worker, 'http.sp500.github', branch,
                         min(timeout, check.remaining()), opener, check, provenance)
        except Sp500FetchError:
            # Preserve the old parser-failure distinction: no master parse retry.
            raise
        except Exception as error:
            if not _provider_failure(error):
                raise
    raise Sp500FetchError('datahub unavailable after two durable attempts')


def _from_wikipedia(worker, timeout, opener, check, provenance):
    check.check()
    return _http(worker, 'http.sp500.wikipedia', 'wikipedia',
                 min(timeout, check.remaining()), opener, check, provenance)


def _fetch_membership_body(worker, timeout, opener, check, provenance):
    """Private child body: never enters the other public operation root."""
    for name, fn in (("datahub", _from_datahub), ("wikipedia", _from_wikipedia)):
        try:
            return fn(worker, timeout, opener, check, provenance), name
        except Exception as error:
            if not _provider_failure(error):
                raise
    raise Sp500FetchError('no provider returned plausible membership')


_CACHE_BYTES = 1024 * 1024
_LEDGER_BYTES = 4 * 1024 * 1024
_ROOT_BUDGET_SECONDS = 360.0
_RIGHTS = frozenset({'http.sp500.github', 'http.sp500.wikipedia'})
_LOCKS_GUARD = threading.Lock()
_PATH_LOCKS = {}


def _directory(value):
    if value is not None and type(value) not in (str, type(Path('.'))):
        raise RequestRefused('membership evidence directory must be a detached path')
    return (Path(value) if value is not None else
            Path(__file__).resolve().parents[1] / '_ingest_reports' / 'fetch-ledgers')


class _MembershipCheck:
    def __init__(self, operation, cancel, seconds):
        if (type(seconds) not in (int, float) or not math.isfinite(seconds)
                or not 0 < seconds <= _ROOT_BUDGET_SECONDS):
            raise RequestRefused('invalid membership operation budget')
        self.operation, self.cancel = operation, cancel
        self.deadline = time.monotonic() + seconds
        context = fops.require_root_admission(operation)
        self.identity = (context.operation_id, context.captured_now.isoformat(),
                         context.authority.fingerprint, context.authority.table_digest,
                         context.authority.consensus_digest)

    def remaining(self):
        value = self.deadline - time.monotonic()
        if value <= 0:
            raise RequestRefused('membership operation deadline expired')
        return value

    def is_set(self):
        from fetch_ibkr_bridge import without_authority
        self.remaining()
        if self.cancel is not None and without_authority(self.cancel.is_set)():
            raise RequestCancelled('membership operation cancelled')
        return False

    def check(self, *, sealed=False):
        context = fops.require_root_admission(self.operation)
        identity = (context.operation_id, context.captured_now.isoformat(),
                    context.authority.fingerprint, context.authority.table_digest,
                    context.authority.consensus_digest)
        if identity != self.identity:
            raise AuthorityError('membership captured authority identity changed')
        if not sealed:
            context.ledger.require_live()
        self.is_set()
        return context

    def publication_check(self, context, candidate):
        """Check detached sealed evidence AFTER cleanup; never restore a handle.

        The closed operation cannot send or make a child. Publication checks only
        the same captured identity, intact confined policy and verified seal.
        """
        from fetch_test_policy import capability_allows
        identity = (context.operation_id, context.captured_now.isoformat(),
                    context.authority.fingerprint, context.authority.table_digest,
                    context.authority.consensus_digest)
        if (identity != self.identity or not context.ledger._closed
                or not context.ledger._file.closed):
            raise AuthorityError('membership cleanup or authority identity differs')
        for producer, endpoint in (('http.sp500.github', 'sp500.github'),
                                   ('http.sp500.wikipedia', 'sp500.wikipedia')):
            if not capability_allows(context.test_capability, producer, 'http-metadata', endpoint):
                raise AuthorityError('membership publication policy expired')
        self.is_set()
        _read(context.ledger.path, _LEDGER_BYTES)
        audit = inspect_ledger(context.ledger.path)
        if not audit['verified'] or audit['operation_id'] != context.operation_id:
            raise LedgerError('membership closed ledger is not verified')
        if candidate is not None and not _cache_valid(candidate, context):
            raise LedgerError('membership detached candidate provenance differs')


def _read(path, maximum, *, missing=False):
    try:
        with Path(path).open('rb') as stream:
            raw = stream.read(maximum + 1)
    except FileNotFoundError:
        if missing:
            return None
        raise
    if len(raw) > maximum:
        raise LedgerError('membership evidence exceeds its bounded size')
    return raw


def _save(path, value):
    from stock_storage import _atomic_write_bytes
    raw = canonical_bytes(value) + b'\n'
    if len(raw) > _CACHE_BYTES:
        raise LedgerError('membership companion exceeds its bounded size')
    try:
        _atomic_write_bytes(Path(path), raw)
        if _read(path, _CACHE_BYTES) != raw:
            raise LedgerError('membership companion verification differs')
    except OSError as error:
        raise LedgerError('membership companion persistence failed') from error


def _path_lock(path):
    key = str(Path(path).resolve())
    with _LOCKS_GUARD:
        return _PATH_LOCKS.setdefault(key, threading.Lock())


def _restore_publication(cache, old, candidate):
    """Rollback only our exact bytes under the caller's publication lock.

    A foreign writer is never overwritten. Atomic replacement plus this
    expected-content check is process-coordinated CAS, not a claim that the
    filesystem offers a cross-process compare-and-swap primitive.
    """
    current = _read(cache, _CACHE_BYTES, missing=True)
    if current == old:
        return False  # The failed replace left the previous state intact.
    if current != candidate:
        raise LedgerError('membership rollback refuses competing cache content')
    if old is None:
        # This invocation created the file. No pre-existing item is removed.
        cache.unlink()
    else:
        from stock_storage import _atomic_write_bytes
        _atomic_write_bytes(cache, old)
    if _read(cache, _CACHE_BYTES, missing=True) != old:
        raise LedgerError('membership rollback verification differs')
    return True


def _sealed_identity(context, provenance, tickers):
    """Join accepted metadata to its actual durable decision/result and seal."""
    if _read(context.ledger.path, _LEDGER_BYTES) is None:
        raise LedgerError('membership ledger missing')
    audit = inspect_ledger(context.ledger.path)
    matches = [event for event in audit['events'] if event['event'] == 'result'
               and event['logical_id'] == provenance['logical_id']
               and event['worker_id'] == provenance['worker_id']]
    if (not audit['verified'] or audit['operation_id'] != provenance['operation_id']
            or len(matches) != 1 or matches[0]['payload'].get('outcome') != 'returned'
            or matches[0]['payload'].get('response_digest') != digest_value(tickers)
            or matches[0]['payload'].get('raw_response_digest') != provenance['raw_digest']):
        raise LedgerError('membership accepted result is not bound to its seal')
    result = matches[0]
    decisions = [event for event in audit['events'] if event['event'] == 'decision'
                 and event['attempt_id'] == result['attempt_id']]
    from fetch_http_membership import _spec, verified_membership_attempt_evidence
    wire = provenance['request_provenance']
    envelope, evidence = _spec(wire['producer'], wire['branch'])
    expected = verified_membership_attempt_evidence(context, wire['producer'], envelope, evidence)
    if (len(decisions) != 1 or wire != expected
            or decisions[0]['logical_id'] != result['logical_id']
            or decisions[0]['worker_id'] != result['worker_id']
            or decisions[0]['producer_id'] != result['producer_id']
            or result['producer_id'] != wire['producer']
            or decisions[0]['payload'].get('captured_now') != context.captured_now.isoformat()
            or decisions[0]['payload'].get('http_request') != wire
            or decisions[0]['payload'].get('authority_fingerprint') != context.authority.fingerprint
            or decisions[0]['payload'].get('table_digest') != context.authority.table_digest
            or decisions[0]['payload'].get('consensus_digest') != context.authority.consensus_digest):
        raise LedgerError('membership decision authority or provenance differs')
    return dict(provenance, attempt_id=result['attempt_id'],
        ledger_name=context.ledger.path.name,
        seal_sha256=hashlib.sha256(_read(context.ledger.receipt_path, _CACHE_BYTES)).hexdigest(),
        authority_fingerprint=context.authority.fingerprint,
        table_digest=context.authority.table_digest,
        consensus_digest=context.authority.consensus_digest,
        captured_now=context.captured_now.isoformat())


def _cache_valid(payload, context):
    """Portable basename locator; evidence is checked against the new authority."""
    try:
        if (type(payload) is not dict or payload.get('kind') != 'sp500_membership'
                or type(payload.get('version')) is not int or payload.get('version') != 1
                or payload.get('subject') != 'SP500'
                or payload.get('source') not in ('datahub', 'wikipedia')):
            return False
        tickers, provenance = payload['tickers'], payload['provenance']
        if type(tickers) is not list or _validate(tickers) != tickers or type(provenance) is not dict:
            return False
        if (provenance['accepted_digest'] != digest_value(tickers)
                or provenance['authority_fingerprint'] != context.authority.fingerprint
                or provenance['table_digest'] != context.authority.table_digest
                or provenance['consensus_digest'] != context.authority.consensus_digest
                or payload['asof'] != provenance['captured_now'][:10]
                or payload['asof'] != context.captured_now.date().isoformat()):
            return False
        name = provenance['ledger_name']
        if type(name) is not str or not re.fullmatch(r'fetch-[0-9]{8}-[0-9]{6}\.jsonl', name):
            return False
        path = context.ledger.path.parent / name
        _read(path, _LEDGER_BYTES)
        audit = inspect_ledger(path)
        receipt = _read(path.with_suffix('.seal.json'), _CACHE_BYTES)
        if (not audit['verified'] or audit['operation_id'] != provenance['operation_id']
                or hashlib.sha256(receipt).hexdigest() != provenance['seal_sha256']):
            return False
        decisions = [event for event in audit['events'] if event['event'] == 'decision'
                     and event['attempt_id'] == provenance['attempt_id']]
        results = [event for event in audit['events'] if event['event'] == 'result'
                   and event['attempt_id'] == provenance['attempt_id']]
        if len(decisions) != 1 or len(results) != 1:
            return False
        decision, result = decisions[0], results[0]
        from fetch_http_membership import _spec, verified_membership_attempt_evidence
        wire = provenance['request_provenance']
        envelope, evidence = _spec(wire['producer'], wire['branch'])
        expected = verified_membership_attempt_evidence(context, wire['producer'], envelope, evidence)
        source = 'datahub' if wire['producer'] == 'http.sp500.github' else 'wikipedia'
        return (payload['source'] == source and wire == expected
                and decision['producer_id'] == result['producer_id'] == wire['producer']
                and decision['payload']['http_request'] == wire
                and decision['logical_id'] == result['logical_id'] == provenance['logical_id']
                and decision['worker_id'] == result['worker_id'] == provenance['worker_id']
                and result['payload'].get('outcome') == 'returned'
                and result['payload'].get('response_digest') == provenance['accepted_digest']
                and result['payload'].get('raw_response_digest') == provenance['raw_digest']
                and decision['payload']['captured_now'] == provenance['captured_now']
                and decision['payload']['authority_fingerprint'] == context.authority.fingerprint
                and decision['payload']['table_digest'] == context.authority.table_digest
                and decision['payload']['consensus_digest'] == context.authority.consensus_digest)
    except (KeyError, TypeError, ValueError, Sp500FetchError, AuthorityError, LedgerError, OSError):
        # Old cached evidence is not the new operation's ledger or authority.
        # A malformed previous cache has no verified-current membership credit.
        return False


def offline_cache_value(raw):
    """Pure unverified legacy/value parsing; no clock, authority or filesystem."""
    if type(raw) is bytes and len(raw) <= _CACHE_BYTES:
        try:
            payload = strict_json(raw)
            if type(payload) is dict and type(payload.get('tickers')) is list:
                tickers = _validate(payload['tickers'])
                asof = payload.get('asof')
                if type(asof) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', asof):
                    asof = None
                else:
                    try:
                        if date.fromisoformat(asof).isoformat() != asof:
                            asof = None
                    except ValueError:
                        asof = None
                return tickers, 'cache', asof
        except (AuthorityError, ValueError, TypeError, Sp500FetchError):
            pass
    return list(SP500), 'bundled', None


def _offline_value(raw, context):
    value = offline_cache_value(raw)
    verified = value[1] == 'cache' and _cache_valid(strict_json(raw), context)
    return value, verified


def _close_membership_child(child):
    """Join cleanup is terminal, but cannot replace an existing primary stop."""
    primary = sys.exc_info()[1]
    try:
        child.close()
    except BaseException as close_error:
        if primary is not None:
            BaseException.add_note(primary, 'membership child cleanup also failed')
            return
        if isinstance(close_error, (AuthorityError, LedgerError, RequestCancelled,
                                    ResponseQuarantined)) or not isinstance(close_error, Exception):
            raise
        raise LedgerError('membership child cleanup failed') from close_error


def _run_membership(operation, lifecycle, *, cache_path=None, timeout=15, opener=None,
                    cancel=None, budget_seconds=_ROOT_BUDGET_SECONDS, use_cache=False):
    context = fops.require_root_admission(operation)
    provenance, failure, candidate = {}, None, None
    companion = context.ledger.path.with_suffix('.root.json')
    report = dict(kind='sp500_membership_operation', version=1,
        operation_id=context.operation_id, ledger_name=context.ledger.path.name,
        captured_now=context.captured_now.isoformat(), membership_verified=False,
        published=False, state='started')
    try:
        check = _MembershipCheck(operation, cancel, budget_seconds)
        check.check()
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise RequestRefused('membership timeout is invalid')
        if cache_path is not None and type(cache_path) not in (str, type(Path('.'))):
            raise RequestRefused('membership cache must be a detached path')
        cache = Path(cache_path) if cache_path is not None else None
        if cache is not None and cache.resolve().is_relative_to(context.ledger.path.parent.resolve()):
            raise RequestRefused('membership cache would overwrite operation evidence')
        old = _read(cache, _CACHE_BYTES, missing=True) if cache is not None else None
        parent = operation.child('membership-root', rights=_RIGHTS)
        try:
            with fops.scoped_worker(parent) as parent_worker:
                child = fops.narrow_worker_child(parent_worker, 'membership-fetch', rights=_RIGHTS)
                try:
                    with fops.scoped_worker(child) as worker:
                        tickers, source = _fetch_membership_body(worker, timeout, opener, check, provenance)
                except Sp500FetchError as error:
                    failure = error
                finally:
                    _close_membership_child(child)
        finally:
            _close_membership_child(parent)
        check.check()
        if failure is not None:
            if use_cache:
                result, prior_verified = _offline_value(old, context)
                report.update(state='offline_unverified', prior_evidence_verified=prior_verified,
                              source=result[1], asof=result[2], fallback='provider_exhaustion')
            else:
                report.update(state='provider_exhausted', source=None)
                result = None
        else:
            result = (tickers, source, context.captured_now.date().isoformat())
            report.update(state='accepted_unpublished', source=source, asof=result[2])
        operation.seal()
        check.check(sealed=True)
        if failure is None:
            identity = _sealed_identity(context, provenance, tickers)
            candidate = dict(kind='sp500_membership', version=1, subject='SP500',
                tickers=tickers, source=source, asof=result[2], provenance=identity)
            report.update(membership_verified=True, provenance=identity)
        _save(companion, report)  # A failed companion can never permit cache publication.
        try:
            operation.close()  # Cleanup is terminal and precedes any cache change.
        finally:
            lifecycle['close_attempted'] = True
        check.publication_check(context, candidate)
        report.update(state='complete' if failure is None else report['state'],
                      ledger_verified=True, publication_refusals=context.publication_refusals())
        final_companion_saved = False
        if candidate is not None and cache is not None:
            from stock_storage import _atomic_write_bytes
            raw = canonical_bytes(candidate) + b'\n'
            if len(raw) > _CACHE_BYTES:
                raise LedgerError('membership cache exceeds its bounded size')
            with _path_lock(cache):
                check.publication_check(context, candidate)
                if _read(cache, _CACHE_BYTES, missing=True) != old:
                    raise LedgerError('membership publication CAS differs')
                report['publication_uncertain'] = True
                try:
                    _atomic_write_bytes(cache, raw)
                    if _read(cache, _CACHE_BYTES) != raw:
                        raise LedgerError('membership published cache verification differs')
                    check.publication_check(context, candidate)
                    report.update(published=True, publication_uncertain=False)
                    _save(companion, report)
                    final_companion_saved = True
                except BaseException as publication_error:
                    try:
                        restored = _restore_publication(cache, old, raw)
                        report.update(published=False, publication_uncertain=False,
                                      publication_rolled_back=restored)
                    except BaseException as recovery_error:
                        report.update(publication_uncertain=True,
                                      rollback_fault_type=type(recovery_error).__name__)
                        BaseException.add_note(publication_error,
                                               'membership cache rollback could not be verified')
                    raise
        if not final_companion_saved:
            _save(companion, report)
        if failure is not None and not use_cache:
            raise failure
        return result
    except BaseException as error:
        report.update(state='failed', fault_type=type(error).__name__,
                      recovery_required=bool(report.get('publication_uncertain') or report.get('published')),
                      ledger_verified=context.ledger.verified)
        try:
            _save(companion, report)
        except BaseException:
            BaseException.add_note(error, 'membership failure companion could not be persisted')
        if isinstance(error, (AuthorityError, LedgerError, RequestCancelled, ResponseQuarantined, Sp500FetchError)):
            raise
        if not isinstance(error, Exception):
            raise
        raise LedgerError('membership root or publication failed') from error


def _close_membership(operation, lifecycle):
    if lifecycle['close_attempted']:
        return
    primary = sys.exc_info()[1]
    try:
        operation.close()
    except BaseException:
        if primary is None:
            raise
        BaseException.add_note(primary, 'membership operation cleanup also failed')


def fetch_current_sp500(timeout=15, _opener=None, *, _test_capability=None,
                        _evidence_dir=None, _authority=None, _clock=None,
                        _governors=None, _fault=None, _cancel=None,
                        _budget_seconds=_ROOT_BUDGET_SECONDS):
    """Held public root; offline acceptance retains the two-value API and companion."""
    operation = fops.begin_operation('membership', _directory(_evidence_dir),
        test_capability=_test_capability, authority=_authority, clock=_clock,
        governors=_governors, fault=_fault)
    lifecycle = {'close_attempted': False}
    try:
        result = _run_membership(operation, lifecycle, timeout=timeout, opener=_opener,
            cancel=_cancel, budget_seconds=_budget_seconds)
        return result[:2]
    finally:
        _close_membership(operation, lifecycle)


def current_sp500(cache_path=None, timeout=15, _opener=None, _today=None, *,
                  _test_capability=None, _evidence_dir=None, _authority=None,
                  _clock=None, _governors=None, _fault=None, _cancel=None,
                  _budget_seconds=_ROOT_BUDGET_SECONDS):
    """Held public root; admitted live -> explicitly unverified cache/bundled data.

    The default hold precedes cache/clock/authority/ledger effects. `_today` is
    retained solely for call compatibility; freshness uses the captured clock.
    A scoped offline opener returns raw bytes, not an unrestricted URL capability.
    """
    operation = fops.begin_operation('membership', _directory(_evidence_dir),
        test_capability=_test_capability, authority=_authority, clock=_clock,
        governors=_governors, fault=_fault)
    lifecycle = {'close_attempted': False}
    try:
        return _run_membership(operation, lifecycle, cache_path=cache_path, timeout=timeout,
            opener=_opener, cancel=_cancel, budget_seconds=_budget_seconds, use_cache=True)
    finally:
        _close_membership(operation, lifecycle)
