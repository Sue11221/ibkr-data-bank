"""Pure S&P parsers/UNVERIFIED values plus real default-held roots.

The former admitted CSV/fallback/cache assertions now run through both real
roots in the existing HttpLabels capability owner: the A64 membership success,
provider-exhaustion, terminal, durability and cache-provenance cases. This
corpus-only suite cannot mint a test policy or patch away production admission.
"""
from contextlib import ExitStack
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import urllib.request
from unittest.mock import patch


def deny(*args, **kwargs):
    raise AssertionError('sp500 parser suite attempted an external effect')


def tripwires():
    stack = ExitStack()
    targets = [(socket, 'getaddrinfo'), (socket, 'create_connection'),
        (socket.socket, 'connect'), (socket.socket, 'connect_ex'),
        (socket.socket, 'sendto'), (urllib.request, 'urlopen'),
        (urllib.request.OpenerDirector, 'open'), (http.client.HTTPConnection, 'connect'),
        (http.client.HTTPSConnection, 'connect'), (subprocess, 'Popen'), (os, 'system')]
    if hasattr(os, 'startfile'):
        targets.append((os, 'startfile'))
    for owner, name in targets:
        stack.enter_context(patch.object(owner, name, deny))
    return stack


sys.path.insert(0, str(Path(__file__).resolve().parent))
with tripwires():
    import sp500
    from fetch_run_context import RequestRefused

_P, _F = [0], [0]


def check(value, name):
    if value:
        _P[0] += 1
    else:
        _F[0] += 1
        print('  FAIL:', name)


CSV = ('Symbol,Security,GICS Sector\n' + '\n'.join(
    f'{symbol},Co {symbol},Tech' for symbol in
    ['MMM', 'AAPL', 'BRK.B', 'BF.B', 'COIN'] + [f'T{i}' for i in range(500)]))


def test_canon():
    check(sp500._canon('brk.b') == 'BRK-B' and sp500._canon(' BF.B ') == 'BF-B',
          '_canon upper + dots->dashes')


def test_validate():
    try:
        sp500._validate(['AAPL', 'MSFT'])
        rejected = False
    except sp500.Sp500FetchError:
        rejected = True
    check(rejected, '_validate rejects an implausibly short list')
    large = sp500._validate(['AAPL', 'AAPL', 'BRK-B'] + [f'T{i}' for i in range(500)])
    check(len(large) == len(set(large)), '_validate dedupes')
    check(large == sorted(large), '_validate deterministic sorting')


def test_datahub_parse():
    symbols = sp500.parse_datahub(CSV)
    check({'BRK-B', 'BF-B', 'COIN'} <= set(symbols), 'pure CSV canonicalized')
    check(400 <= len(symbols) <= 600, 'pure CSV plausible size')


def test_wikipedia_parse():
    wiki = ('<table id="constituents">' + ''.join(
        f'<tr><td><a href=x>{symbol}</a></td><td>Co</td></tr>' for symbol in
        ['MMM', 'AAPL', 'BRK.B'] + [f'W{i}' for i in range(500)]) + '</table>')
    symbols = sp500.parse_wikipedia(wiki)
    check('BRK-B' in symbols and 400 <= len(symbols) <= 600, 'pure Wikipedia plausible/canonical')


def test_unverified_values():
    symbols = sp500.parse_datahub(CSV)
    legacy = json.dumps(dict(tickers=symbols, asof='2026-06-24', source='datahub')).encode()
    check(sp500.offline_cache_value(legacy) == (symbols, 'cache', '2026-06-24'),
          'pure legacy value is cache, not newly verified membership')
    check(sp500.offline_cache_value(None) == (sp500.SP500, 'bundled', None),
          'pure absent cache is bundled')
    check(sp500.offline_cache_value(b'{broken') == (sp500.SP500, 'bundled', None),
          'pure corrupt cache is bundled')
    invalid_date = json.dumps(dict(tickers=symbols, asof='2026-99-99')).encode()
    check(sp500.offline_cache_value(invalid_date) == (symbols, 'cache', None),
          'invalid legacy date cannot claim freshness')


def test_default_hold():
    with tempfile.TemporaryDirectory(prefix='sp500-parser-') as directory:
        cache = Path(directory) / 'warm.json'
        warm = json.dumps(dict(tickers=list(sp500.SP500), asof='2026-08-04')).encode()
        cache.write_bytes(warm)
        for root in (sp500.current_sp500, sp500.fetch_current_sp500):
            refused = False
            with patch.object(sp500, '_read', deny):
                try:
                    args = dict(_opener=deny, _clock=deny, _evidence_dir=Path(directory) / 'ledgers')
                    if root is sp500.current_sp500:
                        args['cache_path'] = cache
                    root(**args)
                except RequestRefused as error:
                    refused = 'not activated' in str(error)
            check(refused and cache.read_bytes() == warm and not (Path(directory) / 'ledgers').exists(),
                  root.__name__ + ' held before effects despite warm cache')


def main():
    with tripwires():
        for name, test in sorted(globals().items()):
            if name.startswith('test_'):
                test()
    total = _P[0] + _F[0]
    print(f'\nsp500_selftest: {_P[0]}/{total} passed, {_F[0]} failed')
    return int(bool(_F[0]))


if __name__ == '__main__':
    raise SystemExit(main())
