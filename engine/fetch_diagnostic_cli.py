"""Process-status presentation for held diagnostic command-line entry points.

Request-owning roots keep their original exception and attached ledger evidence;
only the CLI boundary converts a typed terminal stop to a nonzero process code.
"""

import sys

from fetch_authority import AuthorityError
from fetch_ledger import LedgerError
from fetch_run_context import RequestCancelled

TERMINAL = (AuthorityError, LedgerError, RequestCancelled)
TERMINAL_EXIT = 3


def terminal_exit(exc):
    if not isinstance(exc, TERMINAL):
        raise TypeError("diagnostic CLI received a nonterminal exception")
    print(f"DIAGNOSTIC TERMINAL: {type(exc).__name__}: {exc}", file=sys.stderr)
    return TERMINAL_EXIT
