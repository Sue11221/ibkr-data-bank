"""Portable source-segment custody for mutation-reference harnesses.

The helpers in this module deliberately hash normalized source text rather than
``ast.dump`` output.  AST dumps are interpreter-version-sensitive; source
segments are stable across Python versions and Windows/POSIX checkout EOLs.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
from typing import Iterable


@dataclass(frozen=True)
class FunctionSpan:
    """A decorator-inclusive function span in normalized source text."""

    qualname: str
    start_line: int
    end_line: int


@dataclass(frozen=True)
class ModuleStatementSpan:
    """A stable, named top-level statement span in normalized source text."""

    custody_key: str
    start_line: int
    end_line: int


class SourceCustodyError(ValueError):
    """The source or mutation anchors cannot establish unambiguous custody."""


class _FunctionCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.scope: list[str] = []
        self.spans: list[FunctionSpan] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def _visit_function(
            self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        start_line = min(
            [node.lineno]
            + [decorator.lineno for decorator in node.decorator_list]
        )
        qualname = ".".join((*self.scope, node.name))
        self.spans.append(FunctionSpan(
            qualname=qualname,
            start_line=start_line,
            end_line=node.end_lineno,
        ))
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)


def normalize_source_bytes(payload: bytes) -> str:
    """Decode UTF-8 and normalize CRLF or lone CR line endings to LF."""

    return payload.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")


def function_spans(source: str) -> tuple[FunctionSpan, ...]:
    """Return every function/method/nested-function span with a qualname."""

    if "\r" in source:
        raise SourceCustodyError("source must be EOL-normalized before custody")
    collector = _FunctionCollector()
    collector.visit(ast.parse(source))
    return tuple(collector.spans)


def _bound_names(target: ast.expr) -> tuple[str, ...]:
    if isinstance(target, ast.Name):
        return (target.id,)
    if isinstance(target, (ast.List, ast.Tuple)):
        return tuple(
            name
            for item in target.elts
            for name in _bound_names(item)
        )
    return ()


def _module_statement_key(node: ast.stmt) -> str | None:
    names: tuple[str, ...]
    if isinstance(node, ast.Assign):
        names = tuple(
            name
            for target in node.targets
            for name in _bound_names(target)
        )
    elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
        names = _bound_names(node.target)
    elif isinstance(node, ast.Import):
        names = tuple(
            alias.asname or alias.name.split(".", 1)[0]
            for alias in node.names
        )
    elif isinstance(node, ast.ImportFrom):
        names = tuple(alias.asname or alias.name for alias in node.names)
    else:
        return None
    if not names:
        return None
    return "<module>:" + ",".join(names)


def module_statement_spans(source: str) -> tuple[ModuleStatementSpan, ...]:
    """Return stable spans for custodiable top-level bindings/imports.

    A module-level mutation outside these explicitly named statement forms
    fails closed.  Line-number keys are deliberately forbidden because an
    unrelated insertion above the statement must not require re-pinning.
    """

    if "\r" in source:
        raise SourceCustodyError("source must be EOL-normalized before custody")
    tree = ast.parse(source)
    return tuple(
        ModuleStatementSpan(
            custody_key=key,
            start_line=node.lineno,
            end_line=node.end_lineno,
        )
        for node in tree.body
        if (key := _module_statement_key(node)) is not None
    )


def _changed_line_span(source: str, old: str, new: str) -> tuple[int, int]:
    count = source.count(old)
    if count != 1:
        raise SourceCustodyError(
            f"mutation anchor count is {count}, expected 1: {old[:120]!r}")

    prefix = 0
    prefix_limit = min(len(old), len(new))
    while prefix < prefix_limit and old[prefix] == new[prefix]:
        prefix += 1

    suffix = 0
    suffix_limit = min(len(old) - prefix, len(new) - prefix)
    while suffix < suffix_limit and old[-suffix - 1] == new[-suffix - 1]:
        suffix += 1

    anchor_start = source.index(old)
    changed_start = anchor_start + prefix
    changed_end = anchor_start + len(old) - suffix
    # Insert-only changes have no replaced original character.  An append to
    # the uniquely matched anchor belongs to that anchor, including when the
    # anchor ends with LF (for example ``import x\n`` -> ``import x\nimport
    # y\n``).  A prepend belongs to the anchor's first character; an insertion
    # inside the anchor belongs to its exact source location.
    if changed_start == changed_end and old:
        if prefix == len(old):
            final_character = anchor_start + len(old) - 1
            if source[final_character] == "\n" and final_character > anchor_start:
                final_character -= 1
        elif prefix == 0:
            final_character = anchor_start
        else:
            final_character = changed_start
        first_character = final_character
    else:
        first_character = changed_start
        final_character = max(changed_start, changed_end - 1)
    start_line = source.count("\n", 0, first_character) + 1
    end_line = source.count("\n", 0, final_character) + 1
    return start_line, end_line


def mutation_owner(source: str, old: str, new: str) -> str:
    """Return the smallest named source segment owning changed text.

    Function bodies (including decorators) take precedence.  A mutation at
    module scope is assigned to its enclosing named top-level binding/import,
    such as ``<module>:_LEASE_RE``.  Unsupported or ambiguous module-level
    statements fail closed instead of silently losing custody.
    """

    return _mutation_owner_from_all_spans(
        source,
        old,
        new,
        function_spans(source),
        module_statement_spans(source),
    )


def _mutation_owner_from_spans(
        source: str, old: str, new: str,
        spans: tuple[FunctionSpan, ...]) -> str | None:
    start_line, end_line = _changed_line_span(source, old, new)
    candidates = [
        span for span in spans
        if span.start_line <= start_line and end_line <= span.end_line
    ]
    if not candidates:
        return None
    owner = min(
        candidates,
        key=lambda span: (span.end_line - span.start_line, -span.start_line),
    )
    return owner.qualname


def _mutation_owner_from_all_spans(
        source: str, old: str, new: str,
        functions: tuple[FunctionSpan, ...],
        statements: tuple[ModuleStatementSpan, ...]) -> str:
    function_owner = _mutation_owner_from_spans(
        source, old, new, functions)
    if function_owner is not None:
        return function_owner

    start_line, end_line = _changed_line_span(source, old, new)
    candidates = [
        span for span in statements
        if span.start_line <= start_line and end_line <= span.end_line
    ]
    if not candidates:
        raise SourceCustodyError(
            "module-level mutation owns no supported top-level source segment: "
            f"lines {start_line}-{end_line}"
        )
    if len(candidates) > 1:
        raise SourceCustodyError(
            "module-level mutation source segment is ambiguous: "
            f"{sorted(span.custody_key for span in candidates)}"
        )
    return candidates[0].custody_key


def mutation_owners(
        source: str, mutations: Iterable[tuple[str, str]]) -> frozenset[str]:
    """Derive every exact function or module-statement mutation owner."""

    functions = function_spans(source)
    statements = module_statement_spans(source)
    owners = {
        _mutation_owner_from_all_spans(
            source, old, new, functions, statements)
        for old, new in mutations
    }
    if not owners:
        raise SourceCustodyError("mutation set owns no source segments")
    return frozenset(owners)


def function_segment_sha256(
        source: str, qualnames: Iterable[str]) -> dict[str, str]:
    """Hash named function or module-statement source segments.

    The historical function name remains for harness compatibility.  AST line
    numbers count LF characters only, so splitting must use literal LF rather
    than :meth:`str.splitlines`, which also treats form-feed and other Unicode
    separators as line boundaries.
    """

    all_spans = tuple(
        (span.qualname, span.start_line, span.end_line)
        for span in function_spans(source)
    ) + tuple(
        (span.custody_key, span.start_line, span.end_line)
        for span in module_statement_spans(source)
    )
    requested = frozenset(qualnames)
    matches = {
        name: [span for span in all_spans if span[0] == name]
        for name in requested
    }
    missing = sorted(name for name, found in matches.items() if not found)
    if missing:
        raise SourceCustodyError(f"source segments missing: {missing}")
    ambiguous = sorted(name for name, found in matches.items() if len(found) > 1)
    if ambiguous:
        raise SourceCustodyError(
            f"requested source segments are ambiguous: {ambiguous}")
    spans = {name: found[0] for name, found in matches.items()}
    lines = source.split("\n")
    return {
        name: hashlib.sha256(
            ("\n".join(
                lines[spans[name][1] - 1:spans[name][2]]
            ) + "\n").encode("utf-8")
        ).hexdigest()
        for name in sorted(requested)
    }
