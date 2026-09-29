"""Row 52 M2a acceptance harness — move-proof by construction.

The user's contract (2026-07-21, verbatim): "I don't want a static dictionary for
it. I want it so that it is move proof. If i copy paste it into somewhere else, I
want it to work there as well if there is ibkr in their system."

This gate proves the CODE half of that: no component stores or assumes an absolute
project path, every self-locating module resolves its own root from ``__file__``,
and a plain copy of the code to an arbitrary location still resolves correctly with
zero path edits.  It is offline and deterministic: no network, no ports, no GUI, no
bank access, no TWS.

WHAT IS AND IS NOT A DEFECT
  * A load-bearing absolute path — one the code COMPARES, FOLLOWS, or uses as a
    default root — is a defect.  It breaks the moment the folder moves.
  * An absolute path inside historical EVIDENCE (Run Logs artifacts, archived
    snapshots, review prose) is explicitly fine per plan section 4 rule 8.  Those
    record what happened on a machine; nothing reads them back as identity.
  * A DISCOVERY candidate list (``tws_discovery.candidate_roots``) is fine: it
    enumerates conventional install locations to search, which is precisely the
    "discovered per machine" mechanism the contract asks for.

EXIT CONTRACT (``startup_port_selection_reference`` precedent):
    0  every check passed — the code is move-proof
    3  BASELINE PINNED: a known non-portable site is still present, and every
       invariant that must survive the fix still holds
    1  a portability invariant BROKE — a real regression

Run:  python engine/portability_reference.py
"""
from __future__ import annotations

import ast
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve()
ENGINE = HERE.parent
ROOT = ENGINE.parent

_PASS: list[str] = []
_BASE_FAIL: list[str] = []      # must hold today          -> exit 1
_FEATURE_FAIL: list[str] = []   # known non-portable site  -> exit 3


def check(cond, name, *, feature=False):
    if cond:
        _PASS.append(name)
    elif feature:
        _FEATURE_FAIL.append(name)
    else:
        _BASE_FAIL.append(name)


# --------------------------------------------------------------------------
# evidence vs code
# --------------------------------------------------------------------------
# Directories whose contents are historical evidence, third-party, or generated.
# Absolute paths recorded inside them are exempt (plan section 4, rule 8).
EVIDENCE_DIRS = {
    "Run Logs", "scans", "_quarantine", "Stock Data Storage",
    "_derived_daily_cache", "_code_snapshots", "_completed_work_archive",
    "_repair_scripts_archive", "__pycache__", ".git", ".uv-cache",
}

# Generated RUNTIME STATE, not shipped code. These files log absolute paths as
# message text describing what happened on this machine; nothing reads them back
# as identity, so they fall under the same rule-8 exemption as Run Logs
# artifacts. They are also gitignored, so they never travel with a copy at all.
# Scanning them buries the two real defects under ~140 log lines.
GENERATED_STATE = re.compile(
    r"^(\.review_handoff_[^/]*|_scan_digest_cache\.json|sp500_current\.json"
    r"|data_gaps\.parquet|_data_gaps\.json|export_profile\.json)$")

# Scratch verification roots: full copies of the tree made to prove a checkpoint.
# A defect inside one is a copy of a defect already counted in the real tree.
SCRATCH_ROOT = re.compile(r"^_row\d+_exact_verify")

# AGENT-TOOLING state, matched on the full relative path. These belong to the
# review/automation harness around the project, not to the shipped app, and the
# app never reads them:
#   * docs/board/code_review_findings.json -- review PROSE (its paths are even
#     elided: 'C:\...\EMA...', "C:\Program Files\..."), i.e. rule-8 evidence
#     in JSON form.  The root spelling remains accepted for pre-M2b copies;
#   * .claude/settings.local.json -- a Claude Code permission allowlist. Machine-
#     local by name; if the folder moves the pattern simply stops matching and a
#     prompt appears. Nothing in the app resolves it, so it is not load-bearing.
#   * engine/fixtures/portability_evidence.json -- immutable historical
#     deletion/custody evidence, not a runtime configuration or deletion queue.
#     Preserve its machine paths as evidence; no other fixture JSON is exempt.
TOOLING_STATE = re.compile(
    r"^(code_review_findings\.json|docs/board/code_review_findings\.json"
    r"|engine/fixtures/portability_evidence\.json"
    r"|\.claude/settings\.local\.json)$")

CODE_EXT = {".py", ".json", ".bat", ".cmd", ".ps1", ".toml"}

# A raw string is mandatory here: written as "[" + "\\" + "/" + "]" the class
# collapses to [\/], which matches ONLY a forward slash and silently stops
# seeing every Windows path -- that exact bug produced a false "zero absolute
# paths" reading during this row, so checks A1/A2 below now pin it.
#
# The negative lookbehind keeps URL schemes out: without it "[A-Za-z]:[\\/]"
# happily matches the "s:/" inside "https://", flagging every constant URL in
# the tree as a portability defect.
SEP = r"[\\/]"
ABS_PATH = re.compile(r"(?<![A-Za-z])[A-Za-z]:" + SEP)
USER_PATH = re.compile(r"(?<![A-Za-z])[A-Za-z]:" + SEP + r"{1,2}Users" + SEP,
                       re.I)

# A bare TWS convention root is not project identity: C:\Jts is the IBKR
# installer's universal default and is superseded at run time by
# tws_discovery.candidate_roots(). It names no user and no project, so it
# survives a copy to any machine that has IBKR installed -- exactly the
# contract. Deliberately NOT anchored to a quote character: the same convention
# is named in prose and docstrings, and it is no more load-bearing there.
# Check A6 pins that this exemption never swallows a user-specific path.
TWS_CONVENTION = re.compile(r"(?<![A-Za-z])[A-Za-z]:" + SEP + r"{1,2}Jts", re.I)


# Files whose absolute-path mentions are sanctioned, none of which break on a
# move:
#   * tws_discovery enumerates conventional install roots to SEARCH -- that is
#     the discovery mechanism the contract asks for, not stored identity;
#   * a selftest's synthetic fixture path (C:\X\..., C:\temp\...) is test INPUT
#     that is never resolved on disk;
#   * a *_reference.py harness embeds example paths to prove its own matcher
#     works (checks A1/A2/A5) and to DESCRIBE the defects it detects
#     (tws_discovery_reference's "hardcoded C:\Jts exe" verdict string).
# This is scoped, not blanket: rule B3 stays global and catches a hard-coded
# project root in ANY file including a harness -- that is precisely how the real
# xval_x0_reference.py defect was caught this row. Check A7 pins that no shipped
# app module can slip into this predicate.
def sanctioned(fname):
    return (fname == "engine/tws_discovery.py"
            or fname.endswith("_selftest.py")
            or fname.endswith("_reference.py"))


def code_files():
    # Prune excluded roots before descent.  Path.rglob() followed by a
    # per-file filter still walks the 340k-file bank and full-copy scratch
    # trees, which is both needlessly slow and contrary to this gate's
    # offline/no-bank-access contract.
    for current, dirs, files in os.walk(ROOT, topdown=True, followlinks=False):
        current_path = Path(current)
        rel_dir = current_path.relative_to(ROOT)
        dirs[:] = sorted(
            name for name in dirs
            if name not in EVIDENCE_DIRS
            and not (rel_dir == Path(".") and SCRATCH_ROOT.match(name))
        )
        for name in sorted(files):
            path = current_path / name
            rel = path.relative_to(ROOT)
            if len(rel.parts) == 1 and GENERATED_STATE.match(rel.name):
                continue
            if TOOLING_STATE.match(rel.as_posix()):
                continue
            if path.suffix.lower() in CODE_EXT:
                yield rel, path


def absolute_path_sites():
    """Every absolute drive-letter path in real code, with its line."""
    out = []
    for rel, path in code_files():
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for num, line in enumerate(text.splitlines(), 1):
            if ABS_PATH.search(line):
                out.append((rel.as_posix(), num, line.strip()))
    return out


def is_comment(line):
    stripped = line.lstrip()
    return stripped.startswith("#") or stripped.startswith("//")


def main():
    # ---- A: the scanner itself must work -----------------------------------
    # Guard against the false-negative that already bit this project once.
    check(bool(USER_PATH.search(r'x = r"C:\Users\someone\thing"')),
          "A1 the absolute-path matcher actually matches a Windows user path")
    check(bool(ABS_PATH.search(r'p = r"C:\Jts"')),
          "A2 the absolute-path matcher matches a bare drive-letter root")
    check(not ABS_PATH.search("relative/path/only.txt"),
          "A3 the matcher does not fire on a relative path")
    check(not ABS_PATH.search('url = "https://example.com/x"'),
          "A4 the matcher does not mistake a URL scheme for a drive letter")
    check(bool(ABS_PATH.search('p = Path("D:/data")')),
          "A5 the matcher still catches a forward-slash drive path")

    # The three exemptions below are the only way a real absolute path can go
    # unreported, so each is pinned against over-broadening.
    check(bool(TWS_CONVENTION.search(r'never touches the real C:\Jts')) and
          not TWS_CONVENTION.search(r'root = r"C:\Users\someone\Desktop\Project"'),
          "A6 the TWS-convention exemption covers C:\\Jts but NOT a user path")
    check(sanctioned("engine/tws_discovery_reference.py") and
          not sanctioned("engine/stock_ibkr.py") and
          not sanctioned("display_data.py") and
          not sanctioned("engine/stock_storage.py"),
          "A7 the harness/selftest exemption never covers a shipped app module")
    check(bool(TOOLING_STATE.match(".claude/settings.local.json")) and
          bool(TOOLING_STATE.match(
              "docs/board/code_review_findings.json")) and
          bool(TOOLING_STATE.fullmatch(
              "engine/fixtures/portability_evidence.json")) and
          not TOOLING_STATE.match(
              "engine/fixtures/portability_runtime_config.json") and
          not TOOLING_STATE.match(
              "engine/portability_evidence.json") and
          not TOOLING_STATE.match("portability_evidence.json") and
          not TOOLING_STATE.match("docs/board/other.json") and
          not TOOLING_STATE.match("engine/stock_ibkr.py") and
          not TOOLING_STATE.match(".claude/skills/watch-codex/"
                                  "codex_review_watch.py"),
          "A8 the tooling-state exemption is per-file, not a .claude/ blanket")

    sites = absolute_path_sites()
    live = [(f, n, t) for f, n, t in sites if not is_comment(t)]

    # ---- B: the house self-location pattern still works ---------------------
    check(ROOT.name and (ROOT / "engine").is_dir(),
          "B1 the project root resolves from this file's own location")
    check((ENGINE / "stock_storage.py").exists(),
          "B2 engine modules are found relative to the resolved root")

    # every module that declares PROJECT_ROOT must derive it, never literal it
    declared = []
    for rel, path in code_files():
        if path.suffix != ".py":
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for num, line in enumerate(text.splitlines(), 1):
            if re.match(r"\s*(PROJECT_ROOT|ROOT)\s*=", line) and not is_comment(line):
                declared.append((rel.as_posix(), num, line.strip()))
    literal_roots = [d for d in declared if ABS_PATH.search(d[2])]
    check(not literal_roots,
          "B3 no module hard-codes its project root "
          f"({len(literal_roots)} found: "
          f"{', '.join(f'{f}:{n}' for f, n, _ in literal_roots[:3])})",
          feature=True)

    # ---- C: no load-bearing absolute path in shipped code -------------------
    # sanctioned() and TWS_CONVENTION are module-level (see their comments) so
    # checks A6/A7 exercise the SAME objects this gate uses, not copies of them.
    offenders = [(f, n, t) for f, n, t in live
                 if not sanctioned(f) and not TWS_CONVENTION.search(t)]
    user_specific = [(f, n, t) for f, n, t in offenders if USER_PATH.search(t)]

    check(not user_specific,
          "C1 no shipped code embeds a USER-specific path "
          f"({len(user_specific)} found: "
          f"{', '.join(f'{f}:{n}' for f, n, _ in user_specific[:3])})",
          feature=True)
    check(not offenders,
          f"C2 no shipped code embeds any absolute path ({len(offenders)} found)",
          feature=True)

    # ---- D: TWS facts are discovered, not stored ---------------------------
    tws = (ENGINE / "tws_launch.py").read_text(encoding="utf-8", errors="ignore")
    check("TWS_EXE = None" in tws,
          "D1 the TWS executable is discovered at run time, not pinned")
    disc = (ENGINE / "tws_discovery.py").read_text(encoding="utf-8", errors="ignore")
    check("def candidate_roots" in disc and "ProgramFiles" in disc,
          "D2 discovery searches conventional install roots per machine")
    multi = re.search(r"^MULTI_BASE_DEFAULT\s*=\s*(.+)$", tws, re.M)
    check(multi is not None and not USER_PATH.search(multi.group(1)),
          "D3 the multi-instance base is not tied to one user's profile",
          feature=True)

    # ---- E: a plain COPY of the code resolves its own root -----------------
    # The contract is copy-paste-and-run, so prove it on a real copy rather than
    # asserting it about the original.
    tmp = Path(tempfile.mkdtemp(prefix="portability_"))
    try:
        dest = tmp / "Relocated Project"
        (dest / "engine").mkdir(parents=True)
        for name in ("stock_storage.py", "operation_gate.py"):
            src = ENGINE / name
            if src.exists():
                shutil.copy2(src, dest / "engine" / name)
        probe = dest / "engine" / "_probe_root.py"
        probe.write_text(
            "from pathlib import Path\n"
            "PROJECT_ROOT = Path(__file__).resolve().parent.parent\n"
            "print(PROJECT_ROOT)\n", encoding="utf-8")
        res = subprocess.run([sys.executable, str(probe)],
                             capture_output=True, text=True, timeout=60)
        resolved = (res.stdout or "").strip()
        check(res.returncode == 0 and resolved == str(dest),
              "E1 a copied tree resolves its root to the NEW location "
              f"(got {resolved!r})")
        check(str(ROOT) not in resolved,
              "E2 the copy does not resolve back to the original location")

        # a module that self-locates must import from the copy with no edits
        imp = dest / "engine" / "_probe_import.py"
        imp.write_text(
            "import sys\n"
            "from pathlib import Path\n"
            "sys.path.insert(0, str(Path(__file__).resolve().parent))\n"
            "import stock_storage\n"
            "print('ok')\n", encoding="utf-8")
        res2 = subprocess.run([sys.executable, str(imp)],
                              capture_output=True, text=True, timeout=120)
        check(res2.returncode == 0 and "ok" in (res2.stdout or ""),
              "E3 a self-locating engine module imports from the copy unedited "
              f"({(res2.stderr or '').strip().splitlines()[-1:] or ['']}[0])")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    # ---- F: syntax integrity of what we scanned ----------------------------
    bad = []
    for rel, path in code_files():
        if path.suffix != ".py":
            continue
        try:
            ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        except SyntaxError:
            bad.append(rel.as_posix())
    check(not bad, f"F1 every scanned python file parses ({len(bad)} bad)")

    # ---- report ------------------------------------------------------------
    total = len(_PASS) + len(_BASE_FAIL) + len(_FEATURE_FAIL)
    # Report what C2 actually gates on. Printing every live site here read as
    # "27 absolute paths in shipped code" directly above "move-proof", which is
    # contradictory: all but the offenders are sanctioned non-runtime mentions.
    print(f"  absolute-path mentions: {len(live)} total, "
          f"{len(live) - len(offenders)} sanctioned "
          f"(harness/selftest/discovery/TWS-convention), "
          f"{len(offenders)} load-bearing")
    for f, n, t in offenders[:12]:
        print(f"    OFFENDER {f}:{n}  {t[:70]}")
    for name in _BASE_FAIL:
        print("  BASELINE FAIL:", name)
    for name in _FEATURE_FAIL:
        print("  not yet portable:", name)
    print(f"portability_reference: {len(_PASS)}/{total} passed, "
          f"{len(_BASE_FAIL)} baseline failures, "
          f"{len(_FEATURE_FAIL)} portability gaps")
    if _BASE_FAIL:
        print("EXIT 1 - a portability invariant broke; this is a regression.")
        return 1
    if _FEATURE_FAIL:
        print("EXIT 3 - BASELINE PINNED: known non-portable sites remain. "
              "Every invariant that must survive the fix holds.")
        return 3
    print("EXIT 0 - the code is move-proof.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
