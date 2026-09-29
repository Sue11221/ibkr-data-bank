"""Durable mutation proof for the Row 52 move-proof acceptance gate.

Every mutation is made inside a temporary copied engine.  The project sources
are hashed before and after the proof, and each copied source is restored
byte-for-byte in a ``finally`` block before the final green baseline.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:  # noqa: BLE001 - redirected/older streams
    pass

ENGINE = Path(__file__).resolve().parent
ROOT = ENGINE.parent
sys.path.insert(0, str(ENGINE))

import check_kit  # noqa: E402


KIT = check_kit.CheckKit()
check = KIT.check
section = KIT.section

FIXTURE_FILES = (
    "operation_gate.py",
    "portability_reference.py",
    "stock_storage.py",
    "tws_discovery.py",
    "tws_launch.py",
    "xval_x0_reference.py",
)

# Historical deletion evidence is not a runtime path configuration. Keep its
# exact bytes in the fixture so this regression cannot disappear in a tiny copy.
EVIDENCE_FILE = Path("engine/fixtures/portability_evidence.json")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def run_gate(project_root):
    return subprocess.run(
        [sys.executable, "-B",
         str(Path(project_root) / "engine" / "portability_reference.py")],
        cwd=project_root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
        check=False,
    )


def gate_green(result):
    return (result.returncode == 0
            and "portability_reference: 20/20 passed" in result.stdout
            and "EXIT 0 - the code is move-proof." in result.stdout)


def main():
    section("[P1] copied baseline")
    source_digests = {name: digest(ENGINE / name) for name in FIXTURE_FILES}
    evidence_digest = digest(ROOT / EVIDENCE_FILE)

    with tempfile.TemporaryDirectory(prefix="portability-mutation-") as temp:
        copied_root = Path(temp) / "Relocated Project"
        copied_engine = copied_root / "engine"
        copied_engine.mkdir(parents=True)
        for name in FIXTURE_FILES:
            shutil.copy2(ENGINE / name, copied_engine / name)
        copied_evidence = copied_root / EVIDENCE_FILE
        copied_evidence.parent.mkdir(parents=True)
        shutil.copy2(ROOT / EVIDENCE_FILE, copied_evidence)
        check("the exact historical evidence is copied byte-for-byte",
              digest(copied_evidence) == evidence_digest)

        baseline = run_gate(copied_root)
        check("an unedited copied fixture passes the move-proof gate",
              gate_green(baseline),
              repr((baseline.returncode, baseline.stdout, baseline.stderr)))

        section("[P2] hard-coded project-root mutation")
        xval = copied_engine / "xval_x0_reference.py"
        xval_original = xval.read_bytes()
        xval_text = xval_original.decode("utf-8")
        xval_anchor = (
            "ROOT = Path(__file__).resolve().parent.parent   "
            "# self-locating (Row 52 M2a)")
        check("the project-root mutation anchor is unique",
              xval_text.count(xval_anchor) == 1,
              str(xval_text.count(xval_anchor)))
        root_mutation = "ROOT = Path(" + repr(str(copied_root.resolve())) + ")"
        root_result = None
        try:
            xval.write_bytes(
                xval_text.replace(xval_anchor, root_mutation, 1)
                .encode("utf-8"))
            root_result = run_gate(copied_root)
        finally:
            xval.write_bytes(xval_original)
        check("the gate rejects a copied module with a literal project root",
              root_result is not None
              and root_result.returncode == 3
              and "not yet portable: B3 no module hard-codes its project root"
              in root_result.stdout
              and "EXIT 3 - BASELINE PINNED" in root_result.stdout,
              repr(None if root_result is None else (
                  root_result.returncode,
                  root_result.stdout,
                  root_result.stderr)))
        check("the project-root mutation is restored byte-exactly",
              digest(xval) == hashlib.sha256(xval_original).hexdigest())

        section("[P3] user-profile mutation")
        tws = copied_engine / "tws_launch.py"
        tws_original = tws.read_bytes()
        tws_text = tws_original.decode("utf-8")
        tws_anchor = 'MULTI_BASE_DEFAULT = str(Path.home() / "TwsMulti")'
        check("the user-profile mutation anchor is unique",
              tws_text.count(tws_anchor) == 1,
              str(tws_text.count(tws_anchor)))
        synthetic_user = (
            Path(copied_root.anchor) / "Users" / "MutationProbe" / "TwsMulti")
        # Forward slashes keep the generated Python literal parseable (a raw
        # ``C:\\Users`` literal would otherwise interpret ``\\U``) while the
        # gate's drive-path matcher deliberately covers both slash styles.
        tws_mutation = (
            "MULTI_BASE_DEFAULT = " + repr(synthetic_user.as_posix()))
        tws_result = None
        try:
            tws.write_bytes(
                tws_text.replace(tws_anchor, tws_mutation, 1)
                .encode("utf-8"))
            tws_result = run_gate(copied_root)
        finally:
            tws.write_bytes(tws_original)
        check("the gate rejects a copied TWS default tied to one user",
              tws_result is not None
              and tws_result.returncode == 3
              and "not yet portable: D3 the multi-instance base is not tied"
              in tws_result.stdout
              and "EXIT 3 - BASELINE PINNED" in tws_result.stdout,
              repr(None if tws_result is None else (
                  tws_result.returncode,
                  tws_result.stdout,
                  tws_result.stderr)))
        check("the user-profile mutation is restored byte-exactly",
              digest(tws) == hashlib.sha256(tws_original).hexdigest())

        section("[P4] evidence exception is exact, not a directory or basename")
        synthetic_root = "C:/Users/MutationProbe/RuntimeRoot"
        for relative in (
            Path("engine/fixtures/portability_runtime_config.json"),
            Path("engine") / EVIDENCE_FILE.name,
            Path(EVIDENCE_FILE.name),
        ):
            candidate = copied_root / relative
            try:
                candidate.write_text(json.dumps({"runtime_root": synthetic_root}),
                                     encoding="utf-8")
                result = run_gate(copied_root)
                check("the gate rejects runtime paths in " + relative.as_posix(),
                      result.returncode == 3
                      and "not yet portable: C1" in result.stdout
                      and "not yet portable: C2" in result.stdout
                      and "OFFENDER " + relative.as_posix() in result.stdout,
                      repr((result.returncode, result.stdout, result.stderr)))
            finally:
                candidate.unlink(missing_ok=True)
            check("the synthetic runtime config is removed: " + relative.as_posix(),
                  not candidate.exists())

        section("[P5] restoration and production custody")
        restored = run_gate(copied_root)
        check("the restored copied fixture returns to the green baseline",
              gate_green(restored),
              repr((restored.returncode, restored.stdout, restored.stderr)))
        check("every copied fixture source matches its original digest",
              all(digest(copied_engine / name) == source_digests[name]
                  for name in FIXTURE_FILES))
        check("the copied historical evidence remains byte-exact",
              digest(copied_evidence) == evidence_digest)

    check("the mutation proof never edits any project source",
          all(digest(ENGINE / name) == source_digests[name]
              for name in FIXTURE_FILES))
    check("the historical deletion evidence remains unchanged",
          digest(ROOT / EVIDENCE_FILE) == evidence_digest)
    return KIT.finish()


if __name__ == "__main__":
    raise SystemExit(main())
