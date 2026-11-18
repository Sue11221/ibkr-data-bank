"""Offline tests for approval-first split join-gate enrichment."""

import ast
import copy
import datetime as dt
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_log  # noqa: E402
import split_join_enrichment as enrichment  # noqa: E402
import stock_ibkr as ibkr  # noqa: E402


FAILS = []
COUNT = [0]


def check(name, condition, detail=""):
    COUNT[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILS.append(name)


ROOT = Path(tempfile.mkdtemp(prefix="split_join_enrichment_st_"))
IDENTITY = {"ticker": "AAA", "conid": 123, "provider_symbol": "AAA"}
MANIFEST = {
    "folder": "AAA", "symbol": "AAA", "conid": 123,
    "actions": [], "data_corrections": [],
}


def event(ex_date="2024-06-10", ratio=4.0, *, confirmed=True,
          source_ids=None):
    return {
        "ex_date": ex_date,
        "ratio": ratio,
        "ratio_convention": "new_shares_per_old_share",
        "confidence": "confirmed" if confirmed else "provisional",
        "confirmed": confirmed,
        "source_ids": source_ids or ["issuer:aaa", "provider:aaa"],
    }


def payload(events=None):
    return {
        "ticker": "AAA", "conid": 123, "provider_symbol": "AAA",
        "provider": "fixture-provider", "provider_status": "ok",
        "fetched_at": "2024-06-11T12:00:00+00:00",
        "events": list(events if events is not None else [event()]),
        "coverage": {},
    }


def loaders(*, cache_payload=None, cache_status="ok", identities=None,
            manifests=None, calls=None):
    identities = list(identities or [IDENTITY])
    manifests = list(manifests or [MANIFEST])
    calls = calls if calls is not None else []
    state = {"identity": 0, "manifest": 0}

    def identity_loader(root, ticker):
        index = min(state["identity"], len(identities) - 1)
        state["identity"] += 1
        return copy.deepcopy(identities[index])

    def manifest_loader(path):
        index = min(state["manifest"], len(manifests) - 1)
        state["manifest"] += 1
        return copy.deepcopy(manifests[index])

    def cache_loader(root, identity, **kwargs):
        calls.append((copy.deepcopy(identity), dict(kwargs)))
        if cache_status != "ok":
            return {"status": cache_status, "usable": False,
                    "path": "AAA__123.json"}
        return {
            "status": "ok", "usable": True,
            "path": str(ROOT / "_split_history" / "AAA__123.json"),
            "cache": copy.deepcopy(cache_payload or payload()),
        }

    return identity_loader, cache_loader, manifest_loader


def enrich(*, gate="join", observed=0.25, prior="2024-06-07",
           current="2024-06-10", overlap_verdict=None, **loader_options):
    identity_loader, cache_loader, manifest_loader = loaders(**loader_options)
    return enrichment.enrich_halt(
        ROOT, "AAA", "1m", gate=gate, prior_date=prior,
        current_date=current, observed_factor=observed,
        original_halt="JOIN GATE original text", run_id="run-test",
        overlap_verdict=overlap_verdict,
        identity_loader=identity_loader, cache_loader=cache_loader,
        manifest_loader=manifest_loader)


print("=== helper success and schema =====================================")
row = enrich()
check("forward split creates an unapplied reciprocal-factor proposal",
      row is not None and row["action"]["factor"] == 0.25
      and row["requires_user_approval"] is True
      and row["applied"] is False, str(row))
check("proposal preserves the original halt and bounded provenance",
      row["original_halt"] == "JOIN GATE original text"
      and row["cache"]["path"] == "AAA__123.json"
      and "sources" not in row["cache"] and "payload" not in row, str(row))
reverse = enrich(observed=10.0, cache_payload=payload([event(ratio=0.1)]))
check("reverse split creates the canonical reciprocal factor",
      reverse is not None and reverse["action"]["factor"] == 10.0,
      str(reverse))

overlap = {"kind": "split", "factor": 0.25, "applies": "price"}
entry = enrich(gate="entry_overlap", overlap_verdict=overlap)
check("entry overlap accepts only the existing split verdict",
      entry is not None and entry["gate"] == "entry_overlap", str(entry))
check("generic overlap disagreement never becomes a proposal",
      enrich(gate="entry_overlap", overlap_verdict={
          "kind": "incoherent", "factor": None}) is None)
thin = enrich(gate="entry_thin")
check("thin entry may use the same close/open boundary ratio",
      thin is not None and thin["gate"] == "entry_thin", str(thin))


print("=== fail-closed evidence matrix ===================================")
for status in ("missing", "stale", "corrupt", "identity_mismatch"):
    check(f"{status} cache preserves the un-enriched halt",
          enrich(cache_status=status) is None)
check("provisional event is refused",
      enrich(cache_payload=payload([event(confirmed=False)])) is None)
check("multiple matching events are refused",
      enrich(cache_payload=payload([
          event(), event(ex_date="2024-06-11", ratio=4.0,
                         source_ids=["issuer:bbb", "provider:bbb"])])) is None)
check("source-conflicting events are refused",
      enrich(cache_payload=payload([
          event(ratio=4.0), event(ratio=10.0,
                                  source_ids=["issuer:conflict"])])) is None)
check("factor mismatch is refused",
      enrich(observed=0.6) is None)
check("event outside exact-date tolerance is refused",
      enrich(cache_payload=payload([event(ex_date="2024-06-21")])) is None)

changed_identity = dict(IDENTITY, conid=999)
check("identity race after cache validation is refused",
      enrich(identities=[IDENTITY, changed_identity]) is None)

def raising_cache_loader(root, identity, **kwargs):
    raise RuntimeError("injected read failure")

identity_loader, _cache_loader, manifest_loader = loaders()
read_failure = enrichment.enrich_halt(
    ROOT, "AAA", "1m", gate="join", prior_date="2024-06-07",
    current_date="2024-06-10", observed_factor=0.25,
    original_halt="original", identity_loader=identity_loader,
    cache_loader=raising_cache_loader, manifest_loader=manifest_loader)
check("unexpected cache read exception preserves the un-enriched halt",
      read_failure is None)
changed_manifest = dict(MANIFEST, actions=[{
    "date": "2024-06-10", "kind": "split", "factor": 0.25,
    "applies": "price", "source": "user", "evidence": "race",
    "run": None,
}])
check("action race after cache validation is refused",
      enrich(manifests=[MANIFEST, changed_manifest]) is None)
check("existing price action at the boundary suppresses enrichment",
      enrich(manifests=[changed_manifest]) is None)
malformed_action_manifest = dict(MANIFEST, actions=[{
    "date": "2024-06-10", "kind": "split", "factor": 0.25,
    "applies": "unknown", "source": "user", "evidence": "bad",
}])
check("malformed action scope fails closed",
      enrich(manifests=[malformed_action_manifest]) is None)
correction_manifest = dict(MANIFEST, data_corrections=[{
    "type": "phantom_split_correction", "ex_date": "2024-06-10",
    "factor": 4.0, "applied": True,
}])
check("overlapping durable correction suppresses enrichment",
      enrich(manifests=[correction_manifest]) is None)


print("=== source boundary and write traps ===============================")
source_path = Path(enrichment.__file__)
source = source_path.read_text(encoding="utf-8")
tree = ast.parse(source)
imports = set()
for node in ast.walk(tree):
    if isinstance(node, ast.Import):
        imports.update(alias.name for alias in node.names)
    elif isinstance(node, ast.ImportFrom) and node.module:
        imports.add(node.module)
check("helper imports no provider, broker adapter, GUI, or stock_ibkr",
      not any(any(token in name for token in (
          "provider", "urllib", "requests", "ib_async", "stock_ibkr",
          "display_data", "tkinter")) for name in imports), str(imports))
check("helper has no apply_action or save_manifest reference",
      "apply_action" not in source and "save_manifest" not in source)

write_calls = []
old_apply = enrichment.stock_basis.apply_action
old_save = enrichment.stock_storage.save_manifest
enrichment.stock_basis.apply_action = lambda *a, **k: write_calls.append("apply")
enrichment.stock_storage.save_manifest = lambda *a, **k: write_calls.append("save")
try:
    trapped = enrich()
finally:
    enrichment.stock_basis.apply_action = old_apply
    enrichment.stock_storage.save_manifest = old_save
check("successful enrichment triggers no action or manifest writer",
      trapped is not None and not write_calls, str(write_calls))
check("test-owned storage root remains byte-empty",
      not any(path.is_file() for path in ROOT.rglob("*")))


print("=== stock_ibkr integration and renderers ==========================")
plain = ibkr.SeriesHalt("plain halt")
check("SeriesHalt remains single-string compatible",
      str(plain) == "plain halt" and plain.metadata == {})

old_enrich = ibkr.split_enrichment.enrich_halt
integration_calls = []
ibkr.split_enrichment.enrich_halt = lambda *a, **k: (
    integration_calls.append((a, k)) or copy.deepcopy(row))
try:
    res = {}
    gate_exc = ibkr.SeriesHalt("unchanged halt", metadata={
        "split_gate": {
            "gate": "join", "prior_date": dt.date(2024, 6, 7),
            "current_date": dt.date(2024, 6, 10),
            "observed_factor": 0.25,
        }})
    attached = ibkr._attach_split_enrichment(
        res, ROOT, "AAA", "1m", "run-test", gate_exc)
    no_context = ibkr._attach_split_enrichment(
        {}, ROOT, "AAA", "1m", "run-test", plain)
finally:
    ibkr.split_enrichment.enrich_halt = old_enrich
check("outer wrapper attaches metadata without changing halt text",
      attached and res.get("split_enrichment")
      and str(gate_exc) == "unchanged halt", str(res))
check("non-gate halt performs zero cache/enrichment calls",
      not no_context and len(integration_calls) == 1, str(integration_calls))

ibkr.split_enrichment.enrich_halt = lambda *a, **k: None
try:
    unchanged_res = {}
    unchanged_attached = ibkr._attach_split_enrichment(
        unchanged_res, ROOT, "AAA", "1m", "run-test", gate_exc)
finally:
    ibkr.split_enrichment.enrich_halt = old_enrich
check("failed enrichment preserves the byte-identical halt result shape",
      not unchanged_attached and unchanged_res == {}
      and str(gate_exc) == "unchanged halt", str(unchanged_res))

existing = []
incoming = []
start = dt.datetime(2024, 6, 10, 9, 30)
for index in range(40):
    stamp = start + dt.timedelta(minutes=index)
    existing.append((stamp, 100.0, 101.0, 99.0, 100.0, 1000))
    incoming.append((stamp, 25.0, 25.25, 24.75, 25.0, 1000))
processor = ibkr._make_session_processor(
    {"bars_fetched": 0, "_empty_days": [], "notes": [],
     "committed_through": None},
    "AAA", "1m", existing, 100.0, False, lambda _msg: None,
    lambda upto_month=None: None, {}, [], [dt.date(2024, 6, 10)])
try:
    processor(dt.date(2024, 6, 10), incoming, 0, 1)
except ibkr.SeriesHalt as exc:
    context = exc.metadata.get("split_gate")
else:
    context = None
check("entry gate carries a bounded split-overlap context",
      context is not None and context["gate"] == "entry_overlap"
      and abs(context["observed_factor"] - 0.25) < 1e-9, str(context))

gate_cache_calls = []
old_enrich = ibkr.split_enrichment.enrich_halt
ibkr.split_enrichment.enrich_halt = lambda *a, **k: gate_cache_calls.append(1)
try:
    clean_res = {"bars_fetched": 0, "_empty_days": [], "notes": [],
                 "committed_through": None}
    clean_processor = ibkr._make_session_processor(
        clean_res, "AAA", "1m", existing, 100.0, False,
        lambda _msg: None, lambda upto_month=None: None, {}, [],
        [dt.date(2024, 6, 10)])
    clean_processor(dt.date(2024, 6, 10), list(existing), 0, 1)

    action_res = {"bars_fetched": 0, "_empty_days": [], "notes": [],
                  "committed_through": None}
    recorded = [{
        "date": "2024-06-10", "kind": "split", "factor": 0.25,
        "applies": "price", "source": "user", "evidence": "approved",
        "run": None,
    }]
    action_processor = ibkr._make_session_processor(
        action_res, "AAA", "1m", [], None, True, lambda _msg: None,
        lambda upto_month=None: None, {}, recorded,
        [dt.date(2024, 6, 7), dt.date(2024, 6, 10)])
    action_processor(
        dt.date(2024, 6, 7),
        [(dt.datetime(2024, 6, 7, 9, 30), 100.0, 101.0, 99.0,
          100.0, 1000)], 0, 2)
    action_processor(
        dt.date(2024, 6, 10),
        [(dt.datetime(2024, 6, 10, 9, 30), 25.0, 25.25, 24.75,
          25.0, 1000)], 1, 2)
finally:
    ibkr.split_enrichment.enrich_halt = old_enrich
check("clean and already-recorded gate paths perform zero enrichment reads",
      not gate_cache_calls, str(gate_cache_calls))

series = {"ticker": "AAA", "interval": "1m", "halt": "original",
          "split_enrichment": row}
summary = ibkr.summarize_report({
    "run": "r", "series": [series], "totals": {"halted_series": 1}})
human_log = run_log.format_run_log({
    "run": "r", "series": [series], "totals": {"halted_series": 1}})
check("issue summary renders approval-required and unapplied state",
      any("SPLIT PROPOSAL" in line and "NOT APPLIED" in line
          for line in summary), str(summary))
check("run log renders proposal without raw provider payload",
      "SPLIT PROPOSAL" in human_log and "NOT APPLIED" in human_log
      and "issuer:aaa" not in human_log and "provider:aaa" not in human_log,
      human_log)


print(f"\n{COUNT[0] - len(FAILS)}/{COUNT[0]} checks passed")
if FAILS:
    print("Failures:", ", ".join(FAILS))
    raise SystemExit(1)
