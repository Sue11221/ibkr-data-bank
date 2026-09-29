"""Offline contract tests for the shared event-chained Collector."""

from __future__ import annotations

import sys
import threading
from pathlib import Path


ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
sys.path.insert(0, str(ENGINE_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

import check_kit  # noqa: E402
import event_probe  # noqa: E402


KIT = check_kit.CheckKit()
check = KIT.check
section = KIT.section


def expect_error(name, error, function, text=""):
    try:
        function()
    except error as exc:
        check(name, not text or text in str(exc), str(exc))
        return exc
    except BaseException as exc:  # noqa: BLE001 - exact contract oracle
        check(name, False, f"wrong {type(exc).__name__}: {exc}")
        return exc
    check(name, False, "did not raise")
    return None


class ProbeBase(BaseException):
    pass


class BadCopy:
    def __deepcopy__(self, _memo):
        raise RuntimeError("copy boom")


def test_surface_queries_and_copy_boundaries():
    section("[S1] surface, discriminators, queries, defensive copies")
    check("event probe publishes TEST_ONLY", event_probe.TEST_ONLY is True)
    check("event probe has a narrow explicit export list",
          set(event_probe.__all__)
          == {"Collector", "EventProbeError", "TEST_ONLY"})
    doc = event_probe.Collector.__doc__ or ""
    check("Collector doc pins synchronous triggers and guard-only timeouts",
          "synchronous" in doc.lower()
          and "failure guards" in doc.lower()
          and "never sequencing" in doc.lower())

    collector = event_probe.Collector()
    source = {"kind": "alpha", "type": "wrong",
              "detail": {"items": [1], "inner": {"value": 2}}}
    returned = collector(source)
    source["detail"]["items"].append(99)
    returned["detail"]["inner"]["value"] = 999
    first_events = collector.events
    first_events[0]["detail"]["items"].append(88)
    first_alpha = collector.of("alpha")
    first_alpha[0]["detail"]["inner"]["value"] = 777
    fresh = collector.events[0]
    check("ingress, return, events, and of() are nested-copy isolated",
          fresh["detail"] == {"items": [1], "inner": {"value": 2}},
          repr(fresh))
    check("events is an immutable sequence snapshot",
          isinstance(collector.events, tuple) and len(collector) == 1)
    check("default discriminator uses kind and ignores type",
          len(collector.of("alpha")) == 1
          and collector.of("wrong") == [])

    typed = event_probe.Collector(kind_key="type")
    typed({"kind": "wrong", "type": "alpha"})
    check("explicit type discriminator resolves a both-key event",
          len(typed.of("alpha")) == 1 and typed.of("wrong") == [])

    collector({"kind": "beta", "number": 2})
    check("index_of distinguishes first index zero from missing None",
          collector.index_of(lambda event: event["kind"] == "alpha") == 0
          and collector.index_of(lambda event: event["kind"] == "beta") == 1
          and collector.index_of(lambda event: event["kind"] == "missing")
          is None)

    def reentrant_query(event):
        if event.get("kind") != "alpha":
            return False
        event["detail"]["items"].append("query-local")
        collector({"kind": "query-added"})
        return True

    check("index_of predicate may re-enter over a stable event snapshot",
          collector.index_of(reentrant_query) == 0
          and len(collector) == 3
          and collector.events[0]["detail"]["items"] == [1])

    before = len(collector)
    expect_error("non-mapping ingress is rejected",
                 event_probe.EventProbeError,
                 lambda: collector([("kind", "bad")]), "mappings")
    expect_error("deep-copy failure propagates as probe error",
                 event_probe.EventProbeError,
                 lambda: collector({"kind": "bad", "value": BadCopy()}),
                 "deepcopy-safe")
    check("rejected/copy-failed ingress appends no partial record",
          len(collector) == before)


def test_registration_validation_and_future_only_contract():
    section("[S2] registration validation and future-only names")
    for label, function, text in (
            ("empty kind_key", lambda: event_probe.Collector(kind_key=""),
             "kind_key"),
            ("non-string kind_key", lambda: event_probe.Collector(kind_key=1),
             "kind_key"),
            ("empty trigger name", lambda: event_probe.Collector().on(
                "", lambda _event: True), "trigger names"),
            ("multiline trigger name", lambda: event_probe.Collector().on(
                "bad\nname", lambda _event: True), "single-line"),
            ("non-callable predicate", lambda: event_probe.Collector().on(
                "x", True), "predicate"),
            ("non-callable action", lambda: event_probe.Collector().on(
                "x", lambda _event: True, action=True), "action"),
            ("non-callable snapshot", lambda: event_probe.Collector().on(
                "x", lambda _event: True, snapshot=True), "snapshot"),
            ("non-callable query", lambda: event_probe.Collector().index_of(
                True), "query predicate"),
    ):
        expect_error(f"{label} is rejected", event_probe.EventProbeError,
                     function, text)

    collector = event_probe.Collector()
    collector({"kind": "target", "id": "before"})
    handle = collector.on("future", lambda event:
                          event.get("kind") == "target")
    check("registration does not replay already-recorded events",
          isinstance(handle, threading.Event) and not handle.is_set()
          and not collector.triggered("future"))
    check("named handle lookup returns the exact registered Event",
          collector.handle("future") is handle)
    collector({"kind": "target", "id": "after"})
    check("the next matching event fires a future-only trigger",
          handle.is_set() and collector.triggered("future"))
    collector({"kind": "target", "id": "later"})
    check("one-shot trigger remains fired without replay",
          handle.is_set() and len(collector) == 3)

    before_names = collector.handle("future")
    expect_error("duplicate trigger name is rejected",
                 event_probe.EventProbeError,
                 lambda: collector.on("future", lambda _event: True),
                 "already registered")
    check("duplicate rejection preserves the original named handle",
          collector.handle("future") is before_names)
    for label, function in (
            ("unknown handle", lambda: collector.handle("unknown")),
            ("unknown triggered", lambda: collector.triggered("unknown")),
            ("unknown snapshot", lambda: collector.snapshot("unknown")),
            ("snapshot without hook", lambda: collector.snapshot("future")),
    ):
        expect_error(f"{label} fails closed", event_probe.EventProbeError,
                     function)

    waiting = event_probe.Collector()
    waiting.on("snap", lambda _event: True,
               snapshot=lambda event: dict(event))
    expect_error("snapshot before trigger completion fails closed",
                 event_probe.EventProbeError,
                 lambda: waiting.snapshot("snap"), "no completed snapshot")


def test_trigger_order_snapshots_and_mutation_isolation():
    section("[S3] synchronous trigger order, snapshots, isolation")
    collector = event_probe.Collector()
    trace = []
    external = {"phase": "before"}

    def first_predicate(event):
        event["nested"]["items"].append("predicate-local")
        return event.get("kind") == "go"

    def first_snapshot(event):
        trace.append(("snapshot-1", event["id"]))
        event["nested"]["items"].append("snapshot-local")
        return {"phase": external["phase"], "event": event}

    def first_action(event):
        trace.append(("action-1", event["id"], len(collector)))
        event["nested"]["items"].append("action-local")
        external["phase"] = "after-first"

    def second_predicate(event):
        trace.append(("predicate-2", tuple(event["nested"]["items"])))
        return event.get("kind") == "go"

    def second_action(event):
        trace.append(("action-2", event["id"]))

    def second_snapshot(event):
        trace.append(("snapshot-2", event["id"]))
        return {"phase": external["phase"], "id": event["id"]}

    first = collector.on("first", first_predicate, first_action,
                         snapshot=first_snapshot)
    second = collector.on("second", second_predicate, second_action,
                          snapshot=second_snapshot)
    event = {"kind": "go", "id": "outer", "nested": {"items": [1]}}
    returned = collector(event)
    check("both named wait handles are real and settle synchronously",
          isinstance(first, threading.Event)
          and isinstance(second, threading.Event)
          and first.is_set() and second.is_set())
    check("event is recorded before trigger callbacks",
          ("action-1", "outer", 1) in trace)
    check("all same-event snapshots run before any action",
          [row[0] for row in trace]
          == ["predicate-2", "snapshot-1", "snapshot-2",
              "action-1", "action-2"],
          repr(trace))
    check("each predicate/hook/action receives an isolated nested copy",
          trace[0] == ("predicate-2", (1,))
          and collector.events[0]["nested"]["items"] == [1]
          and returned["nested"]["items"] == [1])
    captured = collector.snapshot("first")
    check("snapshot captures state before the action",
          captured["phase"] == "before"
          and captured["event"]["nested"]["items"]
          == [1, "snapshot-local"]
          and collector.snapshot("second")
          == {"phase": "before", "id": "outer"})
    captured["event"]["nested"]["items"].append("caller-local")
    check("snapshot() returns a defensive nested copy",
          collector.snapshot("first")["event"]["nested"]["items"]
          == [1, "snapshot-local"])

    collector({"kind": "go", "id": "again", "nested": {"items": [2]}})
    check("matching again never re-runs a fired trigger",
          len([row for row in trace if row[0].startswith("action")]) == 2
          and collector.snapshot("first")["event"]["id"] == "outer")


def test_handle_completion_and_reentrant_reservation():
    section("[S4] handle completion and reentrant same-event reservation")
    collector = event_probe.Collector()
    entered = threading.Event()
    release = threading.Event()
    action_done = threading.Event()
    thread_error = []

    def blocking_action(_event):
        entered.set()
        if not release.wait(10):
            raise AssertionError("release failure guard expired")
        action_done.set()

    handle = collector.on("blocked", lambda event:
                          event.get("kind") == "go", blocking_action)

    def producer():
        try:
            collector({"kind": "go"})
        except BaseException as exc:  # noqa: BLE001
            thread_error.append(exc)

    thread = threading.Thread(target=producer)
    thread.start()
    entered_ok = entered.wait(10)
    check("action enters while returned handle remains incomplete",
          entered_ok and not handle.is_set() and thread.is_alive())
    release.set()
    thread.join(20)
    check("handle settles only after the synchronous action finishes",
          not thread.is_alive() and not thread_error
          and action_done.is_set() and handle.is_set(), repr(thread_error))

    reentrant = event_probe.Collector()
    trace = []

    def first_action(event):
        trace.append(("first", event["id"], len(reentrant),
                      reentrant.index_of(lambda row:
                                         row.get("id") == event["id"])))
        reentrant({"kind": "go", "id": "nested"})

    first = reentrant.on("first", lambda event:
                         event.get("kind") == "go", first_action)
    second = reentrant.on("second", lambda event:
                          event.get("kind") == "go",
                          lambda event: trace.append(("second", event["id"])),
                          snapshot=lambda event: {"id": event["id"]})
    reentrant_errors = []

    def reentrant_producer():
        try:
            reentrant({"kind": "go", "id": "outer"})
        except BaseException as exc:  # noqa: BLE001
            reentrant_errors.append(exc)

    reentrant_thread = threading.Thread(target=reentrant_producer)
    reentrant_thread.start()
    reentrant_thread.join(20)
    check("all outer matches reserve before a reentrant action emits",
          not reentrant_thread.is_alive() and not reentrant_errors
          and first.is_set() and second.is_set()
          and trace == [("first", "outer", 1, 0),
                        ("second", "outer")],
          f"trace={trace!r} errors={reentrant_errors!r}")
    check("reentrant event records after outer without stealing triggers",
          [event["id"] for event in reentrant.events]
          == ["outer", "nested"]
          and reentrant.snapshot("second") == {"id": "outer"})


def test_concurrent_one_shot_claim():
    section("[S5] concurrent producers: no loss, atomic first match")
    collector = event_probe.Collector()
    winner = []
    handle = collector.on("winner", lambda event:
                          event.get("kind") == "go",
                          lambda event: winner.append(event["sequence"]))
    worker_count = 20
    barrier = threading.Barrier(worker_count + 1)
    errors = []

    def worker(sequence):
        try:
            barrier.wait(10)
            collector({"kind": "go", "sequence": sequence})
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(index,))
               for index in range(worker_count)]
    for thread in threads:
        thread.start()
    barrier.wait(10)
    for thread in threads:
        thread.join(20)
    events = collector.events
    check("barrier burst records every producer without callback errors",
          not errors and all(not thread.is_alive() for thread in threads)
          and len(events) == worker_count, repr(errors))
    check("named trigger claims and actions exactly once",
          handle.is_set() and len(winner) == 1)
    check("one-shot winner is the earliest recorded matching event",
          winner == [events[0]["sequence"]],
          f"winner={winner} first={events[0] if events else None}")

    shared = event_probe.Collector()
    writer_count, per_writer, reader_count = 6, 40, 4
    start = threading.Barrier(writer_count + reader_count + 1)
    writers_done = threading.Event()
    shared_errors = []
    reader_iterations = []

    def shared_writer(writer_index):
        try:
            start.wait(10)
            for item in range(per_writer):
                shared({"kind": "row", "writer": writer_index,
                        "item": item})
        except BaseException as exc:  # noqa: BLE001
            shared_errors.append(exc)

    def shared_reader():
        iterations = 0
        try:
            start.wait(10)
            while True:
                rows = shared.events
                shared.of("row")
                shared.index_of(lambda event:
                                event.get("kind") == "row")
                len(shared)
                iterations += 1
                if writers_done.is_set():
                    # This final snapshot is taken after the completion signal.
                    rows = shared.events
                    break
            if len(rows) > writer_count * per_writer:
                raise AssertionError("reader observed impossible event count")
        except BaseException as exc:  # noqa: BLE001
            shared_errors.append(exc)
        finally:
            reader_iterations.append(iterations)

    shared_writers = [threading.Thread(target=shared_writer, args=(index,))
                      for index in range(writer_count)]
    shared_readers = [threading.Thread(target=shared_reader)
                      for _index in range(reader_count)]
    for thread in shared_writers + shared_readers:
        thread.start()
    start.wait(10)
    for thread in shared_writers:
        thread.join(20)
    writers_done.set()
    for thread in shared_readers:
        thread.join(20)
    check("simultaneous event queries and writers remain race-free",
          not shared_errors
          and all(not thread.is_alive()
                  for thread in shared_writers + shared_readers)
          and len(shared) == writer_count * per_writer
          and len(shared.of("row")) == writer_count * per_writer
          and len(reader_iterations) == reader_count
          and all(value >= 1 for value in reader_iterations),
          f"errors={shared_errors!r} iterations={reader_iterations!r}")


def test_callback_failures_propagate_and_settle():
    section("[S6] predicate, snapshot, and action failure semantics")
    predicate_collector = event_probe.Collector()
    ordinary_actions = []

    def bad_predicate(_event):
        raise ProbeBase("predicate stop")

    bad_handle = predicate_collector.on("bad-predicate", bad_predicate)
    good_handle = predicate_collector.on(
        "ordinary", lambda _event: True,
        lambda event: ordinary_actions.append(event["id"]))
    error = expect_error("predicate BaseException propagates exactly",
                         ProbeBase,
                         lambda: predicate_collector(
                             {"kind": "go", "id": "outer"}),
                         "predicate stop")
    check("predicate failure is recorded and consumed with wait guard unset",
          isinstance(error, ProbeBase) and len(predicate_collector) == 1
          and not bad_handle.is_set()
          and predicate_collector.triggered("bad-predicate"))
    check("an unrelated same-event match still runs before propagation",
          good_handle.is_set() and ordinary_actions == ["outer"])
    predicate_collector({"kind": "go", "id": "again"})
    check("consumed predicate failure and ordinary trigger never retry",
          len(predicate_collector) == 2 and ordinary_actions == ["outer"])

    snapshot_collector = event_probe.Collector()
    skipped_action = []
    following_action = []

    def bad_snapshot(_event):
        raise RuntimeError("snapshot stop")

    snapshot_handle = snapshot_collector.on(
        "bad-snapshot", lambda _event: True,
        lambda _event: skipped_action.append(True), snapshot=bad_snapshot)
    following_handle = snapshot_collector.on(
        "following", lambda _event: True,
        lambda event: following_action.append(event["id"]))
    expect_error("snapshot exception propagates", RuntimeError,
                 lambda: snapshot_collector(
                     {"kind": "go", "id": "snap"}), "snapshot stop")
    check("failed snapshot skips its action and leaves wait guard unset",
          not snapshot_handle.is_set() and not skipped_action
          and snapshot_collector.triggered("bad-snapshot"))
    check("later reserved trigger still executes and settles",
          following_handle.is_set() and following_action == ["snap"])
    expect_error("failed snapshot remains unavailable",
                 event_probe.EventProbeError,
                 lambda: snapshot_collector.snapshot("bad-snapshot"),
                 "no completed snapshot")
    snapshot_collector({"kind": "go", "id": "snap-again"})
    check("failed snapshot is consumed and never retries",
          not snapshot_handle.is_set() and not skipped_action
          and len(snapshot_collector) == 2)

    action_collector = event_probe.Collector()
    action_calls = []

    def bad_action(event):
        action_calls.append(("bad", event["id"]))
        raise ProbeBase("action stop")

    action_handle = action_collector.on(
        "bad-action", lambda _event: True, bad_action)
    later_handle = action_collector.on(
        "later-action", lambda _event: True,
        lambda event: action_calls.append(("later", event["id"])))
    expect_error("action BaseException propagates exactly", ProbeBase,
                 lambda: action_collector(
                     {"kind": "go", "id": "action"}), "action stop")
    check("failing action stays unset while later success settles in order",
          not action_handle.is_set() and later_handle.is_set()
          and action_calls == [("bad", "action"), ("later", "action")])
    action_collector({"kind": "go", "id": "again"})
    check("failed action is terminal and never retries",
          action_calls == [("bad", "action"), ("later", "action")]
          and len(action_collector) == 2)

    reentrant_predicate = event_probe.Collector()
    predicate_handle = None

    def emitting_predicate(_event):
        reentrant_predicate({"kind": "nested"})
        return True

    predicate_handle = reentrant_predicate.on(
        "emitting", emitting_predicate)
    expect_error("predicate emission fails closed instead of recursing",
                 event_probe.EventProbeError,
                 lambda: reentrant_predicate({"kind": "outer"}),
                 "must not emit")
    check("predicate-emission failure records outer with wait guard unset",
          not predicate_handle.is_set() and len(reentrant_predicate) == 1
          and reentrant_predicate.events[0]["kind"] == "outer")

    guarded = event_probe.Collector()
    guarded_handle = guarded.on(
        "guarded", lambda _event: True,
        lambda _event: (_ for _ in ()).throw(
            RuntimeError("observer-swallowed")))

    def swallowed_observer(event):
        try:
            guarded(event)
        except Exception:
            pass

    swallowed_observer({"kind": "go"})
    check("producer-suppressed callback error cannot false-green a waiter",
          guarded.triggered("guarded")
          and not guarded_handle.wait(0) and len(guarded) == 1)

    multiple = event_probe.Collector()
    multiple_calls = []

    def first_failure(_event):
        multiple_calls.append("first")
        raise RuntimeError("first failure")

    def second_failure(_event):
        multiple_calls.append("second")
        raise ValueError("second failure")

    first_failed = multiple.on("first", lambda _event: True, first_failure)
    second_failed = multiple.on("second", lambda _event: True, second_failure)
    expect_error("multiple callback failures re-raise the first exact error",
                 RuntimeError,
                 lambda: multiple({"kind": "go"}), "first failure")
    check("all reserved failing actions are attempted without false handles",
          multiple_calls == ["first", "second"]
          and not first_failed.is_set() and not second_failed.is_set())


def run():
    test_surface_queries_and_copy_boundaries()
    test_registration_validation_and_future_only_contract()
    test_trigger_order_snapshots_and_mutation_isolation()
    test_handle_completion_and_reentrant_reservation()
    test_concurrent_one_shot_claim()
    test_callback_failures_propagate_and_settle()
    raise SystemExit(KIT.finish())


if __name__ == "__main__":
    run()
