"""TEST-ONLY event-chained progress probes for deterministic harnesses.

``Collector`` records mapping-shaped callback events and runs named one-shot
triggers synchronously inside the producer's callback.  A harness must use the
event that releases the next action as its sequencing boundary; timeout waits
are failure guards only, never sequencing.

Production modules must never import this module.
"""

from __future__ import annotations

import copy
import threading
from collections.abc import Mapping


TEST_ONLY = True
__all__ = ["Collector", "EventProbeError", "TEST_ONLY"]
_MISSING = object()


class EventProbeError(RuntimeError):
    """The probe contract or a named trigger lookup is invalid."""


class _Trigger:
    def __init__(self, name, predicate, action, snapshot):
        self.name = name
        self.predicate = predicate
        self.action = action
        self.snapshot = snapshot
        self.handle = threading.Event()


def _clone(value, label):
    try:
        return copy.deepcopy(value)
    except Exception as exc:
        raise EventProbeError(f"{label} must be deepcopy-safe") from exc


def _name(value):
    if not isinstance(value, str) or not value.strip():
        raise EventProbeError("trigger names must be non-empty strings")
    if "\n" in value or "\r" in value:
        raise EventProbeError("trigger names must be single-line strings")
    return value


class Collector:
    """Thread-safe event recorder with synchronous named one-shot triggers.

    ``kind_key`` is explicit because production callbacks in this repository
    use both ``kind`` and ``type`` discriminators.  ``on`` returns a real
    :class:`threading.Event`; it is set only after every matching snapshot hook
    has captured event-time state and that trigger's action finishes
    successfully.  It remains unset on predicate, snapshot, or action failure,
    so a producer that suppresses callback exceptions still causes the
    harness's bounded failure-guard wait to fail.

    Snapshot hooks run before actions.  Events, callback arguments, query
    results, and stored snapshots are defensive deep copies.  Trigger
    predicates and callbacks run outside the state lock, but callbacks are
    serialized in recorded-event order and may query or re-enter the collector.
    Hooks must remain short and must not wait for any not-yet-set Collector
    handle or callback while the callback sequencer is held.
    Timeouts around returned handles are failure guards, never sequencing.
    """

    def __init__(self, *, kind_key="kind"):
        if (not isinstance(kind_key, str) or not kind_key.strip()
                or "\n" in kind_key or "\r" in kind_key):
            raise EventProbeError("kind_key must be a non-empty single-line string")
        self.kind_key = kind_key
        self._state_lock = threading.RLock()
        self._callback_lock = threading.RLock()
        self._events = []
        self._triggers = {}
        self._fired = set()
        self._snapshots = {}
        self._phase = threading.local()

    def on(self, name, predicate, action=None, *, snapshot=None):
        """Register a future-only one-shot trigger and return its wait handle."""
        name = _name(name)
        if not callable(predicate):
            raise EventProbeError("trigger predicate must be callable")
        if action is not None and not callable(action):
            raise EventProbeError("trigger action must be callable or None")
        if snapshot is not None and not callable(snapshot):
            raise EventProbeError("snapshot hook must be callable or None")
        trigger = _Trigger(name, predicate, action, snapshot)
        with self._state_lock:
            if name in self._triggers:
                raise EventProbeError(f"trigger {name!r} is already registered")
            self._triggers[name] = trigger
        return trigger.handle

    def handle(self, name):
        """Return the wait handle for a registered trigger."""
        trigger = self._registered(name)
        return trigger.handle

    def triggered(self, name):
        """Return whether a registered trigger has claimed its first match."""
        name = _name(name)
        with self._state_lock:
            if name not in self._triggers:
                raise EventProbeError(f"unknown trigger {name!r}")
            return name in self._fired

    def snapshot(self, name):
        """Return a defensive copy of a fired trigger's captured snapshot."""
        trigger = self._registered(name)
        if trigger.snapshot is None:
            raise EventProbeError(f"trigger {trigger.name!r} has no snapshot hook")
        with self._state_lock:
            value = self._snapshots.get(trigger.name, _MISSING)
            if value is _MISSING:
                raise EventProbeError(
                    f"trigger {trigger.name!r} has no completed snapshot")
        return _clone(value, "stored snapshot")

    @property
    def events(self):
        """Return an immutable snapshot of the recorded event sequence."""
        with self._state_lock:
            values = tuple(self._events)
        return tuple(_clone(value, "recorded event") for value in values)

    def of(self, kind):
        """Return recorded events whose configured discriminator equals *kind*."""
        return [event for event in self.events
                if event.get(self.kind_key) == kind]

    def index_of(self, predicate):
        """Return the first recorded-event index matching *predicate*, or None."""
        if not callable(predicate):
            raise EventProbeError("query predicate must be callable")
        for index, event in enumerate(self.events):
            if bool(predicate(event)):
                return index
        return None

    def __len__(self):
        with self._state_lock:
            return len(self._events)

    def __call__(self, event):
        if getattr(self._phase, "value", None) == "predicate":
            raise EventProbeError(
                "trigger predicates must not emit collector events")
        if not isinstance(event, Mapping):
            raise EventProbeError("collector events must be mappings")
        record = _clone(dict(event), "collector event")

        # The callback lock gives concurrently emitting producers one total
        # order for both recording and trigger execution.  It is re-entrant so
        # an action may emit a derived event without deadlocking the harness.
        with self._callback_lock:
            with self._state_lock:
                self._events.append(record)
                triggers = tuple(
                    trigger for trigger in self._triggers.values()
                    if trigger.name not in self._fired)

            decisions = []
            previous_phase = getattr(self._phase, "value", None)
            self._phase.value = "predicate"
            try:
                for trigger in triggers:
                    try:
                        hit = bool(trigger.predicate(
                            _clone(record, "trigger event")))
                    except BaseException as exc:
                        # A predicate failure is terminal for this named
                        # trigger; its unset handle remains the bounded
                        # failure signal if the producer suppresses the error.
                        decisions.append((trigger, exc))
                    else:
                        if hit:
                            decisions.append((trigger, None))
            finally:
                self._phase.value = previous_phase

            # Reserve every same-event match before the first action.  A
            # re-entrant action therefore cannot steal a later trigger from
            # the event that matched it first.
            with self._state_lock:
                claimed = []
                for trigger, predicate_error in decisions:
                    if trigger.name not in self._fired:
                        self._fired.add(trigger.name)
                        claimed.append((trigger, predicate_error))

            errors = []
            failed = set()

            # Capture every snapshot at the event boundary before any action
            # can mutate the external harness state another snapshot observes.
            for trigger, predicate_error in claimed:
                if predicate_error is not None:
                    failed.add(trigger.name)
                    errors.append(predicate_error)
                    continue
                if trigger.snapshot is not None:
                    try:
                        captured = trigger.snapshot(
                            _clone(record, "snapshot event"))
                        captured = _clone(captured, "snapshot result")
                        with self._state_lock:
                            self._snapshots[trigger.name] = captured
                    except BaseException as exc:
                        failed.add(trigger.name)
                        errors.append(exc)

            # Snapshot failure suppresses only that trigger's action.  Other
            # reserved actions still run in registration order, so their
            # successful handles remain useful even when the producer later
            # observes (or suppresses) the first exact callback exception.
            for trigger, predicate_error in claimed:
                if predicate_error is not None or trigger.name in failed:
                    continue
                if trigger.action is not None:
                    try:
                        trigger.action(_clone(record, "action event"))
                    except BaseException as exc:
                        failed.add(trigger.name)
                        errors.append(exc)

            for trigger, _predicate_error in claimed:
                if trigger.name not in failed:
                    trigger.handle.set()

            # Every successful reserved callback has completed before its
            # handle is visible.  Failed handles intentionally remain unset;
            # the harness timeout is their failure guard if a producer catches
            # the callback exception.
            if errors:
                raise errors[0]

        return _clone(record, "collector return event")

    def _registered(self, name):
        name = _name(name)
        with self._state_lock:
            trigger = self._triggers.get(name)
        if trigger is None:
            raise EventProbeError(f"unknown trigger {name!r}")
        return trigger
