"""Offline A2-1 infrastructure, real guards/executor/governors and inverses."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import inspect
import hashlib
import base64
import json
from pathlib import Path
import socket
import tempfile
import textwrap
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import fetch_test_policy as tp

# Install the guard before loading any production module capable of transport.
with tp.offline_transports():
    import fetch_governors as gov
    import fetch_envelopes as envelopes
    import fetch_ibkr_bridge as bridge
    from fetch_authority import AuthorityError, NY, ScheduleAuthority
    from fetch_run_context import FetchRunContext, RequestRefused
    from fetch_ledger import LedgerError, inspect_ledger
    import stock_ibkr as ib
    import fetch_run_context as run_context


class Clock:
    def __init__(self):
        self.value = 0.0
        self.lock = threading.Lock()
        self.waits = []

    def now(self):
        with self.lock:
            return self.value

    def sleep(self, seconds, cancel=None):
        if cancel is not None and cancel.is_set():
            raise ib.Cancelled()
        with self.lock:
            self.waits.append(seconds)
            self.value += seconds


class Infrastructure(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.authority = ScheduleAuthority.load()

    def setUp(self):
        self.guard = tp.offline_transports()
        self.guard.__enter__()
        self.addCleanup(self.guard.__exit__, None, None, None)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.clock = Clock()
        self.registry = gov.GovernorRegistry(time_fn=self.clock.now, sleep_fn=self.clock.sleep)

    def context(self, now="2026-08-04T17:00:00", capability=None, fault=None):
        result = FetchRunContext.create(self.root / "ledgers", authority=self.authority,
            clock=lambda: datetime.fromisoformat(now).replace(tzinfo=NY),
            governors=self.registry, test_capability=capability, fault=fault)
        self.addCleanup(result.ledger.close)
        return result

    def http(self, endpoint="yahoo.splits", start="2026-08-03T00:00:00",
             end="2026-08-05T16:00:00"):
        variant = gov.ENDPOINTS[endpoint].variant
        raw = {"variant": variant, "endpoint": endpoint, "subject": "TEST"}
        if variant == "http-series":
            raw.update(token="1d", intended_start=datetime.fromisoformat(start).replace(tzinfo=NY),
                       intended_end=datetime.fromisoformat(end).replace(tzinfo=NY))
        return raw

    def request(self, ctx, endpoint="yahoo.splits", producer=None, **kwargs):
        producer = producer or {"yahoo.splits": "http.split_provider.yahoo",
            "stockanalysis.history": "http.stockanalysis.validation",
            "sec.companyfacts": "http.split_provider.sec_facts"}[endpoint]
        return ctx.worker("worker").request(producer, self.http(endpoint, **kwargs))

    def execute(self, request, rows=None):
        return request.execute(lambda effective: b"wire", acquire_turn=lambda: -99,
            governor=self.registry.provider(request.envelope.endpoint),
            normalizer=lambda raw: rows if rows is not None else [
                {"timestamp": "2026-08-03T00:00:00-04:00", "value": 1},
                {"timestamp": "2026-08-05T00:00:00-04:00", "value": 2}])

    def test_default_refuses_every_future_endpoint(self):
        ctx = self.context()
        for producer, (variant, endpoint) in tp.TEST_PRODUCERS.items():
            with self.subTest(producer=producer):
                if endpoint is not None:
                    with self.assertRaises(RequestRefused):
                        ctx.worker("w").request(producer, self.http(endpoint))
                elif variant == "ibkr-metadata":
                    method = producer.rsplit(".", 1)[1]
                    with self.assertRaises(RequestRefused):
                        ctx.worker("w").request(producer, {"variant": variant,
                            "method": method, "symbol": "TEST",
                            "con_id": 123 if method == "company_name" else 0})
                else:
                    raw = {"variant": variant, "symbol": "TEST", "con_id": 123,
                           "what_to_show": "TRADES", "use_rth": True}
                    if variant in {"ibkr-bars", "ibkr-bars-unfiltered"}:
                        raw.update(token=("1m-iv" if variant == "ibkr-bars-unfiltered"
                                          else "1m"), bar_size="1 min", duration="1 D",
                            raw_end=datetime(2026, 8, 3, 16, tzinfo=NY),
                            intended_start=datetime(2026, 8, 3, 9, 30, tzinfo=NY),
                            intended_end=datetime(2026, 8, 3, 16, tzinfo=NY))
                        if variant == "ibkr-bars-unfiltered":
                            raw.update(what_to_show="OPTION_IMPLIED_VOLATILITY",
                                       use_rth=False)
                    envelopes.parse_envelope(raw)  # Refusal must be policy, not malformed shape.
                    with self.assertRaises(RequestRefused):
                        ctx.worker("w").request(producer, raw)

    def test_missing_tripwire_refuses(self):
        self.guard.__exit__(None, None, None)
        with self.assertRaisesRegex(AuthorityError, "transport tripwire"):
            with tp.offline_policy():
                self.fail("unconfined policy")

    def test_expired_and_tampered_capabilities_refuse(self):
        with tp.offline_policy() as capability:
            ctx = self.context(capability=capability)
            request = self.request(ctx)
            with patch.object(socket, "getaddrinfo", lambda *a: None):
                with self.assertRaisesRegex(AuthorityError, "intact live"):
                    self.execute(request)
            with self.assertRaisesRegex(RequestRefused, "terminal"):
                self.execute(request)
            unattempted = self.request(ctx)
        with self.assertRaisesRegex(AuthorityError, "expired"):
            self.execute(unattempted)

    def test_capability_expires_when_transport_guard_closes(self):
        with tp.offline_policy() as capability:
            request = self.request(self.context(capability=capability))
            self.guard.__exit__(None, None, None)
            with self.assertRaisesRegex(AuthorityError, "tripwires"):
                self.execute(request)

    def test_forged_capability_refuses(self):
        ctx = self.context(capability=SimpleNamespace(allows=lambda *a: True))
        with self.assertRaisesRegex(RequestRefused, "invalid test-policy"):
            self.request(ctx)

    def test_static_caller_inventory_and_production_inverse(self):
        self.assertEqual(tp.check_callers(), sorted(tp.OFFLINE_SUITES))
        fixtures = ["from fetch_test_policy import offline_policy as p",
                    "import fetch_test_policy as p\np.offline_policy()",
                    "getattr(p, 'offline_policy')()",
                    "from fetch_test_policy import _Policy as p",
                    "import fetch_test_policy as p\np._Policy(guard)",
                    "getattr(p, '_Policy')(guard)",
                    "from fetch_test_policy import _OfflineTransportGuard as g",
                    "p._OfflineTransportGuard([])", "getattr(p, '_OfflineTransportGuard')([])"]
        for source in fixtures:
            with self.subTest(source=source), self.assertRaisesRegex(AuthorityError, "production caller"):
                tp.check_callers({"engine/synthetic_production.py": source})

    def production_constructor(self, guard):
        namespace = {"__file__": str(self.root / "production.py"), "tp": tp}
        exec(compile("def invoke(guard):\n    return tp._Policy(guard)\n",
                     namespace["__file__"], "exec"), namespace)
        return namespace["invoke"](guard)

    def test_direct_constructor_requires_registered_caller(self):
        for guard in (SimpleNamespace(verify=lambda: None), tp._ACTIVE):
            with self.subTest(guard_type=type(guard).__name__):
                with self.assertRaisesRegex(AuthorityError, "not a registered suite"):
                    self.production_constructor(guard)

    def test_direct_constructor_requires_exact_live_guard(self):
        class GuardSubclass(tp._OfflineTransportGuard):
            def verify(self):
                pass

        for guard in (None, SimpleNamespace(verify=lambda: None),
                      tp._OfflineTransportGuard([]), GuardSubclass([])):
            with self.subTest(guard_type=type(guard).__name__):
                with self.assertRaisesRegex(AuthorityError, "live transport tripwires"):
                    tp._Policy(guard)
        with patch.object(socket, "getaddrinfo", lambda *a: None):
            with self.assertRaisesRegex(AuthorityError, "intact live"):
                tp._Policy(tp._ACTIVE)

    def test_constructor_bypassed_capability_refuses_before_transport(self):
        self.guard.__exit__(None, None, None)
        # Reproduce the reviewer's nonregistered probe with construction bypassed
        # deliberately, so the consumer's independent barrier is exercised too.
        namespace = {"__file__": str(self.root / "production.py"), "tp": tp,
                     "fake": SimpleNamespace(verify=lambda: None)}
        exec(compile("def forge():\n    p = object.__new__(tp._Policy)\n"
                     "    p._guard = fake\n    p._live = True\n    return p\n",
                     namespace["__file__"], "exec"), namespace)
        capability = namespace["forge"]()
        ctx = self.context(capability=capability)
        env = envelopes.parse_envelope(self.http("sp500.github"))
        with self.assertRaisesRegex(RequestRefused, "invalid test-policy"):
            ctx.permits("http.sp500.github", env)
        sent = []
        with self.assertRaisesRegex(RequestRefused, "invalid test-policy"):
            req = ctx.worker("w").request("http.sp500.github", self.http("sp500.github"))
            req.execute(lambda effective: sent.append(effective),
                        acquire_turn=lambda: 0,
                        governor=self.registry.provider("sp500.github"))
        self.assertEqual(sent, [])
        with self.assertRaisesRegex(AuthorityError, "tripwires"):
            capability.allows("http.sp500.github", "http-metadata", "sp500.github")

    def test_subclass_and_stale_guard_cannot_override_permits(self):
        class PolicySubclass(tp._Policy):
            def allows(self, *args):
                return True

        with tp.offline_policy() as capability:
            ctx = self.context(capability=capability)
            stale_guard = tp._OfflineTransportGuard([])
            for guard in (stale_guard, SimpleNamespace(verify=lambda: None)):
                original = capability._guard
                capability._guard = guard
                try:
                    with self.assertRaisesRegex(RequestRefused, "invalid test-policy"):
                        self.request(ctx)
                    with self.assertRaisesRegex(AuthorityError, "tripwires"):
                        capability.allows("http.split_provider.yahoo", "http-series", "yahoo.splits")
                finally:
                    capability._guard = original
            with self.assertRaisesRegex(RequestRefused, "invalid test-policy"):
                self.request(self.context(capability=PolicySubclass(tp._ACTIVE)))
            self.assertTrue(self.execute(self.request(ctx)))

    def test_guard_instance_cannot_replace_canonical_verification(self):
        with tp.offline_policy() as capability:
            with self.assertRaises(AttributeError):
                capability._guard.verify = lambda: None
            with self.assertRaises(AttributeError):
                capability.allows = lambda *args: True
            with patch.object(socket, "getaddrinfo", lambda *args: None):
                with self.assertRaisesRegex(AuthorityError, "intact live"):
                    tp.capability_allows(capability, "http.sp500.github", "http-metadata", "sp500.github")

    def test_forged_guard_after_request_refuses_at_execute(self):
        with tp.offline_policy() as capability:
            request = self.request(self.context(capability=capability))
            capability._guard = SimpleNamespace(verify=lambda: None)
            sent = []
            with self.assertRaisesRegex(RequestRefused, "invalid test-policy"):
                request.execute(lambda effective: sent.append(effective),
                                acquire_turn=lambda: 0,
                                governor=self.registry.provider("yahoo.splits"))
            self.assertEqual(sent, [])

    def test_consumer_check_removal_still_hits_canonical_guard(self):
        capability = object.__new__(tp._Policy)
        capability._guard = SimpleNamespace(verify=lambda: None)
        capability._live = True
        ctx = self.context(capability=capability)
        # Removing the consumer predicates alone is redundant: canonical
        # allows still refuses. Removing that guard too exposes the exact bug.
        with patch.object(tp, "is_live_capability", lambda capability: True):
            with self.assertRaisesRegex(AuthorityError, "tripwires"):
                self.request(ctx)
            with patch.object(tp, "_require_guard", lambda guard: None):
                self.assertTrue(self.execute(self.request(ctx)))

    def test_direct_constructor_caller_mutation_exposes_bypass(self):
        source = textwrap.dedent(inspect.getsource(tp._Policy.__init__))
        self.assertEqual(source.count("_check_caller()"), 1)
        namespace = dict(vars(tp))
        exec(compile(source.replace("_check_caller()", "pass"),
                     "<inverse-policy-constructor>", "exec"), namespace)
        with patch.object(tp._Policy, "__init__", namespace["__init__"]):
            # Removing caller enforcement really permits the nonregistered mint
            # under the real guard; production static inventory remains redundant.
            self.assertTrue(tp.is_live_capability(self.production_constructor(tp._ACTIVE)))

    def test_policy_type_static_mutation_exposes_reference(self):
        source = {"engine/synthetic_production.py": "from fetch_test_policy import _Policy as p"}
        with self.assertRaisesRegex(AuthorityError, "production caller"):
            tp.check_callers(source)
        with patch.object(tp, "_API", tp._API - {"_Policy"}):
            self.assertEqual(tp.check_callers(source), [])

    def test_runtime_production_caller_refuses(self):
        namespace = {"__file__": str(self.root / "production.py"), "tp": tp}
        exec(compile("def invoke():\n    with tp.offline_policy():\n        pass\n",
                     namespace["__file__"], "exec"), namespace)
        with self.assertRaisesRegex(AuthorityError, "not a registered suite"):
            namespace["invoke"]()

    def test_transport_tripwire_is_real(self):
        with self.assertRaisesRegex(AuthorityError, "tripwire"):
            socket.getaddrinfo("example.invalid", 443)

    def test_shared_and_isolated_accounts(self):
        a = self.registry.ibkr(["DU111"])
        self.assertIs(a, self.registry.ibkr(["DU111", "DU222"], "DU111"))
        self.assertIsNot(a, self.registry.ibkr(["DU222"]))
        a.wait_turn(metered=False)
        a.wait_turn(metered=False)
        self.assertGreaterEqual(self.clock.now(), 0.15)

    def test_account_evidence_fail_closed(self):
        for accounts, selected in [([], None), (["?"], None), (["unknown"], None),
                (["DU111", "DU222"], None), (["DU111"], "DU222"),
                (["DU111", "DU111"], None), ("DU111", None), ([" DU111"], None)]:
            with self.subTest(accounts=accounts), self.assertRaises(AuthorityError):
                self.registry.ibkr(accounts, selected)

    def test_provider_identity_and_credentials(self):
        a = self.registry.provider("stockanalysis.history")
        self.assertIs(a, self.registry.provider("stockanalysis.history"))
        self.assertIs(self.registry.provider("sec.tickers"), self.registry.provider("sec.companyfacts"))
        self.assertIsNot(a, self.registry.provider("yahoo.splits"))
        private = self.registry.provider("stockanalysis.history", "secret-material")
        self.assertIsNot(a, private)
        self.assertNotIn("secret-material", json.dumps(private.evidence()))
        self.assertEqual(gov.credential_identity("secret-material"), gov.credential_identity(b"secret-material"))

    def test_registry_survives_contexts(self):
        one, two = self.context(), self.context()
        a = one.governors.provider("stockanalysis.history")
        a.wait_turn()
        two.governors.provider("stockanalysis.history").wait_turn()
        self.assertGreaterEqual(self.clock.now(), 1.2)
        self.assertIs(one.governors, two.governors)
        self.assertIs(gov.default_registry(), gov.default_registry())

    def test_default_context_registry_recreation_inverse(self):
        def contexts_share_registry():
            contexts = [FetchRunContext.create(self.root / "default-registry",
                authority=self.authority, clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY))
                for _ in range(2)]
            for ctx in contexts:
                self.addCleanup(ctx.ledger.close)
            return contexts[0].governors is contexts[1].governors
        self.assertTrue(contexts_share_registry())
        source = textwrap.dedent(inspect.getsource(gov.default_registry))
        self.assertEqual(source.count("return _REGISTRY"), 1)
        namespace = dict(vars(gov))
        exec(compile(source.replace("return _REGISTRY", "return GovernorRegistry()"),
                     "<inverse-registry-lifetime>", "exec"), namespace)
        with patch.object(run_context, "default_registry", namespace["default_registry"]):
            self.assertFalse(contexts_share_registry())

    def test_429_saturation_removal_inverse(self):
        def reserves_full_window():
            registry = gov.GovernorRegistry(time_fn=self.clock.now, sleep_fn=self.clock.sleep)
            governor = registry.provider("yahoo.splits")
            governor.http_backoff(429, retry_after="0")
            return len(governor._pacer._stamps) == governor.policy.max_requests
        self.assertTrue(reserves_full_window())
        source = textwrap.dedent(inspect.getsource(gov.http_saturation))
        self.assertEqual(source.count("status == 429"), 1)
        namespace = dict(vars(gov))
        exec(compile(source.replace("status == 429", "False"),
                     "<inverse-429-saturation>", "exec"), namespace)
        with patch.object(gov, "http_saturation", namespace["http_saturation"]):
            self.assertFalse(reserves_full_window())

    def test_same_provider_identity_split_inverse(self):
        def scopes_share():
            registry = gov.GovernorRegistry(time_fn=self.clock.now, sleep_fn=self.clock.sleep)
            return registry.provider("sec.tickers") is registry.provider("sec.companyfacts")
        self.assertTrue(scopes_share())
        source = textwrap.dedent(inspect.getsource(gov.GovernorRegistry.provider))
        self.assertEqual(source.count('("http", endpoint.domain,'), 1)
        namespace = dict(vars(gov))
        exec(compile(source.replace('("http", endpoint.domain,', '("http", endpoint_id,'),
                     "<inverse-provider-identity>", "exec"), namespace)
        with patch.object(gov.GovernorRegistry, "provider", namespace["provider"]):
            self.assertFalse(scopes_share())

    def test_simultaneous_registry_and_reservations(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            governors = list(pool.map(lambda _: self.registry.ibkr(["DU111"]), range(12)))
            waits = list(pool.map(lambda g: g.wait_turn(metered=False), governors))
        self.assertTrue(all(g is governors[0] for g in governors))
        self.assertEqual(len(governors[0]._pacer._burst), 6)
        self.assertGreaterEqual(self.clock.now(), 2.0)
        self.assertTrue(all(w >= 0 for w in waits))

    def test_saturation_and_deadline_isolation(self):
        a, b = self.registry.provider("stockanalysis.history"), self.registry.provider("yahoo.splits")
        a.http_backoff(429, retry_after="10", now=datetime.now(timezone.utc))
        self.assertTrue(a._pacer._forced)
        self.assertEqual(b.wait_turn(), 0)
        a.wait_turn()
        self.assertGreaterEqual(self.clock.now(), 10)

    def test_retry_after_not_truncated_on_giveup(self):
        a = self.registry.provider("sec.tickers")
        with self.assertRaisesRegex(AuthorityError, "wait budget"):
            a.http_backoff(429, retry_after="120", max_wait_s=60)
        self.assertEqual(a._pacer._not_before, 120)
        self.registry.provider("sec.companyfacts").wait_turn()
        self.assertGreaterEqual(self.clock.now(), 120)

    def test_retry_after_dates_and_invalid_values(self):
        now = datetime(2026, 9, 8, 17, tzinfo=timezone.utc)
        self.assertEqual(gov.retry_after_seconds("Tue, 08 Sep 2026 17:02:00 GMT", now), 120)
        self.assertEqual(gov.retry_after_seconds("0", now), 0)
        for value in ("-1", "NaN", "", "1.5", "garbage"):
            with self.subTest(value=value), self.assertRaises(AuthorityError):
                gov.retry_after_seconds(value, now)

    def test_optional_503_and_sec403_are_saturation(self):
        self.assertTrue(gov.http_saturation("yahoo.com", 503, "20"))
        self.assertFalse(gov.http_saturation("yahoo.com", 503))
        self.assertTrue(gov.http_saturation("sec.gov", 403))
        self.assertFalse(gov.http_saturation("yahoo.com", 403))

    def test_cancel_reservation_consumes_no_turn(self):
        governor = self.registry.ibkr(["DU111"])
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(ib.Cancelled):
            governor.wait_turn(cancel)
        self.assertEqual(len(governor._pacer._burst), 0)

    def test_every_policy_floor_weakening_is_rejected(self):
        for domain, policy in gov.HTTP_POLICIES.items():
            endpoint = next(key for key, value in gov.ENDPOINTS.items() if value.domain == domain)
            for field, value in [("min_gap_s", .1), ("max_requests", 61),
                                 ("window_s", 59), ("burst_max", 2),
                                 ("burst_window_s", .1), ("force_metered", False),
                                 ("backoff_base_s", .5), ("backoff_factor", 1.5),
                                 ("backoff_cap_s", 59)]:
                changed = dict(gov.HTTP_POLICIES)
                changed[domain] = replace(policy, **{field: value})
                with self.subTest(domain=domain, field=field), patch.object(gov, "HTTP_POLICIES", changed):
                    with self.assertRaisesRegex(AuthorityError, "weaker"):
                        gov.endpoint_policy(endpoint)
        for field, value in [("min_gap_s", .01), ("max_requests", 59), ("window_s", 599),
                             ("burst_max", 7), ("burst_window_s", 1.9)]:
            with self.subTest(ibkr_field=field), patch.object(
                    gov, "IBKR_POLICY", replace(gov.IBKR_POLICY, **{field: value})):
                with self.assertRaisesRegex(AuthorityError, "weaker"):
                    self.registry.ibkr(["DU111"])

    def test_floor_guard_removal_inverse(self):
        source = textwrap.dedent(inspect.getsource(gov.endpoint_policy))
        needle = 'raise AuthorityError("HTTP policy weaker than reviewed floor")'
        self.assertEqual(source.count(needle), 1)
        namespace = dict(vars(gov))
        exec(compile(source.replace(needle, "pass"), "<inverse-policy-floor>", "exec"), namespace)
        weakened = dict(gov.HTTP_POLICIES)
        weakened["stockanalysis.com"] = replace(weakened["stockanalysis.com"], min_gap_s=.15)
        with patch.object(gov, "HTTP_POLICIES", weakened):
            with self.assertRaisesRegex(AuthorityError, "weaker"):
                gov.endpoint_policy("stockanalysis.history")
        namespace["HTTP_POLICIES"] = weakened
        _, policy = namespace["endpoint_policy"]("stockanalysis.history")
        self.assertLess(policy.min_gap_s, 1.2)

    def test_envelope_shapes_and_endpoint_mismatch(self):
        for endpoint in gov.ENDPOINTS:
            raw = self.http(endpoint)
            self.assertEqual(envelopes.parse_envelope(raw).wire()["endpoint"], endpoint)
            for name, value in (("con_id", 1), ("what_to_show", "TRADES"), ("use_rth", True)):
                with self.subTest(endpoint=endpoint, name=name), self.assertRaises(AuthorityError):
                    envelopes.parse_envelope(dict(raw, **{name: value}))
        with self.assertRaises(AuthorityError):
            envelopes.parse_envelope(dict(self.http(), endpoint="sec.tickers"))

    def test_rangeless_settlement_and_ranged_clamp(self):
        for now, refused in [("2026-08-04T13:20:00", True),
                             ("2026-08-04T16:20:00", False),
                             ("2026-08-08T13:20:00", False)]:
            ctx = self.context(now)
            decision = envelopes.decide(envelopes.parse_envelope(self.http("stockanalysis.history")),
                ctx.authority, ctx.horizons, captured_now=ctx.captured_now)
            self.assertEqual(decision.effective is None, refused)
        ctx = self.context("2026-08-04T13:20:00")
        decision = envelopes.decide(envelopes.parse_envelope(self.http()), ctx.authority,
                                    ctx.horizons, captured_now=ctx.captured_now)
        self.assertEqual(decision.state, "clamped")
        self.assertEqual(decision.effective.intended_end.date(), date(2026, 8, 3))

    def test_http_executor_filters_and_seals_real_evidence(self):
        with tp.offline_policy() as capability:
            ctx = self.context(capability=capability)
            request = self.request(ctx)
            accepted = self.execute(request)
            self.assertEqual(len(accepted), 1)
            ctx.seal()
            events = inspect_ledger(ctx.ledger.path)["events"]
            self.assertEqual(events[0]["payload"]["governor_identity"], ["http", "yahoo.com", "public"])
            result = events[1]["payload"]
            self.assertEqual(result["dropped"], 1)
            self.assertNotEqual(result["observed"]["digest"], result["accepted"]["digest"])
            self.assertEqual(result["raw_response_digest"], hashlib.sha256(b"wire").hexdigest())

    def test_http_refusal_sends_nothing_and_takes_no_turn(self):
        with tp.offline_policy() as capability:
            ctx = self.context("2026-08-04T13:20:00", capability)
            request = self.request(ctx, "stockanalysis.history")
            with self.assertRaises(RequestRefused):
                self.execute(request)
            self.assertEqual(len(self.registry.provider("stockanalysis.history")._pacer._burst), 0)
            ctx.seal()

    def test_http_cancel_during_wait_never_sends_or_reserves(self):
        cancel = threading.Event()
        governor = self.registry.provider("yahoo.splits")
        governor.wait_turn()
        def cancel_sleep(seconds, token):
            self.assertIs(token, cancel)
            cancel.set()
            self.clock.sleep(seconds, token)
        with tp.offline_policy() as capability:
            ctx = self.context(capability=capability)
            sent = []
            with patch.object(governor._pacer, "_sleep", cancel_sleep):
                with self.assertRaises(ib.Cancelled):
                    self.request(ctx).execute(lambda e: sent.append(e) or b"wire",
                        acquire_turn=lambda: 0, governor=governor, cancel=cancel,
                        normalizer=lambda raw: [])
            self.assertEqual(sent, [])
            self.assertEqual(len(governor._pacer._burst), 1)
            ctx.seal()
            self.assertEqual(len(inspect_ledger(ctx.ledger.path)["events"]), 1)

    def test_guard_revoked_during_decision_stops_transport(self):
        with tp.offline_policy() as capability:
            def revoke(kind, phase):
                if (kind, phase) == ("decision", "fsync"):
                    capability._live = False
            ctx = self.context(capability=capability, fault=revoke)
            request = self.request(ctx)
            sent = []
            with self.assertRaisesRegex(AuthorityError, "expired"):
                request.execute(lambda e: sent.append(e) or b"wire",
                    acquire_turn=lambda: 0, governor=self.registry.provider("yahoo.splits"),
                    normalizer=lambda raw: [])
            self.assertEqual(sent, [])
            ctx.seal()
            events = inspect_ledger(ctx.ledger.path)["events"]
            self.assertEqual(events[1]["payload"]["outcome"], "error")

    def test_bound_ib_request_ignores_noop_pacer_and_records_identity(self):
        ctx = self.context()
        adapter = ib.LiveIB()
        adapter.ib = SimpleNamespace(managedAccounts=lambda: ["DU111"], isConnected=lambda: True)
        request = ctx.worker("w").request("ibkr.choke.qualify", {
            "variant": "ibkr-metadata", "method": "qualification", "symbol": "TEST", "con_id": 0})
        bound = bridge.bind_request(adapter, request)
        token = bridge._GOVERNOR.set(None)
        self.addCleanup(bridge._GOVERNOR.reset, token)
        for _ in range(2):
            self.assertEqual(bound.execute(lambda e: {"con_id": 123},
                acquire_turn=lambda: -99), {"con_id": 123})
        ctx.seal()
        decisions = [e for e in inspect_ledger(ctx.ledger.path)["events"] if e["event"] == "decision"]
        self.assertEqual(len(decisions), 2)
        self.assertEqual(decisions[0]["payload"]["governor_identity"], ["ibkr", "DU111"])
        self.assertGreaterEqual(decisions[1]["payload"]["pacer_wait_seconds"], .15)
        self.assertEqual(len(self.registry.ibkr(["DU111"])._pacer._burst), 2)

    def test_bound_ib_account_change_while_waiting_stops_send(self):
        ctx = self.context()
        accounts = ["DU111"]
        adapter = ib.LiveIB()
        adapter.ib = SimpleNamespace(managedAccounts=lambda: list(accounts), isConnected=lambda: True)
        governor = adapter.request_governor(ctx)
        governor.wait_turn()
        def rebind_sleep(seconds, cancel):
            accounts[:] = ["DU222"]
            self.clock.sleep(seconds, cancel)
        request = ctx.worker("w").request("ibkr.choke.qualify", {
            "variant": "ibkr-metadata", "method": "qualification", "symbol": "TEST", "con_id": 0})
        token = bridge._GOVERNOR.set(None)
        self.addCleanup(bridge._GOVERNOR.reset, token)
        sent = []
        with patch.object(governor._pacer, "_sleep", rebind_sleep):
            with self.assertRaisesRegex(RequestRefused, "account changed"):
                bridge.bind_request(adapter, request).execute(lambda e: sent.append(e),
                    acquire_turn=lambda: 0)
        self.assertEqual(sent, [])
        ctx.seal()
        self.assertEqual(inspect_ledger(ctx.ledger.path)["events"][1]["payload"]["outcome"], "error")

    def test_http_metadata_has_raw_digest_no_fake_rows(self):
        with tp.offline_policy() as capability:
            ctx = self.context(capability=capability)
            accepted = self.execute(self.request(ctx, "sec.companyfacts"), {"CIK": 123})
            self.assertEqual(accepted, {"CIK": 123})
            ctx.seal()
            result = inspect_ledger(ctx.ledger.path)["events"][1]["payload"]
            self.assertIn("raw_response_digest", result)
            self.assertNotIn("observed", result)

    def test_http_result_fault_never_publishes(self):
        for kind in ("decision", "result"):
            for phase in ("append", "flush", "fsync"):
                def fault(k, p):
                    if (k, p) == (kind, phase):
                        raise OSError("injected durability fault")
                with self.subTest(kind=kind, phase=phase), tp.offline_policy() as capability:
                    ctx = self.context(capability=capability, fault=fault)
                    request = self.request(ctx)
                    sent, cache = [], {"sentinel": "unchanged"}
                    def transport(effective):
                        sent.append(effective)
                        return b"wire"
                    with self.assertRaises(LedgerError):
                        cache["published"] = request.execute(transport, acquire_turn=lambda: 0,
                            governor=self.registry.provider("yahoo.splits"), normalizer=lambda raw: [])
                    self.assertEqual(cache, {"sentinel": "unchanged"})
                    self.assertEqual(len(sent), 0 if kind == "decision" else 1)

    def test_http_retry_ids_and_canonical_governor(self):
        with tp.offline_policy() as capability:
            ctx = self.context(capability=capability)
            request = self.request(ctx)
            with self.assertRaisesRegex(RequestRefused, "canonical"):
                request.execute(lambda e: b"wire", acquire_turn=lambda: 0,
                    governor=self.registry.provider("sec.tickers"), normalizer=lambda r: [])
            with self.assertRaisesRegex(RequestRefused, "terminal"):
                self.execute(request)
            request = self.request(ctx)
            self.execute(request)
            self.execute(request)
            ctx.seal()
            decisions = [e for e in inspect_ledger(ctx.ledger.path)["events"] if e["event"] == "decision"]
            self.assertEqual(len(decisions), 2)
            self.assertEqual(decisions[0]["logical_id"], decisions[1]["logical_id"])
            self.assertNotEqual(decisions[0]["attempt_id"], decisions[1]["attempt_id"])
            self.assertGreater(decisions[1]["payload"]["pacer_wait_seconds"], 0)

    def test_failed_http_results_keep_lossless_raw_quarantine(self):
        for endpoint in ("yahoo.splits", "sec.companyfacts"):
            for phase in ("append", "flush", "fsync"):
                with self.subTest(endpoint=endpoint, phase=phase), tp.offline_policy() as capability:
                    def fault(kind, stage):
                        if (kind, stage) == ("result", phase):
                            raise OSError("injected result failure")
                    ctx = self.context(capability=capability, fault=fault)
                    request = self.request(ctx, endpoint)
                    wire = b"\xff offline raw response"
                    with self.assertRaises(LedgerError):
                        request.execute(lambda e: wire, acquire_turn=lambda: 0,
                            governor=self.registry.provider(endpoint),
                            normalizer=lambda raw: [] if endpoint == "yahoo.splits" else {"CIK": 123})
                    artifacts = list(ctx.ledger.path.parent.glob(ctx.ledger.path.stem + ".*.quarantine.json"))
                    self.assertEqual(len(artifacts), 1)
                    evidence = json.loads(artifacts[0].read_text(encoding="utf-8"))["response"]
                    self.assertEqual(base64.b64decode(evidence["raw_response_base64"]), wire)
                    with self.assertRaisesRegex(RequestRefused, "terminal"):
                        self.execute(request)

    def test_live_account_binding_revalidates(self):
        ctx = self.context()
        adapter = ib.LiveIB()
        accounts = ["DU111"]
        adapter.ib = SimpleNamespace(managedAccounts=lambda: list(accounts), isConnected=lambda: True)
        a = adapter.request_governor(ctx)
        accounts[:] = ["DU222"]
        b = adapter.request_governor(ctx)
        self.assertIsNot(a, b)
        accounts[:] = ["DU111", "DU222"]
        with self.assertRaises(AuthorityError):
            adapter.request_governor(ctx)
        adapter.selected_account = "DU111"
        self.assertIs(adapter.request_governor(ctx), a)

    def test_worker_account_cannot_switch_between_attempts(self):
        ctx = self.context()
        accounts = ["DU111"]
        adapter = ib.LiveIB()
        adapter.ib = SimpleNamespace(managedAccounts=lambda: list(accounts), isConnected=lambda: True)
        token = bridge._GOVERNOR.set(None)
        self.addCleanup(bridge._GOVERNOR.reset, token)
        sent = []
        def request(worker):
            return ctx.worker(worker).request("ibkr.choke.qualify", {
                "variant": "ibkr-metadata", "method": "qualification", "symbol": "TEST", "con_id": 0})
        def send(request):
            return bridge.bind_request(adapter, request).execute(
                lambda e: sent.append(accounts[0]) or {}, acquire_turn=lambda: 0)
        send(request("same-worker"))
        accounts[:] = ["DU222"]
        retry = request("same-worker")
        with self.assertRaisesRegex(RequestRefused, "explicit new worker"):
            send(retry)
        self.assertEqual(len(self.registry.ibkr(["DU222"])._pacer._burst), 0)
        accounts[:] = ["DU111"]
        with self.assertRaisesRegex(RequestRefused, "terminal"):
            send(retry)
        accounts[:] = ["DU222"]
        send(request("explicit-new-worker"))
        self.assertEqual(sent, ["DU111", "DU222"])
        ctx.seal()

    def test_metadata_producer_method_binding(self):
        with tp.offline_policy() as capability:
            ctx = self.context(capability=capability)
            for producer, method, con_id in [
                    ("ibkr.metadata.company_name", "company_name", 123),
                    ("ibkr.metadata.symbol_search", "symbol_search", 0)]:
                raw = {"variant": "ibkr-metadata", "method": method,
                       "symbol": "TEST", "con_id": con_id}
                request = ctx.worker("w").request(producer, raw)
                self.assertEqual(request.execute(lambda e: {"metadata": True},
                    acquire_turn=lambda: 0), {"metadata": True})
                with self.assertRaises(RequestRefused):
                    ctx.worker("w").request(producer, dict(raw, method="qualification"))
            ctx.seal()

    def test_cancelled_logical_request_cannot_restart(self):
        with tp.offline_policy() as capability:
            ctx = self.context(capability=capability)
            request = self.request(ctx)
            cancel = threading.Event()
            cancel.set()
            with self.assertRaises(ib.Cancelled):
                request.execute(lambda e: self.fail("cancelled transport"),
                    acquire_turn=lambda: 0, governor=self.registry.provider("yahoo.splits"), cancel=cancel)
            cancel.clear()
            with self.assertRaisesRegex(RequestRefused, "terminal"):
                self.execute(request)
            ctx.seal()

    def test_provider_failure_can_retry_same_logical_request(self):
        with tp.offline_policy() as capability:
            ctx = self.context(capability=capability)
            request = self.request(ctx)
            with self.assertRaises(TimeoutError):
                request.execute(lambda e: (_ for _ in ()).throw(TimeoutError("provider timeout")),
                    acquire_turn=lambda: 0, governor=self.registry.provider("yahoo.splits"))
            self.assertEqual(len(self.execute(request)), 1)
            ctx.seal()
            events = inspect_ledger(ctx.ledger.path)["events"]
            self.assertEqual(events[0]["logical_id"], events[2]["logical_id"])
            self.assertNotEqual(events[0]["attempt_id"], events[2]["attempt_id"])

    def test_local_cancel_and_status_survive_canonical_pacing(self):
        ctx = self.context()
        adapter = ib.LiveIB()
        adapter.ib = SimpleNamespace(managedAccounts=lambda: ["DU111"], isConnected=lambda: True)
        governor = adapter.request_governor(ctx)
        governor.wait_turn()
        request = ctx.worker("w").request("ibkr.choke.qualify", {
            "variant": "ibkr-metadata", "method": "qualification", "symbol": "TEST", "con_id": 0})
        cancel = threading.Event()
        statuses = []
        def observer(waiting, info):
            statuses.append(waiting)
            self.assertIsNone(bridge.current_worker())
            if waiting:
                cancel.set()
        token = bridge._GOVERNOR.set(None)
        self.addCleanup(bridge._GOVERNOR.reset, token)
        with bridge.send_scope(request, lambda: self.fail("noncanonical pacer invoked"),
                               cancel=cancel, on_wait=observer):
            attempt = bridge.take("ibkr-metadata", SimpleNamespace(conId=0))
            with self.assertRaises(ib.Cancelled):
                bridge.bind_request(adapter, attempt.request).execute(
                    lambda e: self.fail("cancelled transport"), acquire_turn=attempt.acquire_turn)
        self.assertEqual(statuses, [True, False])
        self.assertEqual(len(governor._pacer._burst), 1)
        ctx.seal()

    def test_factory_preserves_explicit_account_selection(self):
        sentinel = object()
        with patch.object(ib, "LiveIB") as factory:
            factory.return_value.connect.return_value = sentinel
            self.assertIs(ib.live_adapter_factory(selected_account="DU222")(), sentinel)
            self.assertEqual(factory.call_args.kwargs["selected_account"], "DU222")


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(Infrastructure))
    print(f"{result.testsRun} checks, {len(result.errors) + len(result.failures)} failed")
    raise SystemExit(not result.wasSuccessful())
