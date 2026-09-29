"""Request-local HTTPS mechanics; the guarded caller owns the physical open.

No root, capability, callback injection or activation API is provided here.
Only name lookup can outlive a timed-out caller, never a connection or HTTP send.
"""

import contextvars
import http.client
import math
import socket
import ssl
import threading
import time
import urllib.request
from urllib.error import HTTPError

from fetch_authority import AuthorityError


_DNS_SLOT = threading.BoundedSemaphore(1)
_HOST = "stockanalysis.com"


def _interrupt_socket(sock):
    # urllib closes its socket wrapper after giving the response a file ref.
    # The descriptor remains live until that file closes. Use the base shutdown
    # to interrupt recv without SSLSocket's Python-level closed-wrapper check;
    # only the owner thread closes the handle and disposes the SSL object.
    try:
        socket.socket.shutdown(sock, socket.SHUT_RDWR)
    except OSError:
        pass  # Already closed, detached during TLS, or not yet connected.


class Deadline:
    """One monotonic budget, including slow HTTP header and body reads."""

    def __init__(self, seconds):
        if type(seconds) not in (int, float) or not math.isfinite(seconds) or not 0 < seconds <= 120:
            raise AuthorityError("invalid StockAnalysis attempt timeout")
        self.end = time.monotonic() + seconds
        self._lock = threading.Lock()
        self._sockets = []
        self._expired = False
        self._closed = False
        self._timer = threading.Timer(seconds, self._expire)
        self._timer.daemon = True

    def remaining(self):
        value = self.end - time.monotonic()
        if self._closed or self._expired or value <= 0:
            raise TimeoutError("StockAnalysis total attempt deadline expired")
        return value

    def _expire(self):
        with self._lock:
            if self._closed:
                return
            self._expired = True
            sockets = tuple(self._sockets)
        for sock in sockets:
            _interrupt_socket(sock)

    def track(self, sock):
        with self._lock:
            self._sockets.append(sock)
            expired = self._expired or self._closed or time.monotonic() >= self.end
        if expired:
            _interrupt_socket(sock)
        self.remaining()

    def __enter__(self):
        self.remaining()
        self._timer.start()
        return self

    def __exit__(self, *_exc):
        with self._lock:
            self._closed = True
        self._timer.cancel()
        self._timer.join()
        self._sockets.clear()


def _lookup_addresses():
    return socket.getaddrinfo(_HOST, 443, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP)


def resolve(deadline):
    """Bound caller waiting and outstanding DNS work; never launch a sender."""
    deadline.remaining()
    if not _DNS_SLOT.acquire(blocking=False):
        raise AuthorityError("StockAnalysis DNS resolver is still occupied")
    done, result = threading.Event(), []

    def lookup_only():
        try:
            addresses = _lookup_addresses()
            if not isinstance(addresses, list) or not 1 <= len(addresses) <= 32:
                raise AuthorityError("StockAnalysis DNS address count is invalid")
            result.append((True, addresses))
        except BaseException as exc:
            result.append((False, exc))
        finally:
            _DNS_SLOT.release()
            done.set()

    try:
        worker = threading.Thread(target=contextvars.Context().run, args=(lookup_only,),
                                  name="stockanalysis-dns", daemon=True)
        worker.start()
    except BaseException:
        _DNS_SLOT.release()
        raise
    if not done.wait(deadline.remaining()):
        raise TimeoutError("StockAnalysis DNS exceeded total attempt deadline")
    deadline.remaining()  # A late result is never permission to connect.
    succeeded, value = result[0]
    if not succeeded:
        raise value
    return value


def _make_socket(family, kind, protocol):
    return socket.socket(family, kind, protocol)


def connect_resolved(deadline, address, _timeout, source_address=None):
    if address != (_HOST, 443) or source_address is not None:
        raise AuthorityError("StockAnalysis connection target is not fixed")
    addresses = resolve(deadline)
    last = None
    for family, kind, protocol, _canonical, target in addresses:
        if family not in (socket.AF_INET, socket.AF_INET6) or kind != socket.SOCK_STREAM or protocol != socket.IPPROTO_TCP:
            raise AuthorityError("StockAnalysis DNS address is unsupported")
        if not isinstance(target, tuple) or len(target) not in (2, 4) or target[1] != 443:
            raise AuthorityError("StockAnalysis DNS port differs")
        sock = _make_socket(family, kind, protocol)
        try:
            deadline.track(sock)
            sock.settimeout(deadline.remaining())
            sock.connect(target)
            # HTTPSConnection's TLS handshake inherits this *remaining* budget.
            sock.settimeout(deadline.remaining())
            return sock
        except BaseException as exc:
            try:
                sock.close()
            except BaseException:
                raise exc  # Cleanup must not replace the connection failure.
            if not isinstance(exc, OSError):
                raise
            deadline.remaining()
            last = exc
    raise last if last is not None else AuthorityError("StockAnalysis DNS has no usable address")


class DeadlineHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, *, deadline, **kwargs):
        super().__init__(host, **kwargs)
        if self.host != _HOST or self.port != 443:
            raise AuthorityError("StockAnalysis TLS host differs")
        self._deadline = deadline
        self._create_connection = lambda *args: connect_resolved(deadline, *args)

    def connect(self):
        if self._tunnel_host:
            raise AuthorityError("StockAnalysis proxy tunnel is refused")
        self._deadline.remaining()
        # Retain both stdlib connect methods and their offline tripwires.
        super().connect()
        try:
            self._deadline.track(self.sock)
            self.sock.settimeout(self._deadline.remaining())
        except BaseException:
            try:
                self.close()
            except BaseException:
                pass  # Preserve the primary TLS/deadline failure.
            raise


class DeadlineHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, deadline, context):
        super().__init__(context=context)
        self.deadline = deadline

    def https_open(self, request):
        self.deadline.remaining()
        def connection(host, **kwargs):
            return DeadlineHTTPSConnection(host, deadline=self.deadline, **kwargs)
        return self.do_open(connection, request, context=self._context)


class RefuseRedirect(urllib.request.HTTPRedirectHandler):
    def http_error_302(self, request, response, code, message, headers):
        # Do not parse Location, consume the body, or call a second opener.
        raise HTTPError(request.full_url, code, message, headers, response)

    http_error_301 = http_error_303 = http_error_307 = http_error_308 = http_error_302


def _receive_before_deadline(deadline, sock, receiver, *args, **kwargs):
    # Rearm for each raw receive: shutdown alone does not wake Windows reads.
    sock.settimeout(deadline.remaining())
    try:
        value = receiver(sock, *args, **kwargs)
    except OSError as error:
        try:
            deadline.remaining()
        except TimeoutError as expired:
            raise expired from error
        raise
    deadline.remaining()  # Refuse bytes returned after the total budget.
    return value


def build_opener(deadline):
    context = ssl.create_default_context()
    context.set_alpn_protocols(["http/1.1"])
    class AttemptSocket(ssl.SSLSocket):
        # The context and class belong to this attempt; no global socket patch.
        def recv(self, *args, **kwargs):
            return _receive_before_deadline(deadline, self, ssl.SSLSocket.recv, *args, **kwargs)

        def recv_into(self, *args, **kwargs):
            return _receive_before_deadline(deadline, self, ssl.SSLSocket.recv_into, *args, **kwargs)

        def read(self, *args, **kwargs):
            return _receive_before_deadline(deadline, self, ssl.SSLSocket.read, *args, **kwargs)

    context.sslsocket_class = AttemptSocket
    deadline.remaining()
    return urllib.request.build_opener(urllib.request.ProxyHandler({}),
        DeadlineHTTPSHandler(deadline, context), RefuseRedirect())
