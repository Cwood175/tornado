import socket
import typing

import pycares  # type: ignore

from tornado import gen
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.netutil import Resolver, is_valid_ip

if typing.TYPE_CHECKING:
    from typing import Any, Dict, Generator, List, Tuple  # noqa: F401


class CaresResolver(Resolver):
    """Name resolver based on the c-ares library.

    This is a non-blocking and non-threaded resolver.  It may not produce the
    same results as the system resolver, but can be used for non-blocking
    resolution when threads cannot be used.

    ``pycares`` will not return a mix of ``AF_INET`` and ``AF_INET6`` when
    ``family`` is ``AF_UNSPEC``, so it is only recommended for use in
    ``AF_INET`` (i.e. IPv4).  This is the default for
    ``tornado.simple_httpclient``, but other libraries may default to
    ``AF_UNSPEC``.

    .. versionchanged:: 5.0
       The ``io_loop`` argument (deprecated since version 4.1) has been removed.

    .. deprecated:: 6.2
       This class is deprecated and will be removed in Tornado 7.0. Use the default
       thread-based resolver instead.
    """

    def initialize(self) -> None:
        self.io_loop = IOLoop.current()
        self.channel = pycares.Channel(sock_state_cb=self._sock_state_cb)
        self.fds = {}  # type: Dict[int, int]

    def _sock_state_cb(self, fd: int, readable: bool, writable: bool) -> None:
        state = (IOLoop.READ if readable else 0) | (IOLoop.WRITE if writable else 0)
        if not state:
            self.io_loop.remove_handler(fd)
            del self.fds[fd]
        elif fd in self.fds:
            self.io_loop.update_handler(fd, state)
            self.fds[fd] = state
        else:
            self.io_loop.add_handler(fd, self._handle_events, state)
            self.fds[fd] = state

    def _handle_events(self, fd: int, events: int) -> None:
        read_fd = pycares.ARES_SOCKET_BAD
        write_fd = pycares.ARES_SOCKET_BAD
        if events & IOLoop.READ:
            read_fd = fd
        if events & IOLoop.WRITE:
            write_fd = fd
        self.channel.process_fd(read_fd, write_fd)

    @gen.coroutine
    def resolve(
        self, host: str, port: int, family: int = 0
    ) -> "Generator[Any, Any, List[Tuple[int, Any]]]":
        if is_valid_ip(host):
            addresses = [host]
        else:
            # gethostbyname doesn't take callback as a kwarg
            fut = Future()  # type: Future[Tuple[Any, Any]]
            # Prefer gethostbyname if available, fall back to getaddrinfo or socket.getaddrinfo
            if hasattr(self.channel, "gethostbyname"):
                self.channel.gethostbyname(
                    host, family, lambda result, error: fut.set_result((result, error))
                )
                result, error = yield fut
                if error:
                    raise OSError(
                        "C-Ares returned error %s: %s while resolving %s"
                        % (error, pycares.errno.strerror(error), host)
                    )
                addresses = result.addresses
            elif hasattr(self.channel, "getaddrinfo"):
                # Use getaddrinfo if gethostbyname is not available
                self.channel.getaddrinfo(
                    host,
                    None,
                    family,
                    0,
                    0,
                    0,
                    lambda result, error: fut.set_result((result, error)),
                )
                result, error = yield fut
                if error:
                    raise OSError(
                        "C-Ares returned error %s: %s while resolving %s"
                        % (error, pycares.errno.strerror(error), host)
                    )
                addresses = []
                for item in result:
                    if isinstance(item, dict):
                        addr = (
                            item.get("address")
                            or item.get("addr")
                            or item.get("host")
                            or item.get("name")
                        )
                        if addr:
                            addresses.append(addr)
                    elif isinstance(item, (tuple, list)):
                        # Try common tuple/list shapes
                        # e.g. (family, address, ... ) or sockaddr tuples
                        if len(item) >= 2 and isinstance(item[1], str):
                            addresses.append(item[1])
                        elif len(item) >= 1 and isinstance(item[0], str):
                            addresses.append(item[0])
                if not addresses:
                    raise OSError(
                        "C-Ares returned no addresses while resolving %s" % (host,)
                    )
            else:
                # Fallback to synchronous resolution if pycares Channel lacks expected methods
                try:
                    infos = socket.getaddrinfo(host, None, family, 0, 0)
                except socket.gaierror as e:
                    raise OSError(
                        "getaddrinfo failed: %s while resolving %s" % (e, host)
                    )
                addresses = []
                for info in infos:
                    # info: (family, socktype, proto, canonname, sockaddr)
                    sockaddr = info[4]
                    addr = sockaddr[0] if isinstance(sockaddr, tuple) else sockaddr
                    addresses.append(addr)
        addrinfo = []
        for address in addresses:
            if "." in address:
                address_family = socket.AF_INET
            elif ":" in address:
                address_family = socket.AF_INET6
            else:
                address_family = socket.AF_UNSPEC
            if family != socket.AF_UNSPEC and family != address_family:
                raise OSError(
                    "Requested socket family %d but got %d" % (family, address_family)
                )
            addrinfo.append((typing.cast(int, address_family), (address, port)))
        return addrinfo
