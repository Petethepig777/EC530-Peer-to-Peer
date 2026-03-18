#!/usr/bin/env python3
"""
PHASE 1 (Simple & Clear): Basic Peer-to-Peer Chat System
- Python sockets
- Each node runs:
  (1) a server accept loop (thread) to accept incoming connections
  (2) a receiver loop (thread) per connection to receive messages asynchronously
  (3) a main input loop to send messages (broadcast to all peers)

Run (2 terminals):

Terminal A:
  python p2p_phase1_simple.py --name A --port 5001 --connect 127.0.0.1:5002

Terminal B:
  python p2p_phase1_simple.py --name B --port 5002 --connect 127.0.0.1:5001

Type messages and press Enter. Use /quit to exit.
"""

from __future__ import annotations

import argparse
import json
import socket
import struct
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple


# -----------------------------
# (A) Minimal, robust message framing: length-prefixed JSON
# -----------------------------

def recvall(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes; raise ConnectionError if the socket closes."""
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Socket closed")
        data += chunk
    return data


def send_msg(sock: socket.socket, msg: dict) -> None:
    """Send one JSON message with a 4-byte big-endian length prefix."""
    payload = json.dumps(msg, ensure_ascii=False).encode("utf-8")
    header = struct.pack("!I", len(payload))
    sock.sendall(header + payload)


def recv_msg(sock: socket.socket) -> dict:
    """Receive one length-prefixed JSON message."""
    header = recvall(sock, 4)
    (length,) = struct.unpack("!I", header)
    payload = recvall(sock, length)
    return json.loads(payload.decode("utf-8"))


# -----------------------------
# (B) Connection record
# -----------------------------

@dataclass
class Conn:
    sock: socket.socket
    addr: Tuple[str, int]
    peer_name: str = "unknown"


# -----------------------------
# (C) Phase 1 Node
# -----------------------------

class Phase1Node:
    def __init__(self, name: str, host: str, port: int) -> None:
        self.name = name
        self.host = host
        self.port = port

        self._server_sock: Optional[socket.socket] = None
        self._stop = threading.Event()

        self._lock = threading.Lock()
        self._conns: Dict[int, Conn] = {}  # key = id(sock)

    # ---- Start/Stop ----

    def start(self) -> None:
        """Start listening server in a background thread."""
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(20)

        threading.Thread(target=self._accept_loop, daemon=True).start()
        print(f"[{self.name}] Listening on {self.host}:{self.port}")

    def stop(self) -> None:
        """Stop all threads and close sockets."""
        self._stop.set()

        # Close all peer connections
        with self._lock:
            conns = list(self._conns.values())
            self._conns.clear()

        for c in conns:
            try:
                c.sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                c.sock.close()
            except Exception:
                pass

        # Close server socket
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass

    # ---- Server role: accept incoming connections ----

    def _accept_loop(self) -> None:
        """Accept incoming connections forever (until stop)."""
        assert self._server_sock is not None
        while not self._stop.is_set():
            try:
                client_sock, addr = self._server_sock.accept()
            except OSError:
                break  # socket closed

            conn = Conn(sock=client_sock, addr=addr)

            with self._lock:
                self._conns[id(client_sock)] = conn

            print(f"[{self.name}] Accepted from {addr}")

            # Start receiver thread for this connection (async receive)
            threading.Thread(target=self._recv_loop, args=(conn,), daemon=True).start()

            # Minimal handshake (optional but helps in demo output)
            self._send_hello(conn)

    # ---- Client role: connect to another peer ----

    def connect_to(self, host: str, port: int) -> None:
        """Connect out to a peer and start its receiver thread."""
        try:
            sock_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_.connect((host, port))
        except Exception as e:
            print(f"[{self.name}] Connect failed to {host}:{port} -> {e}")
            return

        conn = Conn(sock=sock_, addr=(host, port))

        with self._lock:
            self._conns[id(sock_)] = conn

        print(f"[{self.name}] Connected to {host}:{port}")

        # Start receiver thread for this connection (async receive)
        threading.Thread(target=self._recv_loop, args=(conn,), daemon=True).start()

        # Send hello
        self._send_hello(conn)

    # ---- Messaging ----

    def _send_hello(self, conn: Conn) -> None:
        msg = {
            "type": "HELLO",
            "from": self.name,
            "ts": time.time(),
        }
        try:
            send_msg(conn.sock, msg)
        except Exception:
            self._drop(conn)

    def broadcast_chat(self, text: str) -> None:
        """Send a chat message to all currently connected peers."""
        msg = {
            "type": "CHAT",
            "from": self.name,
            "ts": time.time(),
            "text": text,
        }

        with self._lock:
            conns = list(self._conns.values())

        if not conns:
            print(f"[{self.name}] (no peers connected)")
            return

        for conn in conns:
            try:
                send_msg(conn.sock, msg)
            except Exception:
                self._drop(conn)

    def _recv_loop(self, conn: Conn) -> None:
        """Receive messages from one peer forever (until disconnect)."""
        while not self._stop.is_set():
            try:
                msg = recv_msg(conn.sock)
            except Exception:
                self._drop(conn)
                return

            mtype = msg.get("type", "UNKNOWN")
            sender = msg.get("from", "unknown")

            if mtype == "HELLO":
                conn.peer_name = sender
                print(f"[{self.name}] Handshake with {conn.peer_name} @ {conn.addr}")
            elif mtype == "CHAT":
                text = msg.get("text", "")
                print(f"[{self.name}] <{sender}> {text}")
            else:
                print(f"[{self.name}] Received {mtype} from {sender}: {msg}")

    def _drop(self, conn: Conn) -> None:
        """Remove a connection and close its socket."""
        with self._lock:
            self._conns.pop(id(conn.sock), None)
        try:
            conn.sock.close()
        except Exception:
            pass
        print(f"[{self.name}] Disconnected {conn.peer_name} @ {conn.addr}")


# -----------------------------
# (D) CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--name", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument(
        "--connect",
        action="append",
        default=[],
        help="Peer as host:port (can be repeated)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    node = Phase1Node(args.name, args.host, args.port)
    node.start()

    # Optional outgoing connections (hardcoded via CLI)
    for hp in args.connect:
        try:
            h, p = hp.split(":")
            node.connect_to(h, int(p))
        except ValueError:
            print(f"[{args.name}] Invalid --connect '{hp}' (expected host:port)")

    print(f"[{args.name}] Type messages to broadcast. Use /quit to exit.")

    try:
        while True:
            line = input().strip()
            if not line:
                continue
            if line == "/quit":
                break
            node.broadcast_chat(line)
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        node.stop()
        print(f"[{args.name}] Bye.")


if __name__ == "__main__":
    main()