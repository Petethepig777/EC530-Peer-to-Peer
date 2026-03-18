#!/usr/bin/env python3
"""
Phase 2: Structured Events / Commands
- Still direct peer-to-peer
- Supports COMMAND and EVENT messages
- Example commands:
    AddNumbers
    MultiplyNumbers
    DivideNumbers
- Example events:
    AdditionRequested
    MultiplicationRequested
    DivisionRequested
    CalculationCompleted
    CalculationFailed

Example:
Terminal A:
  python phase2_structured_events.py --name Node1 --port 5001 --connect 127.0.0.1:5002 --connect 127.0.0.1:5003 --connect 127.0.0.1:5004

Terminal B:
  python phase2_structured_events.py --name Node2 --port 5002 --role add --connect 127.0.0.1:5001

Terminal C:
  python phase2_structured_events.py --name Node3 --port 5003 --role div --connect 127.0.0.1:5001

Terminal D:
  python phase2_structured_events.py --name Node4 --port 5004 --role mul --connect 127.0.0.1:5001
"""

from __future__ import annotations
import argparse
import json
import socket
import struct
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional, Tuple


def recvall(sock: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Socket closed")
        data += chunk
    return data


def send_msg(sock: socket.socket, msg: dict) -> None:
    payload = json.dumps(msg).encode("utf-8")
    header = struct.pack("!I", len(payload))
    sock.sendall(header + payload)


def recv_msg(sock: socket.socket) -> dict:
    header = recvall(sock, 4)
    (length,) = struct.unpack("!I", header)
    payload = recvall(sock, length)
    return json.loads(payload.decode("utf-8"))


@dataclass
class Conn:
    sock: socket.socket
    addr: Tuple[str, int]
    peer_name: str = "unknown"


class Phase2Node:
    def __init__(self, name: str, host: str, port: int, role: str = "general") -> None:
        self.name = name
        self.host = host
        self.port = port
        self.role = role

        self._server_sock: Optional[socket.socket] = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._conns: Dict[int, Conn] = {}

    def start(self) -> None:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(20)

        threading.Thread(target=self._accept_loop, daemon=True).start()
        print(f"[{self.name}] Listening on {self.host}:{self.port} (role={self.role})")

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            conns = list(self._conns.values())
            self._conns.clear()

        for conn in conns:
            try:
                conn.sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                conn.sock.close()
            except Exception:
                pass

        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass

    def _accept_loop(self) -> None:
        assert self._server_sock is not None
        while not self._stop.is_set():
            try:
                client_sock, addr = self._server_sock.accept()
            except OSError:
                break

            conn = Conn(sock=client_sock, addr=addr)
            with self._lock:
                self._conns[id(client_sock)] = conn

            print(f"[{self.name}] Accepted from {addr}")
            threading.Thread(target=self._recv_loop, args=(conn,), daemon=True).start()
            self._send_hello(conn)

    def connect_to(self, host: str, port: int) -> None:
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
        threading.Thread(target=self._recv_loop, args=(conn,), daemon=True).start()
        self._send_hello(conn)

    def _send_hello(self, conn: Conn) -> None:
        msg = {
            "type": "HELLO",
            "from": self.name,
            "role": self.role,
            "ts": time.time(),
        }
        try:
            send_msg(conn.sock, msg)
        except Exception:
            self._drop(conn)

    def broadcast_event(self, event_name: str, payload: dict) -> None:
        msg = {
            "type": "EVENT",
            "event": event_name,
            "message_id": str(uuid.uuid4()),
            "from": self.name,
            "payload": payload,
            "ts": time.time(),
        }
        self._broadcast(msg)

    def send_command(self, target_name: str, command_name: str, payload: dict) -> None:
        msg = {
            "type": "COMMAND",
            "command": command_name,
            "message_id": str(uuid.uuid4()),
            "from": self.name,
            "to": target_name,
            "payload": payload,
            "ts": time.time(),
        }
        self._broadcast(msg)

    def _broadcast(self, msg: dict) -> None:
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
        while not self._stop.is_set():
            try:
                msg = recv_msg(conn.sock)
            except Exception:
                self._drop(conn)
                return

            mtype = msg.get("type", "UNKNOWN")

            if mtype == "HELLO":
                conn.peer_name = msg.get("from", "unknown")
                print(f"[{self.name}] Handshake with {conn.peer_name} @ {conn.addr}")
                continue

            if mtype == "COMMAND":
                self._handle_command(msg)
                continue

            if mtype == "EVENT":
                self._handle_event(msg)
                continue

            print(f"[{self.name}] Unknown message: {msg}")

    def _handle_command(self, msg: dict) -> None:
        target = msg.get("to")
        sender = msg.get("from")
        command = msg.get("command")
        payload = msg.get("payload", {})
        message_id = msg.get("message_id")

        if target != self.name:
            return

        print(f"[{self.name}] COMMAND from {sender}: {command} -> {payload}")

        try:
            if command == "AddNumbers" and self.role == "add":
                a = float(payload["a"])
                b = float(payload["b"])
                result = a + b
                self.broadcast_event("CalculationCompleted", {
                    "original_message_id": message_id,
                    "operation": "addition",
                    "result": result,
                    "handled_by": self.name
                })

            elif command == "MultiplyNumbers" and self.role == "mul":
                a = float(payload["a"])
                b = float(payload["b"])
                result = a * b
                self.broadcast_event("CalculationCompleted", {
                    "original_message_id": message_id,
                    "operation": "multiplication",
                    "result": result,
                    "handled_by": self.name
                })

            elif command == "DivideNumbers" and self.role == "div":
                a = float(payload["a"])
                b = float(payload["b"])
                if b == 0:
                    raise ValueError("division by zero")
                result = a / b
                self.broadcast_event("CalculationCompleted", {
                    "original_message_id": message_id,
                    "operation": "division",
                    "result": result,
                    "handled_by": self.name
                })

            else:
                self.broadcast_event("CalculationFailed", {
                    "original_message_id": message_id,
                    "reason": f"{self.name} cannot handle command {command}",
                    "handled_by": self.name
                })

        except Exception as e:
            self.broadcast_event("CalculationFailed", {
                "original_message_id": message_id,
                "reason": str(e),
                "handled_by": self.name
            })

    def _handle_event(self, msg: dict) -> None:
        sender = msg.get("from")
        event = msg.get("event")
        payload = msg.get("payload", {})
        print(f"[{self.name}] EVENT from {sender}: {event} -> {payload}")

    def _drop(self, conn: Conn) -> None:
        with self._lock:
            self._conns.pop(id(conn.sock), None)
        try:
            conn.sock.close()
        except Exception:
            pass
        print(f"[{self.name}] Disconnected {conn.peer_name} @ {conn.addr}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--name", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--role", default="general", choices=["general", "add", "mul", "div"])
    p.add_argument("--connect", action="append", default=[])
    return p.parse_args()


def main():
    args = parse_args()
    node = Phase2Node(args.name, args.host, args.port, args.role)
    node.start()

    for hp in args.connect:
        try:
            h, p = hp.split(":")
            node.connect_to(h, int(p))
        except ValueError:
            print(f"[{args.name}] Invalid --connect value: {hp}")

    print(f"[{args.name}] Commands:")
    print("  /cmd Node2 AddNumbers a=5 b=17")
    print("  /cmd Node3 DivideNumbers a=7 b=9")
    print("  /cmd Node4 MultiplyNumbers a=9 b=5")
    print("  /event EventName key=value key=value")
    print("  /quit")

    try:
        while True:
            line = input().strip()
            if not line:
                continue
            if line == "/quit":
                break

            if line.startswith("/cmd "):
                parts = line.split()
                if len(parts) < 3:
                    print("Usage: /cmd TargetNode CommandName a=... b=...")
                    continue
                target = parts[1]
                command = parts[2]
                payload = {}
                for x in parts[3:]:
                    if "=" in x:
                        k, v = x.split("=", 1)
                        payload[k] = v
                node.send_command(target, command, payload)
                continue

            if line.startswith("/event "):
                parts = line.split()
                if len(parts) < 2:
                    print("Usage: /event EventName key=value ...")
                    continue
                event_name = parts[1]
                payload = {}
                for x in parts[2:]:
                    if "=" in x:
                        k, v = x.split("=", 1)
                        payload[k] = v
                node.broadcast_event(event_name, payload)
                continue

            print("Unknown command.")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        node.stop()
        print(f"[{args.name}] Bye.")


if __name__ == "__main__":
    main()