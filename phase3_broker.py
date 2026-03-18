#!/usr/bin/env python3
"""
Phase 3 Broker
- Accepts subscriber and publisher connections
- Tracks topic subscriptions
- Forwards published messages to subscribers
"""

from __future__ import annotations
import argparse
import json
import socket
import struct
import threading
from collections import defaultdict


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


class Broker:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.subscribers = defaultdict(set)   # topic -> set of sockets
        self.names = {}                       # socket -> name
        self.lock = threading.Lock()

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(50)
        print(f"[BROKER] Listening on {self.host}:{self.port}")

        while True:
            client_sock, addr = server.accept()
            print(f"[BROKER] Accepted {addr}")
            threading.Thread(target=self.handle_client, args=(client_sock,), daemon=True).start()

    def handle_client(self, sock: socket.socket):
        try:
            while True:
                msg = recv_msg(sock)
                mtype = msg.get("type")

                if mtype == "HELLO":
                    name = msg.get("from", "unknown")
                    with self.lock:
                        self.names[sock] = name
                    print(f"[BROKER] HELLO from {name}")

                elif mtype == "SUBSCRIBE":
                    topic = msg.get("topic")
                    with self.lock:
                        self.subscribers[topic].add(sock)
                    print(f"[BROKER] {self.names.get(sock, 'unknown')} subscribed to {topic}")

                elif mtype == "UNSUBSCRIBE":
                    topic = msg.get("topic")
                    with self.lock:
                        self.subscribers[topic].discard(sock)
                    print(f"[BROKER] {self.names.get(sock, 'unknown')} unsubscribed from {topic}")

                elif mtype == "PUBLISH":
                    topic = msg.get("topic")
                    with self.lock:
                        targets = list(self.subscribers.get(topic, set()))
                    print(f"[BROKER] publish topic={topic} to {len(targets)} subscribers")
                    for target in targets:
                        try:
                            send_msg(target, msg)
                        except Exception:
                            pass

                elif mtype == "LIST":
                    with self.lock:
                        result = {
                            topic: len(socks) for topic, socks in self.subscribers.items()
                        }
                    send_msg(sock, {"type": "TOPICS", "topics": result})

        except Exception:
            self.cleanup(sock)

    def cleanup(self, sock: socket.socket):
        with self.lock:
            for topic in self.subscribers:
                self.subscribers[topic].discard(sock)
            if sock in self.names:
                print(f"[BROKER] disconnected {self.names[sock]}")
                del self.names[sock]
        try:
            sock.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6000)
    args = parser.parse_args()

    broker = Broker(args.host, args.port)
    broker.start()


if __name__ == "__main__":
    main()