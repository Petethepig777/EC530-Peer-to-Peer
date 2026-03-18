#!/usr/bin/env python3
"""
Phase 3 Node
- Connects to broker
- Can subscribe/unsubscribe to topics
- Can publish to topics
- Receives published messages from broker

Example:
Terminal 1:
  python phase3_broker.py --port 6000

Terminal 2:
  python phase3_node.py --name Node1 --broker-port 6000

Terminal 3:
  python phase3_node.py --name Node2 --broker-port 6000
"""

from __future__ import annotations
import argparse
import json
import socket
import struct
import threading
import time
import uuid


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


class PubSubNode:
    def __init__(self, name: str, broker_host: str, broker_port: int):
        self.name = name
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.sock = None

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.broker_host, self.broker_port))
        send_msg(self.sock, {"type": "HELLO", "from": self.name, "ts": time.time()})
        threading.Thread(target=self.recv_loop, daemon=True).start()
        print(f"[{self.name}] Connected to broker at {self.broker_host}:{self.broker_port}")

    def subscribe(self, topic: str):
        send_msg(self.sock, {"type": "SUBSCRIBE", "from": self.name, "topic": topic})
        print(f"[{self.name}] subscribed to {topic}")

    def unsubscribe(self, topic: str):
        send_msg(self.sock, {"type": "UNSUBSCRIBE", "from": self.name, "topic": topic})
        print(f"[{self.name}] unsubscribed from {topic}")

    def publish(self, topic: str, event_name: str, payload: dict):
        send_msg(self.sock, {
            "type": "PUBLISH",
            "from": self.name,
            "topic": topic,
            "event": event_name,
            "message_id": str(uuid.uuid4()),
            "payload": payload,
            "ts": time.time(),
        })
        print(f"[{self.name}] published {event_name} to {topic}")

    def list_topics(self):
        send_msg(self.sock, {"type": "LIST", "from": self.name})

    def recv_loop(self):
        try:
            while True:
                msg = recv_msg(self.sock)
                mtype = msg.get("type")

                if mtype == "PUBLISH":
                    print(f"[{self.name}] RECEIVED topic={msg.get('topic')} event={msg.get('event')} payload={msg.get('payload')}")
                elif mtype == "TOPICS":
                    print(f"[{self.name}] TOPICS -> {msg.get('topics')}")
                else:
                    print(f"[{self.name}] {msg}")
        except Exception:
            print(f"[{self.name}] disconnected from broker")

    def stop(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--broker-host", default="127.0.0.1")
    parser.add_argument("--broker-port", type=int, default=6000)
    args = parser.parse_args()

    node = PubSubNode(args.name, args.broker_host, args.broker_port)
    node.start()

    print(f"[{args.name}] Commands:")
    print("  /sub topic")
    print("  /unsub topic")
    print("  /pub topic EventName key=value key=value")
    print("  /topics")
    print("  /quit")

    try:
        while True:
            line = input().strip()
            if not line:
                continue
            if line == "/quit":
                break
            if line == "/topics":
                node.list_topics()
                continue
            if line.startswith("/sub "):
                node.subscribe(line.split(maxsplit=1)[1])
                continue
            if line.startswith("/unsub "):
                node.unsubscribe(line.split(maxsplit=1)[1])
                continue
            if line.startswith("/pub "):
                parts = line.split()
                if len(parts) < 3:
                    print("Usage: /pub topic EventName key=value ...")
                    continue
                topic = parts[1]
                event_name = parts[2]
                payload = {}
                for x in parts[3:]:
                    if "=" in x:
                        k, v = x.split("=", 1)
                        payload[k] = v
                node.publish(topic, event_name, payload)
                continue
            print("Unknown command.")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        node.stop()
        print(f"[{args.name}] Bye.")


if __name__ == "__main__":
    main()