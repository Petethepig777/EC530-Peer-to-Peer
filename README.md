Name: Youwei Chen
BU Email ID: ychen143@bu.edu
Homework: EC530 Peer-to-Peer Node System

Files included:

* p2p_phase1.py
* phase2_structured_events.py
* phase3_node.py
* phase3_broker.py

How to run:

* Place all files in the same directory.
* Open 4 terminals and navigate to the folder.

Node1:

* python phase2_structured_events.py --name Node1 --port 5001 --role general

Node2:

* python phase2_structured_events.py --name Node2 --port 5002 --role add --connect 127.0.0.1:5001

Node3:

* python phase2_structured_events.py --name Node3 --port 5003 --role div --connect 127.0.0.1:5001

Node4:

* python phase2_structured_events.py --name Node4 --port 5004 --role mul --connect 127.0.0.1:5001


Running time:

* All scripts run in real time with minimal delay.

Comments:

* Phase 1 implements basic peer-to-peer communication.
* Phase 2 supports structured command parsing and role-based execution.
* Phase 3 adds event-driven responses, where nodes return results using structured events.

Dependencies:

* Python 3.x
* No external libraries required.
