# Distributed Job Queue and Worker Management System

A distributed job system built with Python TCP sockets and Mininet. Client sends jobs periodically to a central server, which shares them among worker nodes. If workers crash, the server safely queues new jobs and automatically resumes processing once the workers reconnect, keeping the system fully stable and fault-tolerant.

---

## Repository Structure

```text
├── topology.py             # Mininet network topology setup (hosts, switches, TCLinks)
├── server.py               # Central broker/server managing TCP connections, queue, and heartbeats
├── worker.py               # Worker node implementation with load calculation and execution
├── load_test_client.py     # Automated stress-test client submitting jobs every 3 seconds
└── README.md               # Project documentation and reproduction instructions
