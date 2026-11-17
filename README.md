# Distributed-Job-Queue
A distributed job system built with Python TCP sockets and Mininet. Clients send math jobs to a central server, which shares them among worker nodes. If workers crash, the server safely queues new jobs and automatically resumes processing once the workers reconnect, keeping the system fully stable and fault-tolerant
