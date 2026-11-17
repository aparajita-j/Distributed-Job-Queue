import queue
import socket
import threading
import time

HOST = "0.0.0.0"
PORT = 5000
HEARTBEAT_TIMEOUT = 8.0

job_queue = queue.Queue()
workers = {}
jobs = {}
job_counter = 0
state_lock = threading.Lock()


def send_line(conn, message):
    conn.sendall((message + "\n").encode("utf-8"))


def requeue_job(job_id, reason):
    """Put a running job back in the queue at most once."""
    with state_lock:
        job = jobs.get(job_id)
        if job and job["status"] == "RUNNING":
            job["status"] = "QUEUED"
            job["worker"] = None
            job_queue.put(job_id)
            print(
                f"[RECOVERY] {job_id} returned to queue ({reason}). "
                f"Queue size: {job_queue.qsize()}", flush=True
            )


def remove_worker(worker_id, conn, reason="connection closed"):
    """Remove the matching worker connection and recover its active job."""
    active_job = None
    removed = False
    with state_lock:
        info = workers.get(worker_id)
        if info is not None and info["conn"] is conn:
            active_job = info["current_job"]
            del workers[worker_id]
            removed = True
    if removed:
        print(f"[WORKER] {worker_id} removed ({reason}).", flush=True)
        if active_job:
            requeue_job(active_job, f"worker {worker_id} {reason}")


def dispatch_jobs():
    """Assign queued jobs to the idle worker with the lowest demo load score."""
    while True:
        assignment = None
        with state_lock:
            idle_workers = [
                (worker_id, info)
                for worker_id, info in workers.items()
                if info["status"] == "IDLE"
            ]
            idle_workers.sort(key=lambda item: item[1]["load"])
            if idle_workers:
                try:
                    job_id = job_queue.get_nowait()
                except queue.Empty:
                    job_id = None
                if job_id is not None:
                    job = jobs.get(job_id)
                    if job is not None and job["status"] == "QUEUED":
                        worker_id, info = idle_workers[0]
                        info["status"] = "BUSY"
                        info["current_job"] = job_id
                        job["status"] = "RUNNING"
                        job["worker"] = worker_id
                        assignment = (worker_id, info["conn"], job_id, job["operation"])
        if assignment:
            worker_id, worker_conn, job_id, operation = assignment
            try:
                send_line(worker_conn, f"JOB|{job_id}|{operation}")
                print(f"[SCHEDULER] Assigned {job_id} to {worker_id}.", flush=True)
            except OSError:
                print(f"[SCHEDULER] Failed to send {job_id} to {worker_id}.", flush=True)
                remove_worker(worker_id, worker_conn, "send failed")
        else:
            time.sleep(0.1)


def handle_worker(conn, reader, worker_id, address):
    with state_lock:
        if worker_id in workers:
            send_line(conn, f"ERROR|Worker ID {worker_id} is already registered")
            return
        workers[worker_id] = {
            "conn": conn,
            "address": address,
            "status": "IDLE",
            "current_job": None,
            "last_seen": time.monotonic(),
            "load": 20.0,
        }
    send_line(conn, f"REGISTERED|{worker_id}")
    print(f"[WORKER] {worker_id} registered from {address}.", flush=True)

    try:
        while True:
            line = reader.readline()
            if not line:
                break
            message = line.strip()
            if not message:
                continue

            if message.startswith("HEARTBEAT|"):
                parts = message.split("|", 1)
                try:
                    load_value = float(parts[1])
                    load_value = max(0.0, min(100.0, load_value))
                except ValueError:
                    load_value = 50.0
                with state_lock:
                    info = workers.get(worker_id)
                    if info is not None and info["conn"] is conn:
                        info["last_seen"] = time.monotonic()
                        info["load"] = load_value
                continue

            if message.startswith("RESULT|"):
                parts = message.split("|", 2)
                if len(parts) != 3:
                    continue
                _, job_id, result = parts
                with state_lock:
                    info = workers.get(worker_id)
                    job = jobs.get(job_id)
                    if (
                        info is not None
                        and info["conn"] is conn
                        and job is not None
                        and info["current_job"] == job_id
                        and job["status"] == "RUNNING"
                    ):
                        job["status"] = "COMPLETED"
                        job["result"] = result
                        info["status"] = "IDLE"
                        info["current_job"] = None
                        print(
                            f"[RESULT] {job_id} completed by {worker_id}: {result}",
                            flush=True,
                        )
    finally:
        remove_worker(worker_id, conn)


def handle_client_request(conn, message):
    global job_counter

    if message.startswith("SUBMIT|"):
        operation = message.split("|", 1)[1].strip()
        if not operation:
            send_line(conn, "ERROR|Job cannot be empty")
            return
        with state_lock:
            job_counter += 1
            job_id = f"JOB{job_counter:03d}"
            jobs[job_id] = {
                "id": job_id,
                "operation": operation,
                "status": "QUEUED",
                "worker": None,
                "result": None,
            }
            job_queue.put(job_id)
            queued_count = job_queue.qsize()
            
            # Check if any workers are registered at all
            active_workers_count = len(workers)

        send_line(conn, f"ACK|{job_id}")
        
        # Explicitly log queue buffering if workers are down
        if active_workers_count == 0:
            print(
                f"[QUEUE BUFFER] {job_id} ({operation}) queued. Reason: All workers offline. Queue size: {queued_count}",
                flush=True,
            )
        else:
            print(
                f"[JOB] Received {job_id}: {operation}. Queue size: {queued_count}.",
                flush=True,
            )
        return

    if message.startswith("STATUS|"):
        job_id = message.split("|", 1)[1].strip()
        with state_lock:
            job = jobs.get(job_id)
            if job is None:
                response = f"STATUS|{job_id}|NOT_FOUND|-|-"
            else:
                worker_id = job["worker"] if job["worker"] else "-"
                result = job["result"] if job["result"] is not None else "-"
                response = f"STATUS|{job_id}|{job['status']}|{worker_id}|{result}"
        send_line(conn, response)
        return

    if message == "WORKERS":
        with state_lock:
            entries = [
                f"{worker_id}:{info['status']}:load={info['load']:.0f}%"
                for worker_id, info in sorted(workers.items())
            ]
        send_line(conn, "WORKERS|" + (";".join(entries) if entries else "none"))
        return

    send_line(conn, "ERROR|Use SUBMIT|expression, STATUS|JOB001, or WORKERS")


def handle_connection(conn, address):
    reader = None
    try:
        reader = conn.makefile("r", encoding="utf-8", newline="\n")
        first_line = reader.readline()
        if not first_line:
            return
        message = first_line.strip()
        if message.startswith("REGISTER|"):
            worker_id = message.split("|", 1)[1].strip()
            if not worker_id:
                send_line(conn, "ERROR|Worker ID cannot be empty")
                return
            handle_worker(conn, reader, worker_id, address)
        else:
            handle_client_request(conn, message)
    except (ConnectionError, OSError, UnicodeError) as error:
        print(f"[CONNECTION] {address}: {error}", flush=True)
    finally:
        if reader is not None:
            try:
                reader.close()
            except OSError:
                pass
        try:
            conn.close()
        except OSError:
            pass


def monitor_workers():
    """Detect workers that stop sending heartbeats and recover active jobs."""
    while True:
        time.sleep(1.0)
        now = time.monotonic()
        stale_workers = []
        with state_lock:
            for worker_id, info in workers.items():
                if now - info["last_seen"] > HEARTBEAT_TIMEOUT:
                    stale_workers.append((worker_id, info["conn"]))
        for worker_id, conn in stale_workers:
            print(f"[FAILURE] {worker_id} heartbeat timed out.", flush=True)
            remove_worker(worker_id, conn, "heartbeat timeout")
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(50)

    threading.Thread(target=dispatch_jobs, daemon=True).start()
    threading.Thread(target=monitor_workers, daemon=True).start()

    print("=" * 52, flush=True)
    print(" DISTRIBUTED JOB QUEUE SERVER - STAGE 3", flush=True)
    print("=" * 52, flush=True)
    print(f"Listening on {HOST}:{PORT}", flush=True)
    print("Worker heartbeat timeout:", HEARTBEAT_TIMEOUT, "seconds", flush=True)
    print("Start workers, then use one or more clients.", flush=True)
    print("Press Ctrl+C to stop the server.\n", flush=True)

    try:
        while True:
            conn, address = server.accept()
            threading.Thread(
                target=handle_connection,
                args=(conn, address),
                daemon=True,
            ).start()
    except KeyboardInterrupt:
        print("\nServer shutting down...", flush=True)
    finally:
        server.close()


if __name__ == "__main__":
    main()


