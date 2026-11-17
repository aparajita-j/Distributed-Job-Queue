import ast
import operator
import socket
import sys
import threading
import time

HOST = "10.0.0.2"
PORT = 5000

BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
UNARY_OPS = {ast.UAdd: operator.pos, ast.USub: operator.neg}
send_lock = threading.Lock()
busy_event = threading.Event()


def send_line(sock, message):
    with send_lock:
        sock.sendall((message + "\n").encode("utf-8"))


def eval_node(node):
    if isinstance(node, ast.Constant) and type(node.value) in (int, float):
        return node.value
    if isinstance(node, ast.BinOp) and type(node.op) in BIN_OPS:
        left = eval_node(node.left)
        right = eval_node(node.right)
        if isinstance(node.op, ast.Pow) and abs(right) > 10:
            raise ValueError("exponent too large (maximum absolute value is 10)")
        value = BIN_OPS[type(node.op)](left, right)
        if isinstance(value, (int, float)) and abs(value) > 10**100:
            raise ValueError("result too large")
        return value
    if isinstance(node, ast.UnaryOp) and type(node.op) in UNARY_OPS:
        return UNARY_OPS[type(node.op)](eval_node(node.operand))
    raise ValueError("only arithmetic expressions are supported")


def calculate(expression):
    try:
        return str(eval_node(ast.parse(expression, mode="eval").body))
    except Exception as error:
        return "ERROR: " + str(error)


def heartbeat(sock, stop_event, base_load):
    while not stop_event.is_set():
        # Demo load score, not actual CPU utilization. Stage 4 can add OS metrics.
        load = min(100, base_load + (65 if busy_event.is_set() else 0))
        try:
            send_line(sock, f"HEARTBEAT|{load}")
        except OSError:
            return
        if stop_event.wait(2.0):
            return


def main():
    if len(sys.argv) != 2:
        print("Usage: py worker.py W1")
        return

    worker_id = sys.argv[1]
    # Different baseline values make load-aware scheduling easy to demonstrate.
    try:
        worker_number = int(worker_id[1:]) if worker_id[1:].isdigit() else 1
    except ValueError:
        worker_number = 1
    base_load = min(60, 10 + max(0, worker_number - 1) * 15)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    reader = sock.makefile("r", encoding="utf-8", newline="\n")
    send_line(sock, f"REGISTER|{worker_id}")
    response = reader.readline().strip()
    print(response)
    if not response.startswith("REGISTERED|"):
        reader.close()
        sock.close()
        return

    print(f"[{worker_id}] Ready. Demo baseline load: {base_load}%.")
    print(f"[{worker_id}] Press Ctrl+C to simulate worker shutdown.")
    stop_event = threading.Event()
    threading.Thread(
        target=heartbeat,
        args=(sock, stop_event, base_load),
        daemon=True,
    ).start()

    try:
        while True:
            line = reader.readline()
            if not line:
                print(f"[{worker_id}] Server disconnected.")
                break
            message = line.strip()
            if not message or not message.startswith("JOB|"):
                continue
            parts = message.split("|", 2)
            if len(parts) != 3:
                continue
            _, job_id, operation = parts
            busy_event.set()
            print(f"[{worker_id}] Received {job_id}: {operation}", flush=True)
            if operation.startswith("WAIT:"):
                try:
                    seconds = float(operation.split(":", 1)[1])
                    if not 1 <= seconds <= 60:
                        raise ValueError("WAIT duration must be between 1 and 60 seconds")
                    print(f"[{worker_id}] Simulating a long job for {seconds:g} seconds.", flush=True)
                    time.sleep(seconds)
                    result = f"WAITED {seconds:g} seconds"
                except (ValueError, OverflowError) as error:
                    result = "ERROR: " + str(error)
            else:
                time.sleep(0.7)  # Visible processing delay for the classroom demo.
                result = calculate(operation)
            try:
                send_line(sock, f"RESULT|{job_id}|{result}")
                print(f"[{worker_id}] Completed {job_id}: {result}", flush=True)
            except OSError:
                print(f"[{worker_id}] Could not return result; connection lost.", flush=True)
                break
            finally:
                busy_event.clear()
    except KeyboardInterrupt:
        print(f"\n[{worker_id}] Stopping...")
    finally:
        stop_event.set()
        reader.close()
        sock.close()


if __name__ == "__main__":
    main()


