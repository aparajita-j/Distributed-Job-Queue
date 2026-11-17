import socket
import time

HOST = "10.0.0.2"
PORT = 5000

def submit_job(expression):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((HOST, PORT))
        sock.sendall((f"SUBMIT|{expression}\n").encode("utf-8"))
        reader = sock.makefile("r", encoding="utf-8", newline="\n")
        response = reader.readline().strip()
        reader.close()
        return response
    except Exception as e:
        return f"ERROR: {e}"
    finally:
        sock.close()

def main():
    print("=== AUTOMATED STRESS & QUEUE TEST CLIENT ===")
    print("Submitting jobs every 3 seconds. Watch the server terminal!")
    
    job_num = 1
    try:
        while True:
            expr = f"{job_num} * 10 + 5"
            print(f"Sending job: {expr}...", end="", flush=True)
            res = submit_job(expr)
            print(f" Response: {res}")
            job_num += 1
            time.sleep(3.0)  # Gap between submissions
    except KeyboardInterrupt:
        print("\nTest client stopped.")

if __name__ == "__main__":
    main()
