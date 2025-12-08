import requests
import time
import sys
from datetime import datetime

# Configuration
URL = "http://localhost:8080/xrpc/app.vylet.feed.getActorPosts?actor=hailey.at"
INITIAL_DELAY = 5.0
MIN_DELAY = 0.001
RAMP_FACTOR = 0.5


def make_request(session, request_num, delay):
    try:
        start_time = time.time()
        response = session.get(URL, timeout=10)
        elapsed = time.time() - start_time

        timestamp = datetime.now().strftime("%H:%M:%S")
        rate = 1 / delay if delay > 0 else float("inf")

        print(
            f"[{timestamp}] Request #{request_num:4d} | "
            f"Status: {response.status_code} | "
            f"Time: {elapsed:.3f}s | "
            f"Rate: {rate:.2f} req/s"
        )

        return True
    except requests.exceptions.RequestException as e:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] Request #{request_num:4d} | ERROR: {e}")
        return False


def main():
    print("=" * 70)
    print("Gradual Load Tester")
    print("=" * 70)
    print(f"Target URL: {URL}")
    print(f"Starting delay: {INITIAL_DELAY}s ({1 / INITIAL_DELAY:.2f} req/s)")
    print(f"Min delay: {MIN_DELAY}s ({1 / MIN_DELAY:.2f} req/s)")
    print(
        f"Ramp factor: {RAMP_FACTOR} (gets {(1 - RAMP_FACTOR) * 100:.0f}% faster each cycle)"
    )
    print("=" * 70)
    print("\nPress Ctrl+C to stop\n")

    session = requests.Session()
    delay = INITIAL_DELAY
    request_num = 0

    try:
        while True:
            request_num += 1
            make_request(session, request_num, delay)

            time.sleep(delay)

            if delay > MIN_DELAY:
                delay = max(delay * RAMP_FACTOR, MIN_DELAY)
                if delay == MIN_DELAY:
                    print(f"\n{'=' * 70}")
                    print(f"Reached maximum rate: {1 / MIN_DELAY:.2f} requests/second")
                    print(f"{'=' * 70}\n")

    except KeyboardInterrupt:
        print(f"\n\n{'=' * 70}")
        print(f"Stopped after {request_num} requests")
        print(f"Final rate: {1 / delay:.2f} requests/second")
        print(f"{'=' * 70}")
        sys.exit(0)


if __name__ == "__main__":
    main()
