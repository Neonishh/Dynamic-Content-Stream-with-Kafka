# consumer.py (Linux/Mac version with real-time key unsubscribe)
import json
import logging
import time
import threading
import requests
import sys
import select
import tty
import termios
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ---------- CONFIG ----------
BROKER = "10.147.17.172:9092"
ADMIN_API = "http://10.147.17.174:5000"
USER = input("👤 Enter your username: ").strip() or "anonymous_user"

REFRESH_INTERVAL = 5
POLL_TIMEOUT_MS = 1000

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

desired_topics = set()
desired_lock = threading.Lock()
rebind_event = threading.Event()
stop_event = threading.Event()

# ---------- TTY SETUP ----------
old_settings = termios.tcgetattr(sys.stdin)

def get_key_nonblocking():
    """Return a single key press if pressed, otherwise None."""
    dr, dw, de = select.select([sys.stdin], [], [], 0)
    if dr:
        return sys.stdin.read(1)
    return None


def fetch_active_topics():
    try:
        r = requests.get(f"{ADMIN_API}/api/active_topics", timeout=5)
        if r.status_code == 200:
            return set(r.json())
        return set()
    except:
        return set()


def choose_topics(active_topics):
    topics = sorted(list(active_topics))
    print("\n📡 Active topics:\n")
    for i, t in enumerate(topics, 1):
        print(f"  {i}. {t}")
    print("  0. Subscribe to ALL topics")

    choice = input("👉 Enter topic numbers (e.g., 1,3) or 0 for ALL: ").strip()

    if choice == "0":
        return set(topics)
    try:
        idx = [int(x) for x in choice.split(",")]
        selected = {topics[i - 1] for i in idx if 1 <= i <= len(topics)}
        if selected:
            return selected
    except:
        pass
    return set(topics)


def notify_admin_subscriptions(user, topics):
    for topic in topics:
        try:
            requests.post(f"{ADMIN_API}/api/subscribe",
                          json={"user_name": user, "topic_name": topic}, timeout=5)
        except:
            pass


def notify_admin_unsubscribe(user, topics):
    for topic in topics:
        try:
            requests.post(f"{ADMIN_API}/api/unsubscribe",
                          json={"user_name": user, "topic_name": topic}, timeout=5)
        except:
            pass


def watcher_thread_func(user_wants):
    logging.info("Watcher started.")
    while not stop_event.is_set():
        active = fetch_active_topics()
        with desired_lock:
            new_desired = set(user_wants).intersection(active)
            if new_desired != desired_topics:
                desired_topics.clear()
                desired_topics.update(new_desired)
                rebind_event.set()
        time.sleep(REFRESH_INTERVAL)


def keypress_listener(user_wants):
    """Live unsubscribe: press a number to unsubscribe."""
    tty.setcbreak(sys.stdin.fileno())
    try:
        while not stop_event.is_set():
            key = get_key_nonblocking()
            if not key:
                time.sleep(0.1)
                continue

            with desired_lock:
                topics_list = sorted(list(user_wants))

                if key == "0":
                    notify_admin_unsubscribe(USER, user_wants.copy())
                    user_wants.clear()
                    print("\n🚫 Unsubscribed from ALL topics")
                    rebind_event.set()

                elif key.isdigit() and 1 <= int(key) <= len(topics_list):
                    topic = topics_list[int(key) - 1]
                    user_wants.remove(topic)
                    notify_admin_unsubscribe(USER, {topic})
                    print(f"\n🚫 Unsubscribed from: {topic}")
                    rebind_event.set()

    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)


def build_consumer():
    return KafkaConsumer(
        bootstrap_servers=[BROKER],
        group_id=f"{USER}_group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000
    )


def main():
    user_wants = set()
    while True:
        active = fetch_active_topics()
        if active:
            user_wants = choose_topics(active)
            break
        time.sleep(3)

    notify_admin_subscriptions(USER, user_wants)

    watcher = threading.Thread(target=watcher_thread_func, args=(user_wants,), daemon=True)
    watcher.start()

    keylistener = threading.Thread(target=keypress_listener, args=(user_wants,), daemon=True)
    keylistener.start()

    consumer = build_consumer()

    print("\n🎮 LIVE CONTROLS (NO ENTER REQUIRED):")
    print("  Press 1,2,3... → Unsubscribe from that topic number")
    print("  Press 0 → Unsubscribe from ALL topics\n")

    try:
        while not stop_event.is_set():
            if rebind_event.is_set():
                with desired_lock:
                    to_sub = list(desired_topics)
                if to_sub:
                    logging.info("🔄 Subscribing to: %s", to_sub)
                    consumer.subscribe(to_sub)
                else:
                    logging.info("🚫 No topics left → Unsubscribing")
                    consumer.unsubscribe()
                rebind_event.clear()

            polled = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            for tp, messages in polled.items():
                for msg in messages:
                    print(f"\n📩 [{msg.topic}] {msg.value.get('title','No title')}")

    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        consumer.close()
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        print("\n👋 Consumer stopped.")


if __name__ == "__main__":
    main()

