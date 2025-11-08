"""
============================================================
Dynamic Kafka Producer (Node 1)
Big Data 2025 – UE23CS343AB2 Project 2
------------------------------------------------------------
Implements:
 - Topic Watcher Thread  → Auto topic creation/deletion via Admin API
 - Publisher Thread      → Publishes messages to Kafka topics
 - Streamer Thread       → Streams Reddit/mock data continuously
 - Input Listener Thread → Accepts live/file input
------------------------------------------------------------
Author: <Your Name>
Node: Producer (Node 1)
============================================================
"""

import threading
import queue
import json
import time
import logging
import requests
import praw
import random
import os
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaTimeoutError, NoBrokersAvailable

# ============================================================
# CONFIGURATION
# ============================================================
BROKER = "10.147.17.172:9092"            # Kafka Broker (Node 2)
ADMIN_API = "http://10.147.17.174:5000"  # Admin + DB Node (Node 4)
POLL_INTERVAL = 5                        # Poll frequency for Admin API

# Reddit API credentials
REDDIT_CLIENT_ID = "4FNBhv-I8IGnNCUmjDDZNg"
REDDIT_SECRET = "4yZSw1I6AWQxJmy-MM3kMVaWtTae2g"
REDDIT_USER_AGENT = "KafkaStreamApp:v1.0"

# Predefined Topic → Subreddit mapping
TOPIC_SUBREDDITS = {
    "catmemes": "CatMemes",
    "funny-animals": "FunnyAnimals",
    "technology": "technology",
    "wholesomememes": "WholesomeMemes",
    "gaming": "gaming",
    "worldnews": "worldnews",
}

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S",
)

# ============================================================
# GLOBALS
# ============================================================
msg_queue = queue.Queue(maxsize=1000)
stop_event = threading.Event()
active_topics = set()
seen_posts = set()

# ============================================================
# REDDIT CLIENT
# ============================================================
try:
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )
    logging.info("✅ Reddit API initialized successfully")
except Exception as e:
    logging.error("❌ Failed to initialize Reddit API: %s", e)
    reddit = None

# ============================================================
# KAFKA CLIENT INITIALIZATION
# ============================================================
def init_kafka_clients():
    """Initialize Kafka Admin and Producer clients."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=BROKER, client_id="admin-client")
        producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            acks="all",
        )
        logging.info("✅ Connected to Kafka broker at %s", BROKER)
        return admin, producer
    except NoBrokersAvailable:
        logging.error("❌ Kafka broker unavailable at %s. Start Kafka first.", BROKER)
        raise

admin_client, producer = init_kafka_clients()

# ============================================================
# THREAD 1 – TOPIC WATCHER
# ============================================================
def topic_watcher():
    """Continuously polls Admin API for approved topics and creates/deletes them in Kafka."""
    logging.info("🔍 Topic Watcher started (poll interval: %ss)", POLL_INTERVAL)
    global active_topics

    while not stop_event.is_set():
        try:
            # Safe fetch with retry protection
            try:
                resp = requests.get(f"{ADMIN_API}/api/approved_topics", timeout=10)
                resp.raise_for_status()
                approved_topics = set(resp.json())
            except Exception as e:
                logging.warning("⚠️ Admin API unavailable (%s). Skipping this poll cycle.", e)
                time.sleep(POLL_INTERVAL)
                continue

            existing_topics = set(admin_client.list_topics())

            # === CREATE NEW TOPICS ===
            for topic in approved_topics:
                if topic not in existing_topics:
                    try:
                        admin_client.create_topics([
                            NewTopic(name=topic, num_partitions=3, replication_factor=1)
                        ])
                        logging.info("🆕 Created new Kafka topic: %s", topic)
                        requests.post(f"{ADMIN_API}/api/mark_active/{topic}")
                        active_topics.add(topic)
                    except TopicAlreadyExistsError:
                        active_topics.add(topic)
                    except KafkaTimeoutError:
                        logging.warning("⚠️ Timeout creating topic %s", topic)
                    except Exception as e:
                        logging.error("❌ Error creating topic %s: %s", topic, e)
                elif topic not in active_topics:
                    # Reactivate existing topic
                    requests.post(f"{ADMIN_API}/api/mark_active/{topic}")
                    active_topics.add(topic)

            # === REMOVE DEACTIVATED TOPICS (ONLY IF EXPLICITLY REMOVED BY ADMIN) ===
            for topic in list(active_topics):
                if topic not in approved_topics:
                    logging.info("🗑️ Topic removed (deactivated by Admin): %s", topic)
                    active_topics.remove(topic)

        except Exception as e:
            logging.error("❌ Topic Watcher error: %s", e)

        time.sleep(POLL_INTERVAL)

# ============================================================
# THREAD 2 – PUBLISHER
# ============================================================
def publisher_worker():
    """Reads from queue and publishes messages to Kafka topics."""
    logging.info("🚀 Publisher thread started.")
    while not stop_event.is_set():
        try:
            topic, payload = msg_queue.get(timeout=2)
            if topic not in active_topics:
                continue
            producer.send(topic, value=payload)
            producer.flush()
            logging.info("📤 Published → [%s]: %s", topic, payload.get("title", "No title"))
        except queue.Empty:
            continue
        except Exception as e:
            logging.error("❌ Publisher error: %s", e)
            time.sleep(2)

# ============================================================
# THREAD 3 – DATA STREAMER (REDDIT)
# ============================================================
def data_streamer():
    """Continuously fetches live Reddit or mock posts for each active topic."""
    logging.info("📰 Data Streamer started.")
    global seen_posts

    while not stop_event.is_set():
        if not active_topics:
            time.sleep(5)
            continue

        for topic in list(active_topics):
            subreddit_name = TOPIC_SUBREDDITS.get(topic, topic)
            try:
                if reddit is None:
                    raise Exception("Reddit API not initialized")

                subreddit = reddit.subreddit(subreddit_name)
                for post in subreddit.new(limit=5):  # newest posts to avoid repetition
                    if post.id in seen_posts:
                        continue
                    seen_posts.add(post.id)

                    data = {
                        "title": post.title,
                        "author": str(post.author),
                        "score": post.score,
                        "url": post.url,
                        "timestamp": time.time(),
                        "source": "reddit",
                    }
                    msg_queue.put((topic, data))
                    logging.info("🆕 Queued Reddit post → topic=%s (r/%s)", topic, subreddit_name)

            except Exception as e:
                logging.warning("⚠️ Reddit fetch failed for %s: %s", subreddit_name, e)
                mock_data = {
                    "title": f"Generated mock post for {topic}",
                    "score": random.randint(1, 1000),
                    "timestamp": time.time(),
                    "source": "mock",
                }
                msg_queue.put((topic, mock_data))

        time.sleep(10)

# ============================================================
# THREAD 4 – INPUT LISTENER
# ============================================================
def input_listener():
    """Accepts manual or file input for manual message injection."""
    logging.info("🎧 Input Listener started. Type 'topic: message' or 'file:<path>' to load file.")
    while not stop_event.is_set():
        try:
            user_input = input("> ").strip()
            if not user_input:
                continue

            # Load messages from file
            if user_input.startswith("file:"):
                path = user_input.split("file:", 1)[1].strip()
                if not os.path.exists(path):
                    logging.error("❌ File not found: %s", path)
                    continue
                with open(path, "r") as f:
                    for line in f:
                        if ":" in line:
                            topic, msg = line.strip().split(":", 1)
                            payload = {
                                "title": msg,
                                "timestamp": time.time(),
                                "source": "file",
                            }
                            msg_queue.put((topic.strip(), payload))
                            logging.info("📁 Queued from file → %s", topic)
                continue

            # Manual input
            if ":" in user_input:
                topic, msg = user_input.split(":", 1)
                topic, msg = topic.strip(), msg.strip()
                if topic and msg:
                    payload = {
                        "title": msg,
                        "timestamp": time.time(),
                        "source": "manual",
                    }
                    msg_queue.put((topic, payload))
                    logging.info("🧠 Queued manual message → %s", topic)
                else:
                    logging.warning("⚠️ Invalid input format.")
            else:
                logging.warning("⚠️ Use 'topic: message' format.")
        except Exception as e:
            logging.error("❌ Input listener error: %s", e)

# ============================================================
# MAIN FUNCTION
# ============================================================
if __name__ == "__main__":
    try:
        logging.info("=" * 60)
        logging.info("🔗 Starting Producer Node (Node 1)")
        logging.info("=" * 60)

        threads = [
            threading.Thread(target=topic_watcher, name="TopicWatcher", daemon=True),
            threading.Thread(target=publisher_worker, name="Publisher", daemon=True),
            threading.Thread(target=data_streamer, name="DataStreamer", daemon=True),
            threading.Thread(target=input_listener, name="InputListener", daemon=False),
        ]

        for t in threads:
            t.start()

        logging.info("✅ All producer threads running. Press Ctrl+C to exit.\n")
        threads[-1].join()  # Wait for Input Listener
    except KeyboardInterrupt:
        logging.info("🛑 Shutting down gracefully...")
    finally:
        stop_event.set()
        try:
            producer.close(timeout=5)
            logging.info("✅ Kafka producer closed")
        except Exception as e:
            logging.error("❌ Error closing producer: %s", e)
        logging.info("✅ Producer terminated cleanly")

