# producer.py
import threading, time, sqlite3, queue, json, argparse, logging
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

DB = "control.db"
#ADMIN_API = "http://127.0.0.1:5000/api/approved_topics"
KAFKA_BOOTSTRAP = "10.147.17.172:9092" 

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

message_queue = queue.Queue(maxsize=1000)

# Kafka clients
def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )

def make_admin():
    return KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=20000)

producer = None
admin_client = None

def ensure_kafka():
    global producer, admin_client
    try:
        producer = make_producer()
        admin_client = make_admin()
    except NoBrokersAvailable as e:
        logging.error("Kafka broker not available. Make sure Kafka is running.")
        raise

# ---------- Topic Watcher ----------
def topic_watcher(poll_interval=5):
    logging.info("Topic watcher started (poll interval %s s)", poll_interval)
    while True:
        try:
            conn = sqlite3.connect(DB)
            c = conn.cursor()
            c.execute("SELECT topic_name FROM topics WHERE status='approved'")
            approved = [r[0] for r in c.fetchall()]

            existing = []
            try:
                existing = admin_client.list_topics()
            except Exception as e:
                logging.warning("Could not list topics: %s", e)

            for topic in approved:
                if topic in existing:
                    # already exists -> mark active
                    c.execute("UPDATE topics SET status='active' WHERE topic_name=? AND status!='active'", (topic,))
                    conn.commit()
                    continue
                try:
                    logging.info("Creating topic: %s", topic)
                    new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
                    admin_client.create_topics([new_topic])
                    c.execute("UPDATE topics SET status='active' WHERE topic_name=?", (topic,))
                    conn.commit()
                    logging.info("Topic created and marked active: %s", topic)
                except TopicAlreadyExistsError:
                    c.execute("UPDATE topics SET status='active' WHERE topic_name=?", (topic,))
                    conn.commit()
                except Exception as e:
                    logging.error("Error creating topic %s: %s", topic, e)
            conn.close()
        except Exception as e:
            logging.exception("Topic watcher exception: %s", e)
        time.sleep(poll_interval)

# ---------- Input Listener (CSV) ----------
def input_listener_csv(csv_path, interval=2):
    logging.info("CSV input listener started: %s", csv_path)
    import csv
    while True:
        with open(csv_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # row has 'topic' and 'message'
                topic = row.get('topic')
                raw_msg = row.get('message')
                try:
                    # If message is JSON-like string ensure parsed:
                    msg = json.loads(raw_msg) if raw_msg and raw_msg.strip().startswith("{") else raw_msg
                except Exception:
                    msg = raw_msg
                payload = {"message": msg, "timestamp": time.time()}
                try:
                    message_queue.put({"topic": topic, "payload": payload}, timeout=5)
                    logging.info("Enqueued -> %s", topic)
                except queue.Full:
                    logging.warning("Queue full, skipping message")
                time.sleep(interval)
        # after one pass, wait before reading file again
        time.sleep(1)

# ---------- Input Listener (Flask API) ----------
def start_input_api(host='0.0.0.0', port=5001):
    from flask import Flask, request, jsonify
    app = Flask("input_api")

    @app.route("/produce", methods=["POST"])
    def produce():
        j = request.get_json(force=True)
        topic = j.get("topic")
        payload = j.get("payload") or j.get("message")
        if not topic or payload is None:
            return jsonify({"status": "error", "reason":"topic and payload required"}), 400
        try:
            message_queue.put({"topic": topic, "payload": payload}, timeout=2)
            return jsonify({"status":"enqueued"})
        except queue.Full:
            return jsonify({"status":"error", "reason":"queue full"}), 503

    logging.info("Starting input API on %s:%s", host, port)
    app.run(host=host, port=port, threaded=True)

# ---------- Publisher ----------
def publisher_worker():
    logging.info("Publisher started")
    while True:
        try:
            msg = message_queue.get()
            if not msg:
                time.sleep(0.1)
                continue
            topic = msg['topic']
            payload = msg['payload']
            try:
                producer.send(topic, value=payload)
                producer.flush()  # flush optional; remove for better throughput
                logging.info("Published to %s: %s", topic, payload.get("message") if isinstance(payload, dict) else payload)
            except Exception as e:
                logging.error("Failed to publish to %s: %s", topic, e)
        except Exception as e:
            logging.exception("Publisher error: %s", e)
        time.sleep(0.01)

# ---------- Main ----------
def main(args):
    ensure_kafka()
    threads = []

    # Topic watcher
    t_tw = threading.Thread(target=topic_watcher, daemon=True)
    t_tw.start()
    threads.append(t_tw)

    # Publisher
    t_pub = threading.Thread(target=publisher_worker, daemon=True)
    t_pub.start()
    threads.append(t_pub)

    # Input mode
    if args.mode == "csv":
        t_in = threading.Thread(target=input_listener_csv, args=(args.csv_path, args.interval), daemon=True)
        t_in.start()
        threads.append(t_in)
    elif args.mode == "api":
        # run flask API in separate thread
        t_in = threading.Thread(target=start_input_api, args=(args.host, args.port), daemon=True)
        t_in.start()
        threads.append(t_in)
    else:
        logging.error("Unknown mode. choose csv or api")
        return

    logging.info("All threads started. Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down producer...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["csv","api"], default="csv")
    parser.add_argument("--csv_path", default="data/sample_data.csv")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5001)
    args = parser.parse_args()
    main(args)

