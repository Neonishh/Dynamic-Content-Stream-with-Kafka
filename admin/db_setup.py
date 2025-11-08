# db_setup.py
from flask import Flask
from models import db, Topic, UserSubscription
import config
import os
from sqlalchemy import inspect

def init_db(reset=False, insert_samples=True):
    """
    Initialize or reset the database for the Admin Node (Node 4)
    - Creates tables for topics and user_subscriptions
    - Optionally inserts sample topics
    """
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{config.DB_PATH}"
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)

    with app.app_context():
        if reset and os.path.exists(config.DB_PATH):
            print("🗑️  Removing existing database...")
            os.remove(config.DB_PATH)

        db.create_all()
        print(f"✅ Database initialized and tables created at {config.DB_PATH}")

        # ✅ List tables to verify creation
        inspector = inspect(db.engine)
        tables = inspector.get_table_names()
        print("📋 Tables created:", tables)

        # ✅ Insert sample topics if requested
        if insert_samples:
            existing_count = Topic.query.count()
            if existing_count == 0:
                sample_topics = [
                    Topic(topic_name="tech-news", status="pending"),
                    Topic(topic_name="python-posts", status="pending"),
                    Topic(topic_name="gaming-news", status="pending")
                ]
                db.session.add_all(sample_topics)
                db.session.commit()
                print("🌱 Inserted sample topics:")
                for t in sample_topics:
                    print(f"   - {t.topic_name} ({t.status})")
            else:
                print("⚠️  Sample topics skipped — topics already exist.")

if __name__ == "__main__":
    # Run with reset=True to rebuild a clean DB
    init_db(reset=True)
