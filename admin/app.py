from flask import Flask, render_template, request, jsonify, redirect
from models import db, Topic, UserSubscription
import config, os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{config.DB_PATH}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

# ==========================
#   API ENDPOINTS (For Producer & Consumer)
# ==========================

@app.route("/api/approved_topics", methods=["GET"])
def api_approved_topics():
    topics = Topic.query.filter_by(status="approved").all()
    return jsonify([t.topic_name for t in topics])

@app.route("/api/mark_active/<topic>", methods=["POST"])
def api_mark_active(topic):
    t = Topic.query.filter_by(topic_name=topic).first()
    if not t:
        return jsonify({"error": "Topic not found"}), 404
    t.status = "active"
    db.session.commit()
    return jsonify({"message": f"{topic} marked active"})

@app.route("/api/active_topics", methods=["GET"])
def api_active_topics():
    topics = Topic.query.filter_by(status="active").all()
    return jsonify([t.topic_name for t in topics])

@app.route("/api/subscriptions/<user>", methods=["GET"])
def api_user_subs(user):
    subs = UserSubscription.query.filter_by(user_name=user).all()
    return jsonify([s.topic_name for s in subs])

@app.route("/api/subscriptions", methods=["GET"])
def api_all_subscriptions():
    subs = UserSubscription.query.order_by(UserSubscription.subscribed_at.desc()).all()
    return jsonify([
        {
            "id": s.id,
            "user_name": s.user_name,
            "topic_name": s.topic_name,
            "subscribed_at": s.subscribed_at.strftime("%Y-%m-%d %H:%M:%S")
        } for s in subs
    ])

# ✅ New JSON-based unsubscribe endpoint
@app.route("/api/unsubscribe", methods=["POST"])
def api_unsubscribe():
    """Unsubscribe a user from a topic (called by Consumer)."""
    if not request.is_json:
        return jsonify({"error": "Expected JSON"}), 400
    data = request.get_json()
    user = data.get("user_name", "").strip()
    topic = data.get("topic_name", "").strip()
    if not user or not topic:
        return jsonify({"error": "Missing fields"}), 400

    sub = UserSubscription.query.filter_by(user_name=user, topic_name=topic).first()
    if not sub:
        return jsonify({"message": "Subscription not found"}), 404

    db.session.delete(sub)
    db.session.commit()
    print(f"❎ Unsubscribed {user} from {topic}")
    return jsonify({"message": f"{user} unsubscribed from {topic}"}), 200

# ==========================
#   ADMIN PANEL (UI)
# ==========================

@app.route("/")
def index():
    topics = Topic.query.order_by(Topic.created_at.desc()).all()
    return render_template("index.html", topics=topics)

@app.route("/subscriptions")
def subscriptions():
    subs = UserSubscription.query.order_by(UserSubscription.subscribed_at.desc()).all()
    topics = Topic.query.order_by(Topic.topic_name).all()
    return render_template("subscriptions.html", subs=subs, topics=topics)

# ---------- Topic Management ----------
@app.route("/add_topic", methods=["POST"])
def add_topic():
    name = request.form.get("topic_name", "").strip()
    if name and not Topic.query.filter_by(topic_name=name).first():
        db.session.add(Topic(topic_name=name, status="pending"))
        db.session.commit()
    return redirect("/")

@app.route("/approve/<int:tid>", methods=["POST"])
def approve(tid):
    t = Topic.query.get_or_404(tid)
    t.status = "approved"
    db.session.commit()
    return redirect("/")

@app.route("/reject/<int:tid>", methods=["POST"])
def reject(tid):
    t = Topic.query.get_or_404(tid)
    db.session.delete(t)
    db.session.commit()
    return redirect("/")

@app.route("/deactivate/<int:tid>", methods=["POST"])
def deactivate(tid):
    t = Topic.query.get_or_404(tid)
    t.status = "inactive"
    db.session.commit()
    return redirect("/")

@app.route("/reactivate/<int:tid>", methods=["POST"])
def reactivate(tid):
    t = Topic.query.get_or_404(tid)
    if t.status == "inactive":
        t.status = "approved"
        db.session.commit()
    return redirect("/")

@app.route("/delete/<int:tid>", methods=["POST"])
def delete_topic(tid):
    t = Topic.query.get_or_404(tid)
    db.session.delete(t)
    db.session.commit()
    return redirect("/")

# ---------- Subscription Management ----------
@app.route("/api/subscribe", methods=["POST"])
@app.route("/subscribe", methods=["POST"])
def subscribe():
    if request.is_json:
        data = request.get_json()
        user = data.get("user_name", "").strip()
        topic = data.get("topic_name", "").strip()
    else:
        user = request.form.get("user_name", "").strip()
        topic = request.form.get("topic_name", "").strip()

    if user and topic:
        existing = UserSubscription.query.filter_by(user_name=user, topic_name=topic).first()
        if not existing:
            db.session.add(UserSubscription(user_name=user, topic_name=topic))
            db.session.commit()
            print(f"✅ New subscription: {user} -> {topic}")
            if request.is_json:
                return jsonify({"message": "Subscription added"}), 201
    if request.is_json:
        return jsonify({"message": "Subscription exists or invalid"}), 200
    return redirect("/subscriptions")

@app.route("/unsubscribe/<int:sub_id>", methods=["POST"])
def unsubscribe(sub_id):
    s = UserSubscription.query.get_or_404(sub_id)
    db.session.delete(s)
    db.session.commit()
    return redirect("/subscriptions")

# ---------- Debug & Startup ----------
@app.route("/debug/reset", methods=["POST"])
def debug_reset():
    db.drop_all()
    db.create_all()
    return jsonify({"status": "reset"})

if __name__ == "__main__":
    if not os.path.exists(config.DB_PATH):
        with app.app_context():
            db.create_all()
            print("✅ Database created at", config.DB_PATH)
    app.run(host=config.HOST, port=config.PORT, debug=config.DEBUG)
