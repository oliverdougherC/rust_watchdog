"""Flask web dashboard and health endpoint for the transcode watchdog."""

import logging
import threading
from typing import Optional

from flask import Flask, jsonify, render_template

from db import WatchdogDB
from shared_state import WatchdogState
from main import StatisticsTracker


def create_app(
    state: WatchdogState,
    db: WatchdogDB,
    stats: StatisticsTracker,
) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")

    # Suppress Flask request logging in production
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.WARNING)

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/api/status")
    def api_status():
        snap = state.snapshot()
        snap["files_transcoded"] = db.get_transcode_count()
        snap["total_space_saved"] = db.get_total_space_saved()
        snap["inspected_count"] = db.get_inspected_count()
        return jsonify(snap)

    @app.route("/api/history")
    def api_history():
        rows = db.get_recent_transcodes(limit=100)
        return jsonify(rows)

    @app.route("/api/console")
    def api_console():
        return jsonify(state.get_log_lines())

    @app.route("/health")
    def health():
        snap = state.snapshot()
        return jsonify({
            "alive": True,
            "state": snap["state"],
            "last_pass_time": snap["last_pass_time"],
            "queue_depth": snap["queue_total"] - snap["queue_position"],
            "nfs_healthy": snap["nfs_healthy"],
        })

    return app


def start_dashboard_thread(
    state: WatchdogState,
    db: WatchdogDB,
    stats: StatisticsTracker,
    port: int = 8095,
) -> threading.Thread:
    """Launch the Flask dashboard as a daemon thread."""
    app = create_app(state, db, stats)

    def _run():
        app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True, debug=False)

    t = threading.Thread(target=_run, name="dashboard", daemon=True)
    t.start()
    return t
