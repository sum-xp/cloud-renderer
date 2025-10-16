from flask import Flask, request, jsonify
import json
import re

app = Flask(__name__)

# --- Health & root endpoints ---
@app.get("/health")
def health():
    """Simple health check for Render + monitoring."""
    return jsonify(status="ok"), 200


@app.route("/", methods=["GET", "POST"])
def root():
    """Handles basic GET/POST to prevent 405 errors."""
    return jsonify(message="App is live. POST to /webhook for payloads."), 200


# --- Webhook endpoint ---
@app.post("/webhook")
def webhook():
    """Receives webhook POSTs from Breeze Cloud and logs payload info."""
    raw_data = request.get_data(as_text=True)
    app.logger.info("Webhook received raw: %s", raw_data[:1500])

    # Try to parse JSON
    payload = request.get_json(silent=True)
    if not payload:
        try:
            payload = json.loads(raw_data)
        except Exception:
            payload = {}

    # Look for an MP4 URL anywhere in the payload
    mp4_pattern = re.compile(r"https?://[^\s\"']+\.mp4(?:\?[^\s\"']*)?", re.IGNORECASE)
    match = mp4_pattern.search(raw_data)
    mp4_url = match.group(0) if match else None

    # Try to extract a session/id if present
    uid = None
    for key in ("id", "sessionid", "media_id", "filename"):
        if key in payload:
            uid = payload[key]
            break

    if mp4_url:
        app.logger.info(f"✅ MP4 detected for id={uid}: {mp4_url}")
        # --- Future step: call FFmpeg pipeline here ---
        return jsonify(status="ok", id=uid, media_url=mp4_url), 200
    else:
        app.logger.warning(f"⚠️ No MP4 found in payload keys={list(payload.keys())}")
        return jsonify(status="ignored", reason="no_mp4_found", id=uid), 200


# --- Optional test endpoint for manual posts ---
@app.post("/webhook/test")
def webhook_test():
    """Echo endpoint for quick local/remote tests."""
    data = request.get_json(silent=True)
    return jsonify(received=data, headers=dict(request.headers)), 200


if __name__ == "__main__":
    # Local dev (Render uses gunicorn)
    app.run(host="0.0.0.0", port=10000)
