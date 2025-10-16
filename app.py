# app.py (replace your /webhook route with this)
from flask import Flask, request, jsonify
import json, re

app = Flask(__name__)

MP4_RE = re.compile(r"https?://[^\s\"']+\.mp4(?:\?[^\s\"']*)?", re.IGNORECASE)

def find_mp4_in_obj(obj):
    """Recursively search dict/list/str for the first MP4 URL."""
    if obj is None:
        return None
    if isinstance(obj, str):
        m = MP4_RE.search(obj)
        return m.group(0) if m else None
    if isinstance(obj, dict):
        # try common keys first
        for k in ("media_url", "url", "file", "mp4", "video", "source", "original"):
            if k in obj:
                u = find_mp4_in_obj(obj[k])
                if u: return u
        # then search everything
        for v in obj.values():
            u = find_mp4_in_obj(v)
            if u: return u
        return None
    if isinstance(obj, list):
        for v in obj:
            u = find_mp4_in_obj(v)
            if u: return u
    return None

@app.route("/webhook", methods=["POST"])
def webhook():
    # Raw body for debugging
    raw = request.get_data() or b""
    app.logger.info("Webhook headers: %s", dict(request.headers))
    app.logger.info("Webhook raw (truncated): %s", raw[:2048].decode("utf-8", "ignore"))

    payload = request.get_json(silent=True)
    if payload is None:
        # try form-encoded
        if request.form:
            payload = request.form.to_dict(flat=False)
        else:
            # as a last resort, try to parse raw as JSON
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                payload = {}

    mp4_url = find_mp4_in_obj(payload)
    req_id = None

    # try to pick an id if present (optional)
    for key in ("id", "media_id", "session_id", "uid", "filename"):
        if isinstance(payload, dict) and key in payload:
            req_id = str(payload[key]); break

    if not mp4_url:
        app.logger.error("No MP4 URL found in payload; returning 200 to avoid retries. id=%s payload_keys=%s",
                         req_id, list(payload.keys()) if isinstance(payload, dict) else type(payload))
        return jsonify({"status": "ignored", "reason": "no_mp4_in_payload"}), 200

    app.logger.info("MP4 detected: %s (id=%s)", mp4_url, req_id)

    # >>> your existing pipeline here <<<
    # e.g., download(mp4_url) -> overlay -> upload -> return final URL
    # Make sure to catch exceptions and log.

    return jsonify({"status": "queued", "media_url": mp4_url, "id": req_id}), 200


@app.route("/webhook/test", methods=["POST"])
def webhook_test():
    # Echo back what we received to help you inspect from Breeze's "Test" button (if present)
    try:
        body = request.get_json(force=False, silent=True)
    except Exception:
        body = None
    return jsonify({
        "headers": dict(request.headers),
        "json": body,
        "form": request.form.to_dict(flat=False),
        "raw_preview": (request.get_data() or b"")[:2048].decode("utf-8", "ignore")
    }), 200
