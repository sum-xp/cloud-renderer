import logging
import re
from typing import Any, Optional

from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


@app.get("/health")
def health():
    return jsonify({"ok": True}), 200


# ---------- MP4 finder ----------
def _find_mp4_in_obj(obj: Any) -> Optional[str]:
    """Recursively search any object for an http(s) URL ending in .mp4 (allowing query strings)."""
    if isinstance(obj, str):
        m = re.search(r"https?://\S+?\.mp4(?:\?\S+)?", obj, re.IGNORECASE)
        return m.group(0) if m else None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() in {"image_url", "thumbnail", "thumb"}:
                continue
            hit = _find_mp4_in_obj(v)
            if hit:
                return hit
    if isinstance(obj, (list, tuple)):
        for v in obj:
            hit = _find_mp4_in_obj(v)
            if hit:
                return hit
    return None


def extract_mp4_url(payload: dict) -> Optional[str]:
    """
    Try hard to find an MP4 URL in Breeze webhook payloads.
    - Checks common top-level keys
    - Looks into 'data'/'files'/'media'/'assets'
    - Falls back to scanning the whole payload
    """
    # 1) easy top-level keys
    for key in ("media_url", "mp4_url", "video_url", "url"):
        url = payload.get(key)
        if isinstance(url, str) and url.lower().endswith(".mp4"):
            return url

    # 2) typical containers Breeze uses
    for key in ("data", "files", "media", "assets"):
        if key in payload:
            hit = _find_mp4_in_obj(payload[key])
            if hit:
                return hit

    # 3) last resort: scan entire payload
    return _find_mp4_in_obj(payload)


# ---------- Webhook ----------
@app.post("/webhook")
def webhook():
    """Receives webhook POSTs from Breeze Cloud and logs payload info."""
    raw = request.get_data(as_text=True) or ""
    app.logger.info("Webhook received raw (first 1500 chars): %s", raw[:1500])

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        app.logger.warning("❌ No/invalid JSON payload")
        return jsonify({"status": "bad_request", "reason": "invalid_json"}), 400

    mp4_url = extract_mp4_url(payload)
    if not mp4_url:
        app.logger.warning("⚠️ No MP4 found in payload; keys=%s", list(payload.keys()))
        # Return 200 so Breeze doesn't retry forever
        return jsonify({"status": "ignored", "reason": "no_mp4"}), 200

    app.logger.info("🎬 MP4 detected: %s", mp4_url)

    # TODO: your processing here (download, render, upload back to Breeze, etc.)
    # For now, just acknowledge.
    return jsonify({"status": "ok", "mp4": mp4_url}), 200


# Optional root for convenience
@app.get("/")
def index():
    return "Cloud renderer is live. Try POST /webhook", 200


if __name__ == "__main__":
    # Local dev only; Render will run gunicorn
    app.run(host="0.0.0.0", port=10000)
