import os
import re
import json
import time
import shutil
import logging
import tempfile
import subprocess
from typing import Any, Optional
from urllib.parse import urlparse

import boto3
import requests
from flask import Flask, jsonify, request

# ------------------------
# Config / Globals
# ------------------------
AWS_REGION   = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET    = os.getenv("S3_BUCKET", "sumxp-renders")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "renders/").rstrip("/") + "/"

# Optional overlay sources (pick one)
OVERLAY_URL     = os.getenv("OVERLAY_URL")        # e.g. https://.../hand_overlay.mov
OVERLAY_S3_KEY  = os.getenv("OVERLAY_S3_KEY")     # e.g. hand_overlay.mov (in same bucket)

FFMPEG_LOGLEVEL = os.getenv("FFMPEG_LOGLEVEL", "error")  # quiet|panic|fatal|error|warning|info|debug

session = boto3.session.Session(region_name=AWS_REGION)
s3 = session.client("s3")

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = app.logger

# ------------------------
# Helpers
# ------------------------

def looks_like_mp4(u: str) -> bool:
    if not isinstance(u, str) or not u:
        return False
    base = u.split("?", 1)[0].lower()
    return base.endswith(".mp4")

def _find_mp4_in_obj(obj: Any) -> Optional[str]:
    """Recursively search any object for an http(s) URL ending in .mp4 (allowing query strings)."""
    if isinstance(obj, str):
        m = re.search(r"https?://\S+?\.mp4(?:\?\S+)?", obj, re.IGNORECASE)
        return m.group(0) if m else None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() in {"image_url", "thumbnail", "thumb"}:
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
    """Try hard to find an MP4 URL in Breeze webhook payloads."""
    # common top-level keys
    for key in ("media_url", "mp4_url", "video_url", "url"):
        url = payload.get(key)
        if isinstance(url, str) and looks_like_mp4(url):
            return url
    # typical containers
    for key in ("data", "files", "media", "assets"):
        if key in payload:
            hit = _find_mp4_in_obj(payload[key])
            if hit:
                return hit
    # last resort: scan whole payload
    return _find_mp4_in_obj(payload)

def retry(n=3, backoff=0.6):
    def deco(fn):
        def run(*args, **kwargs):
            last = None
            for i in range(1, n+1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last = e
                    if i < n:
                        time.sleep(backoff * i)
            raise last
        return run
    return deco

@retry(n=3, backoff=0.7)
def http_download(url: str, dst_path: str):
    headers = {"User-Agent": "cloud-renderer/1.0"}
    with requests.get(url, headers=headers, stream=True, timeout=30) as r:
        r.raise_for_status()
        with open(dst_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

def s3_download(bucket: str, key: str, dst_path: str):
    s3.download_file(bucket, key, dst_path)

def presigned_get(bucket: str, key: str, seconds: int = 3600) -> str:
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=seconds,
    )

@retry(n=3, backoff=0.8)
def s3_upload_file(src_path: str, bucket: str, key: str):
    s3.upload_file(src_path, bucket, key)

def maybe_download_overlay(tmpdir: str) -> Optional[str]:
    """Return local path to overlay if provided, else None."""
    if OVERLAY_URL:
        local = os.path.join(tmpdir, "overlay.mov")
        log.info("Downloading overlay from URL: %s", OVERLAY_URL)
        http_download(OVERLAY_URL, local)
        return local
    if OVERLAY_S3_KEY:
        local = os.path.join(tmpdir, os.path.basename(OVERLAY_S3_KEY))
        log.info("Downloading overlay from s3://%s/%s", S3_BUCKET, OVERLAY_S3_KEY)
        s3_download(S3_BUCKET, OVERLAY_S3_KEY, local)
        return local
    return None

def ffprobe_fps(path: str) -> Optional[float]:
    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=r_frame_rate",
            "-of", "default=noprint_wrappers=1:nokey=1", path
        ]
        out = subprocess.check_output(cmd, text=True).strip()
        if "/" in out:
            num, den = out.split("/")
            return float(num) / float(den)
        return float(out)
    except Exception:
        return None

def run_ffmpeg(input_mp4: str, output_mp4: str, overlay_path: Optional[str] = None):
    """
    Encode a safe, shareable MP4 (yuv420p). If overlay is provided, it’s composited on top.
    Assumes overlay has premultiplied or straight alpha — ffmpeg will respect it if present.
    """
    # base filter: nothing (pass-through); ensure yuv420p for compatibility
    vf = []
    inputs = ["-i", input_mp4]

    if overlay_path:
        inputs += ["-i", overlay_path]
        # compose overlay on top; if your overlay needs positioning, adjust 'overlay=x:y'
        vf.append("overlay=0:0")

    filter_arg = []
    if vf:
        filter_arg = ["-filter_complex", ",".join(vf)]

    fps = ffprobe_fps(input_mp4) or 20
    cmd = [
        "ffmpeg", "-y",
        "-loglevel", FFMPEG_LOGLEVEL,
        *inputs,
        *filter_arg,
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-r", str(int(round(fps))),
        "-movflags", "+faststart",
        "-c:a", "aac", "-b:a", "128k",
        output_mp4
    ]
    log.info("FFmpeg cmd: %s", " ".join(cmd))
    subprocess.check_call(cmd)

# ------------------------
# Routes
# ------------------------

@app.get("/health")
def health():
    return jsonify({"ok": True}), 200

@app.get("/")
def root():
    return "Cloud renderer is live. POST /webhook", 200

@app.post("/webhook")
def webhook():
    raw = request.get_data(as_text=True) or ""
    log.info("Webhook received raw (first 1500 chars): %s", raw[:1500])

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        log.warning("❌ invalid json")
        return jsonify({"status": "bad_request", "reason": "invalid_json"}), 400

    uid = str(payload.get("id") or payload.get("uid") or int(time.time()))
    mp4_url = extract_mp4_url(payload)
    if not mp4_url:
        log.warning("⚠️ No MP4 found; keys=%s", list(payload.keys()))
        return jsonify({"status": "ignored", "reason": "no_mp4"}), 200

    if not looks_like_mp4(mp4_url):
        log.warning("⚠️ URL is not a direct .mp4: %s", mp4_url)
        return jsonify({"status": "ignored", "reason": "not_direct_mp4"}), 200

    with tempfile.TemporaryDirectory() as tmp:
        src_path  = os.path.join(tmp, "source.mp4")
        out_path  = os.path.join(tmp, f"{uid}_final.mp4")

        # download source mp4
        log.info("⬇️  downloading MP4: %s", mp4_url)
        http_download(mp4_url, src_path)

        # maybe get overlay
        overlay_local = maybe_download_overlay(tmp)

        # render
        t0 = time.time()
        run_ffmpeg(src_path, out_path, overlay_local)
        elapsed = time.time() - t0
        size_mb = os.path.getsize(out_path) / (1024 * 1024)

        # upload to S3
        key = f"{OUTPUT_PREFIX}{uid}_final.mp4"
        log.info("⬆️  uploading to s3://%s/%s (%.2f MB)", S3_BUCKET, key, size_mb)
        s3_upload_file(out_path, S3_BUCKET, key)

        # presign
        signed_url = presigned_get(S3_BUCKET, key, seconds=3600)

    # minimal media probe: width/height/fps (optional)
    meta = {}
    try:
        probe = subprocess.check_output(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height,r_frame_rate",
             "-of", "json", out_path],
            text=True
        )
        j = json.loads(probe)
        st = j["streams"][0]
        fr = st.get("r_frame_rate", "20/1")
        if "/" in fr:
            num, den = fr.split("/")
            fps = round(float(num) / float(den))
        else:
            fps = round(float(fr))
        meta = {"width": st.get("width"), "height": st.get("height"), "fps": fps}
    except Exception:
        pass

    log.info("✅ done uid=%s time=%.2fs meta=%s", uid, elapsed, meta or {})

    return jsonify({
        "status": "ok",
        "uid": uid,
        "processed_url": signed_url,         # presigned (no more 403)
        "s3_uri": f"s3://{S3_BUCKET}/{key}",
        **meta
    }), 200


if __name__ == "__main__":
    # local dev only; Render uses gunicorn
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
