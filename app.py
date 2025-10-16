import os
import re
import json
import time
import shutil
import tempfile
import logging
import subprocess
from typing import Optional, Tuple

import requests
from flask import Flask, request, jsonify
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ──────────────────────────────────────────────────────────────────────────────
# Config (env vars)
# ──────────────────────────────────────────────────────────────────────────────
PORT                = int(os.getenv("PORT", "10000"))

AWS_REGION          = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET           = os.getenv("S3_BUCKET", "")                     # e.g. sumxp-renders
OUTPUT_PREFIX       = os.getenv("OUTPUT_PREFIX", "renders/")         # e.g. renders/
OVERLAY_S3_KEY      = os.getenv("OVERLAY_S3_KEY", "").strip()        # e.g. hand_overlay.mov
MAKE_PUBLIC         = os.getenv("MAKE_PUBLIC", "false").lower() == "true"
PRESIGN_TTL         = int(os.getenv("PRESIGN_TTL", "43200"))         # seconds; 0=off

# Render settings (kept simple and robust for booth media)
TARGET_FPS          = int(os.getenv("TARGET_FPS", "20"))
FFMPEG_BIN          = os.getenv("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN         = os.getenv("FFPROBE_BIN", "ffprobe")

# Optional: post the processed URL back to Breeze (Manual Uploads)
POST_BACK_TO_BREEZE = os.getenv("POST_BACK_TO_BREEZE", "false").lower() == "true"
BREEZE_UPLOAD_URL   = os.getenv("BREEZE_UPLOAD_URL", "").strip()     # your Breeze API "upload by URL" endpoint
BREEZE_API_KEY      = os.getenv("BREEZE_API_KEY", "").strip()        # Bearer token / API key

# ──────────────────────────────────────────────────────────────────────────────
# App & AWS clients
# ──────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.logger.setLevel(logging.INFO)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

_session = boto3.session.Session(region_name=AWS_REGION)
_s3 = _session.client("s3")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _run(cmd: list[str]) -> Tuple[int, str]:
    """Run a shell command and capture combined output."""
    app.logger.info("FFmpeg cmd: %s", " ".join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = []
    for line in p.stdout:
        out.append(line)
    code = p.wait()
    return code, "".join(out)

def _download(url: str, dst_path: str):
    """HTTP download to file."""
    app.logger.info("⬇️  downloading %s", url)
    with requests.get(url, stream=True, timeout=30) as r:
        r.raise_for_status()
        with open(dst_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)

def s3_download(bucket: str, key: str, dst_path: str):
    app.logger.info("Downloading overlay from s3://%s/%s", bucket, key)
    _s3.download_file(bucket, key, dst_path)

def s3_upload(src_path: str, bucket: str, key: str) -> str:
    """Upload to S3. Return s3:// URL; also return https if public or presigned if configured."""
    extra = {}
    if MAKE_PUBLIC:
        extra["ACL"] = "public-read"
    app.logger.info("⬆️  uploading to s3://%s/%s (%0.2f MB)", bucket, key, os.path.getsize(src_path)/1e6)
    _s3.upload_file(src_path, bucket, key, ExtraArgs=extra)
    # Choose best URL to give back
    if MAKE_PUBLIC:
        return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{key}"
    if PRESIGN_TTL > 0:
        return _s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=PRESIGN_TTL
        )
    return f"s3://{bucket}/{key}"

def ffprobe_meta(path: str) -> dict:
    """Get basic meta (width/height/fps)."""
    try:
        cmd = [
            FFPBROBE_BIN := FFPROBE_BIN, "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,avg_frame_rate",
            "-of", "json", path
        ]
        code, out = _run(cmd)
        if code == 0:
            data = json.loads(out)
            st = data.get("streams", [{}])[0]
            fps = st.get("avg_frame_rate", "0/1")
            try:
                n, d = fps.split("/")
                fps_val = round(float(n) / float(d), 3) if float(d) != 0 else TARGET_FPS
            except Exception:
                fps_val = TARGET_FPS
            return {"width": st.get("width"), "height": st.get("height"), "fps": fps_val}
    except Exception:
        pass
    return {}

MP4_RE = re.compile(r'https?://[^"\']+\.mp4[^"\']*', re.I)

def resolve_mp4_from_page(url: str) -> Optional[str]:
    """Fetch a page and pull the first .mp4 URL."""
    app.logger.info("🔎 resolving MP4 from page: %s", url)
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    m = MP4_RE.search(r.text)
    if m:
        return m.group(0)
    return None

def extract_uid(payload: dict) -> str:
    # Try explicit id, else session id, else timestamp
    return (
        payload.get("id")
        or payload.get("eventkitesessionid")
        or str(int(time.time()))
    )

def maybe_download_overlay(workdir: str) -> Optional[str]:
    """Return local path to overlay or None."""
    if not OVERLAY_S3_KEY:
        app.logger.info("No OVERLAY_S3_KEY set; skipping overlay.")
        return None
    local = os.path.join(workdir, os.path.basename(OVERLAY_S3_KEY))
    try:
        s3_download(S3_BUCKET, OVERLAY_S3_KEY, local)
        return local
    except ClientError as e:
        app.logger.warning("Overlay download failed: %s", e)
        return None

def compose_with_ffmpeg(src_mp4: str, out_mp4: str, overlay: Optional[str]) -> None:
    """
    Composite overlay (if present) and re-encode to event-safe H.264 + faststart + 20fps.
    We keep sizing as-is (no forced scaling) to avoid soft blur; both streams should be 960x1440.
    """
    if overlay:
        # Use alpha if present; overlay fully covers; shortest=1 to account for any duration mismatch
        filtergraph = "[0:v]format=rgba[base];[1:v]format=rgba[ol];[base][ol]overlay=0:0:format=auto:shortest=1[vout]"
        cmd = [
            FFMPEG_BIN, "-y", "-loglevel", "error",
            "-i", src_mp4, "-i", overlay,
            "-filter_complex", filtergraph,
            "-map", "[vout]", "-map", "0:a?",               # keep audio if exists
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            "-r", str(TARGET_FPS),
            "-movflags", "+faststart",
            "-c:a", "aac", "-b:a", "128k",
            out_mp4
        ]
    else:
        cmd = [
            FFMPEG_BIN, "-y", "-loglevel", "error",
            "-i", src_mp4,
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            "-r", str(TARGET_FPS),
            "-movflags", "+faststart",
            "-c:a", "aac", "-b:a", "128k",
            out_mp4
        ]
    code, out = _run(cmd)
    if code != 0 or not os.path.exists(out_mp4):
        raise RuntimeError(f"ffmpeg failed (code {code})\n{out}")

def post_back_to_breeze(processed_url: str, payload: dict) -> dict:
    """
    Push the processed URL to Breeze as a 'Manual Upload' so the Microsite shows it.
    Adjust the payload keys to match your Breeze endpoint if needed.
    """
    if not (POST_BACK_TO_BREEZE and BREEZE_UPLOAD_URL and BREEZE_API_KEY):
        return {"status": "skipped"}

    headers = {
        "Authorization": f"Bearer {BREEZE_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {
        "gallery_id": payload.get("eventkitegalleryid"),
        "session_id": payload.get("eventkitesessionid"),
        "url": processed_url,
        "media_type": "video/mp4",
        # "replace_existing": True,  # enable if your endpoint supports replacing the visible video
    }
    try:
        r = requests.post(BREEZE_UPLOAD_URL, headers=headers, json=body, timeout=20)
        ok = 200 <= r.status_code < 300
        app.logger.info("Breeze post-back: %s %s", r.status_code, r.text[:400])
        return {"status": "ok" if ok else "error", "code": r.status_code, "body": r.text}
    except Exception as e:
        app.logger.exception("Breeze post-back failed")
        return {"status": "error", "error": str(e)}

# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return "ok"

@app.get("/")
def root():
    return "renderer ready"

@app.get("/warmup")
def warmup():
    # Quick, non-failing probes
    try:
        _s3.list_buckets()
    except Exception:
        pass
    return jsonify({"status": "ok"})

@app.post("/webhook")
def webhook():
    started = time.time()
    raw = request.get_data(as_text=True)[:1500]
    app.logger.info("Webhook received raw (first 1500 chars): %s", raw)
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception:
        payload = {}

    uid = extract_uid(payload)

    # Determine source MP4:
    mp4_url = None

    # If Breeze sends us a direct MP4 URL (custom webhook), accept it
    if isinstance(payload.get("mp4_url"), str) and payload["mp4_url"].lower().endswith(".mp4"):
        mp4_url = payload["mp4_url"]
        app.logger.info("🎬 MP4 detected: %s", mp4_url)

    # If Breeze sends "media_url" and it's a microsite/page, resolve to MP4
    if not mp4_url and isinstance(payload.get("media_url"), str):
        page = payload["media_url"]
        mp4_url = resolve_mp4_from_page(page)

    # If Breeze sends 'image_url' (often a microsite path), try that too
    if not mp4_url and isinstance(payload.get("image_url"), str):
        mp4_url = resolve_mp4_from_page(payload["image_url"])

    if not mp4_url:
        app.logger.warning("⚠️ No MP4 found; keys=%s", list(payload.keys()))
        return jsonify({"status": "ignored", "reason": "no_mp4"}), 200

    # Work in temp dir
    with tempfile.TemporaryDirectory() as tmp:
        src_path = os.path.join(tmp, "source.mp4")
        out_path = os.path.join(tmp, f"{uid}_final.mp4")

        # Download source
        _download(mp4_url, src_path)

        # Optional overlay
        overlay_local = maybe_download_overlay(tmp)

        # Compose
        compose_with_ffmpeg(src_path, out_path, overlay_local)

        # Upload to S3
        key = f"{OUTPUT_PREFIX.rstrip('/')}/{uid}_final.mp4"
        processed_url = s3_upload(out_path, S3_BUCKET, key)

        # Probe some meta (non-fatal)
        meta = ffprobe_meta(out_path)

    # Optional: send back to Breeze (Manual Uploads)
    breeze = post_back_to_breeze(processed_url, payload) if POST_BACK_TO_BREEZE else {"status": "skipped"}

    took = round(time.time() - started, 2)
    app.logger.info("✅ done uid=%s time=%0.2fs meta=%s", uid, took, meta)
    return jsonify({
        "status": "ok",
        "uid": uid,
        "processed_url": processed_url,
        "width": meta.get("width"),
        "height": meta.get("height"),
        "fps": meta.get("fps") or TARGET_FPS,
        "breeze": breeze
    })

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
