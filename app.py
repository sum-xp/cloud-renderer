import os
import re
import json
import time
import tempfile
import logging
import subprocess
from typing import Optional, Tuple

import requests
from flask import Flask, request, jsonify
import boto3
from botocore.exceptions import ClientError, BotoCoreError

# ──────────────────────────────────────────────────────────────────────────────
# Environment / Config
# ──────────────────────────────────────────────────────────────────────────────
PORT                = int(os.getenv("PORT", "10000"))

# AWS / S3
AWS_REGION          = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET           = os.getenv("S3_BUCKET", "")
OUTPUT_PREFIX       = os.getenv("OUTPUT_PREFIX", "renders/")
OVERLAY_S3_KEY      = os.getenv("OVERLAY_S3_KEY", "").strip()
MAKE_PUBLIC         = os.getenv("MAKE_PUBLIC", "false").lower() == "true"
PRESIGN_TTL         = int(os.getenv("PRESIGN_TTL", "43200"))  # 0 disables presign

# Render settings
TARGET_FPS          = int(os.getenv("TARGET_FPS", "20"))
FFMPEG_BIN          = os.getenv("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN         = os.getenv("FFPROBE_BIN", "ffprobe")

# HTTP
HEADERS             = {"User-Agent": "cloud-renderer/1.0 (+https://render.com)"}
REQUEST_TIMEOUT     = int(os.getenv("REQUEST_TIMEOUT", "25"))

# Optional: Post finished URL back to Breeze (Manual Uploads)
POST_BACK_TO_BREEZE = os.getenv("POST_BACK_TO_BREEZE", "false").lower() == "true"
BREEZE_UPLOAD_URL   = os.getenv("BREEZE_UPLOAD_URL", "").strip()  # e.g. https://<breeze>/api/uploads/by-url
BREEZE_API_KEY      = os.getenv("BREEZE_API_KEY", "").strip()

# Optional: Breeze API fallback to fetch MP4 by session id
# Templated URL; we will substitute {session_id} and {gallery_id} when present.
# Example (YOU provide the correct one from your docs):
#   BREEZE_ASSETS_URL_TEMPLATE="https://cloud.breezesoftware.com/api/eventkite/sessions/{session_id}/assets"
BREEZE_ASSETS_URL_TEMPLATE = os.getenv("BREEZE_ASSETS_URL_TEMPLATE", "").strip()
BREEZE_API_AUTH_HEADER     = os.getenv("BREEZE_API_AUTH_HEADER", "Authorization").strip()  # usually "Authorization"
BREEZE_API_AUTH_PREFIX     = os.getenv("BREEZE_API_AUTH_PREFIX", "Bearer ").strip()        # usually "Bearer "

# Resolver retries
RESOLVE_TRIES        = int(os.getenv("RESOLVE_TRIES", "6"))
RESOLVE_SLEEP_SEC    = float(os.getenv("RESOLVE_SLEEP_SEC", "2.0"))

# ──────────────────────────────────────────────────────────────────────────────
# App + AWS
# ──────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.logger.setLevel(logging.INFO)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

session = boto3.session.Session(region_name=AWS_REGION)
s3 = session.client("s3")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _run(cmd: list[str]) -> Tuple[int, str]:
    app.logger.info("FFmpeg cmd: %s", " ".join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = []
    for line in p.stdout:
        out.append(line)
    code = p.wait()
    return code, "".join(out)

def _fetch(url: str, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    return requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)

def _download(url: str, dst_path: str):
    app.logger.info("⬇️  downloading %s", url)
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=HEADERS) as r:
        r.raise_for_status()
        with open(dst_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)

def s3_download(bucket: str, key: str, dst_path: str):
    app.logger.info("Downloading overlay from s3://%s/%s", bucket, key)
    s3.download_file(bucket, key, dst_path)

def s3_upload(src_path: str, bucket: str, key: str) -> str:
    extra = {}
    if MAKE_PUBLIC:
        extra["ACL"] = "public-read"
    size_mb = os.path.getsize(src_path) / 1e6
    app.logger.info("⬆️  uploading to s3://%s/%s (%.2f MB)", bucket, key, size_mb)
    s3.upload_file(src_path, bucket, key, ExtraArgs=extra)

    # Choose URL to return
    if MAKE_PUBLIC:
        return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{key}"
    if PRESIGN_TTL > 0:
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=PRESIGN_TTL
        )
    return f"s3://{bucket}/{key}"

def ffprobe_meta(path: str) -> dict:
    try:
        cmd = [
            F F P R O B E := FFPROBE_BIN, "-v", "error",
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

# ──────────────────────────────────────────────────────────────────────────────
# MP4 resolution (microsite HTML + retries + /location fallback)
# ──────────────────────────────────────────────────────────────────────────────
MP4_RE = re.compile(r'https?://[^"\']+\.mp4[^"\']*', re.I)

def _scrape_mp4_from_html(html: str) -> Optional[str]:
    cands = []
    # 1) any .mp4 string
    cands += MP4_RE.findall(html)
    # 2) <source>/<video> tags
    cands += re.findall(r'src=["\'](https?://[^"\']+\.mp4[^"\']*)', html, re.I)
    # 3) OpenGraph
    cands += re.findall(r'property=["\']og:video["\']\s+content=["\'](https?://[^"\']+\.mp4[^"\']*)', html, re.I)
    # 4) JSON strings containing .mp4
    cands += re.findall(r'"(https?://[^"]+\.mp4[^"]*)"', html, re.I)
    return cands[0] if cands else None

def resolve_mp4_from_page(url: str, tries: int = RESOLVE_TRIES, sleep_sec: float = RESOLVE_SLEEP_SEC) -> Optional[str]:
    app.logger.info("🔎 resolving MP4 from page: %s", url)

    def try_once(u: str) -> Optional[str]:
        r = _fetch(u)
        r.raise_for_status()
        # if redirect landed on .mp4
        if r.url.lower().endswith(".mp4"):
            return r.url
        return _scrape_mp4_from_html(r.text)

    for attempt in range(1, tries + 1):
        try:
            hit = try_once(url)
            if hit:
                return hit

            # Fallback: try without /location
            if url.rstrip("/").endswith("/location"):
                alt = url.rstrip("/")[:-len("/location")]
                app.logger.info("🔁 retrying without /location: %s (attempt %d/%d)", alt, attempt, tries)
                hit = try_once(alt)
                if hit:
                    return hit

        except Exception as e:
            app.logger.warning("Resolver try %d/%d error: %s", attempt, tries, e)

        if attempt < tries:
            time.sleep(sleep_sec)
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Breeze API fallback by session id (optional)
# ──────────────────────────────────────────────────────────────────────────────
def breeze_api_find_mp4(session_id: Optional[str], gallery_id: Optional[str]) -> Optional[str]:
    """If configured, call Breeze API to list assets for a session, return an .mp4 URL."""
    if not (BREEZE_ASSETS_URL_TEMPLATE and BREEZE_API_KEY and session_id):
        return None

    url = BREEZE_ASSETS_URL_TEMPLATE.format(session_id=session_id, gallery_id=gallery_id or "")
    hdrs = {
        BREEZE_API_AUTH_HEADER: f"{BREEZE_API_AUTH_PREFIX}{BREEZE_API_KEY}",
        "Accept": "application/json"
    }
    try:
        app.logger.info("🛰️  Breeze API lookup: %s", url)
        r = requests.get(url, headers=hdrs, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        text = r.text or ""
        # Try to parse JSON; if structure unknown, still search for .mp4
        mp4 = None
        try:
            data = r.json()
            # VERY generic walkers: look for common fields or scan strings
            # 1) Lists of dicts with url/mime or similar
            def walk(obj):
                nonlocal mp4
                if mp4:
                    return
                if isinstance(obj, dict):
                    # direct fields
                    u = obj.get("url") or obj.get("href") or obj.get("download_url")
                    m = obj.get("mime") or obj.get("content_type") or obj.get("type")
                    if u and isinstance(u, str) and u.lower().endswith(".mp4"):
                        mp4 = u
                        return
                    if m and "mp4" in str(m).lower() and u:
                        mp4 = u
                        return
                    for v in obj.values():
                        walk(v)
                elif isinstance(obj, list):
                    for v in obj:
                        walk(v)
                elif isinstance(obj, str):
                    if obj.lower().endswith(".mp4"):
                        mp4 = obj
            walk(data)
        except Exception:
            pass

        if not mp4:
            # fallback: regex on raw JSON/text
            mp = MP4_RE.search(text)
            if mp:
                mp4 = mp.group(0)
        if mp4:
            app.logger.info("🛰️  Breeze API mp4: %s", mp4)
        return mp4
    except Exception as e:
        app.logger.warning("Breeze API fallback failed: %s", e)
        return None

# ──────────────────────────────────────────────────────────────────────────────
# FFmpeg pipeline
# ──────────────────────────────────────────────────────────────────────────────
def maybe_download_overlay(workdir: str) -> Optional[str]:
    if not OVERLAY_S3_KEY:
        app.logger.info("No OVERLAY_S3_KEY set; skipping overlay.")
        return None
    local = os.path.join(workdir, os.path.basename(OVERLAY_S3_KEY))
    try:
        s3_download(S3_BUCKET, OVERLAY_S3_KEY, local)
        return local
    except (ClientError, BotoCoreError) as e:
        app.logger.warning("Overlay download failed: %s", e)
        return None

def compose_with_ffmpeg(src_mp4: str, out_mp4: str, overlay: Optional[str]):
    if overlay:
        # Alpha-aware overlay, keep base size, stop at shortest stream
        filtergraph = "[0:v]format=rgba[base];[1:v]format=rgba[ol];[base][ol]overlay=0:0:format=auto:shortest=1[vout]"
        cmd = [
            FFMPEG_BIN, "-y", "-loglevel", "error",
            "-i", src_mp4, "-i", overlay,
            "-filter_complex", filtergraph,
            "-map", "[vout]", "-map", "0:a?",
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
    """Attach processed video as Manual Upload (Microsite shows it)."""
    if not (POST_BACK_TO_BREEZE and BREEZE_UPLOAD_URL and BREEZE_API_KEY):
        return {"status": "skipped"}
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        BREEZE_API_AUTH_HEADER: f"{BREEZE_API_AUTH_PREFIX}{BREEZE_API_KEY}",
    }
    body = {
        "gallery_id": payload.get("eventkitegalleryid"),
        "session_id": payload.get("eventkitesessionid"),
        "url": processed_url,
        "media_type": "video/mp4"
        # add flags from your API spec if supported:
        # "replace_existing": True,
        # "title": "Processed",
    }
    try:
        r = requests.post(BREEZE_UPLOAD_URL, headers=headers, json=body, timeout=REQUEST_TIMEOUT)
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
    try:
        s3.list_buckets()
    except Exception:
        pass
    return jsonify({"status": "ok"})

@app.post("/webhook")
def webhook():
    started = time.time()
    raw = request.get_data(as_text=True)[:1500]
    app.logger.info("Webhook received raw (first 1500 chars): %s", raw)

    try:
        payload = request.get_json(force=True)
    except Exception:
        payload = {}

    # identify
    uid = (
        payload.get("id")
        or payload.get("eventkitesessionid")
        or str(int(time.time()))
    )
    session_id = payload.get("eventkitesessionid")
    gallery_id = payload.get("eventkitegalleryid")

    # 1) direct mp4 url provided?
    mp4_url = None
    if isinstance(payload.get("mp4_url"), str) and ".mp4" in payload["mp4_url"]:
        mp4_url = payload["mp4_url"]
        app.logger.info("🎬 MP4 detected (direct): %s", mp4_url)

    # 2) try page URLs (media_url or image_url)
    if not mp4_url and isinstance(payload.get("media_url"), str):
        mp4_url = resolve_mp4_from_page(payload["media_url"])
    if not mp4_url and isinstance(payload.get("image_url"), str):
        mp4_url = resolve_mp4_from_page(payload["image_url"])

    # 3) Breeze API fallback by session id (optional)
    if not mp4_url:
        mp4_url = breeze_api_find_mp4(session_id, gallery_id)

    if not mp4_url:
        app.logger.warning("⚠️ No MP4 found after retries + API fallback; keys=%s", list(payload.keys()))
        return jsonify({"status": "ignored", "reason": "no_mp4"}), 200

    # Work in /tmp
    with tempfile.TemporaryDirectory() as tmp:
        src = os.path.join(tmp, "source.mp4")
        out = os.path.join(tmp, f"{uid}_final.mp4")

        _download(mp4_url, src)
        overlay = maybe_download_overlay(tmp)
        compose_with_ffmpeg(src, out, overlay)

        key = f"{OUTPUT_PREFIX.rstrip('/')}/{uid}_final.mp4"
        processed_url = s3_upload(out, S3_BUCKET, key)
        meta = ffprobe_meta(out)

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
