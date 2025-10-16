import os, uuid, tempfile, subprocess, re
from flask import Flask, request, jsonify
import requests, boto3

app = Flask(__name__)

# ---------- CONFIG (from environment) ----------
S3_BUCKET       = os.getenv("S3_BUCKET", "")
S3_PREFIX       = os.getenv("S3_PREFIX", "renders/")
AWS_REGION      = os.getenv("REGION", "us-east-1")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "")  # optional CDN domain

# Overlay source: prefer LOCAL_OVERLAY if present, else download from OVERLAY_URL
LOCAL_OVERLAY   = os.getenv("LOCAL_OVERLAY", "")    # e.g. ./assets/hand_overlay.mov
OVERLAY_URL     = os.getenv("OVERLAY_URL", "")      # e.g. https://drive.google.com/uc?export=download&id=...

# Output geometry & timing (defaults match your Breeze output)
OUTPUT_WIDTH    = int(os.getenv("OUTPUT_WIDTH", "960"))
OUTPUT_HEIGHT   = int(os.getenv("OUTPUT_HEIGHT", "1440"))
OUTPUT_FPS      = int(os.getenv("OUTPUT_FPS", "20"))

# Overlay placement (top-left by default). Change if your overlay isn’t full-frame.
OVERLAY_X       = os.getenv("OVERLAY_X", "0")
OVERLAY_Y       = os.getenv("OVERLAY_Y", "0")

# ---------- AWS ----------
s3 = boto3.client("s3", region_name=AWS_REGION)

def s3_upload(local_path: str, key: str) -> str:
    """Upload MP4 to S3. Bucket policy should allow public read on the prefix.
       If you prefer private objects, switch to a presigned URL here."""
    s3.upload_file(local_path, S3_BUCKET, key,
                   ExtraArgs={"ContentType": "video/mp4"})
    if PUBLIC_BASE_URL:
        return f"{PUBLIC_BASE_URL.rstrip('/')}/{key}"
    return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"

# ---------- HTTP download helpers (robust, incl. Google Drive) ----------
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

def download(url: str, dest_path: str) -> None:
    """Stream a URL to disk. Handles Google Drive 'confirm' for large files."""
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA})

    # Handle common Google Drive forms:
    m = re.search(r"drive\.google\.com/(?:file/d/|uc\?export=download&(?:amp;)?id=)([^/&?]+)", url)
    if m:
        file_id = m.group(1)
        _download_gdrive(sess, file_id, dest_path)
        return

    with sess.get(url, stream=True, allow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1<<20):
                if chunk:
                    f.write(chunk)

def _download_gdrive(sess: requests.Session, file_id: str, dest_path: str) -> None:
    base = "https://drive.google.com/uc?export=download"
    params = {"id": file_id}
    r = sess.get(base, params=params, stream=True, timeout=120)
    token = _gdrive_confirm_token(r)
    if token:
        params["confirm"] = token
        r = sess.get(base, params=params, stream=True, timeout=120)
    r.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1<<20):
            if chunk:
                f.write(chunk)

def _gdrive_confirm_token(response: requests.Response):
    for k, v in response.cookies.items():
        if k.startswith("download_warning"):
            return v
    m = re.search(r'confirm=([0-9A-Za-z\-_]+)&', response.text or "")
    return m.group(1) if m else None

# ---------- Overlay selection ----------
def get_overlay_path(tmpdir: str) -> str:
    """Pick local overlay if exists; otherwise download from OVERLAY_URL."""
    if LOCAL_OVERLAY and os.path.exists(LOCAL_OVERLAY):
        return LOCAL_OVERLAY
    if OVERLAY_URL:
        p = os.path.join(tmpdir, "overlay.mov")
        download(OVERLAY_URL, p)
        return p
    raise RuntimeError("Overlay not configured (set OVERLAY_URL or LOCAL_OVERLAY).")

# ---------- FFmpeg compositing ----------
def run_ffmpeg(base_mp4: str, overlay_mov: str, out_mp4: str) -> None:
    """
    Pipeline:
      - Force base video to OUTPUT_FPS, square pixels, exact OUTPUT_WIDTHxOUTPUT_HEIGHT.
      - Scale overlay to match output size, keep alpha.
      - Overlay at (OVERLAY_X, OVERLAY_Y).
      - H.264 (yuv420p) with faststart for mobile compatibility.
    """
    filtergraph = (
        f"[0:v]fps={OUTPUT_FPS},setsar=1,"
        f"scale={OUTPUT_WIDTH}:{OUTPUT_HEIGHT}:flags=lanczos[base];"
        f"[1:v]format=rgba,scale={OUTPUT_WIDTH}:{OUTPUT_HEIGHT}:flags=lanczos[ov];"
        f"[base][ov]overlay=x={OVERLAY_X}:y={OVERLAY_Y}:format=auto[v]"
    )

    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-i", base_mp4, "-i", overlay_mov,
        "-filter_complex", filtergraph,
        "-map", "[v]", "-map", "0:a?",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
        "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        out_mp4
    ]
    subprocess.check_call(cmd)

# ---------- Flask endpoints ----------
@app.get("/health")
def health():
    return "ok", 200

@app.post("/webhook")
def webhook():
    """
    Expect JSON with a direct media URL (Breeze Cloud webhook or manual test), e.g.:
      { "id": "demo123", "media_url": "https://.../clip.mp4" }
    We also try a few alternate keys and nested forms.
    """
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        return jsonify({"error": "invalid json"}), 400

    # find a source url in common fields
    src_url = None
    for k in ("media_url", "file_url", "asset_url", "video_url", "url"):
        u = payload.get(k)
        if isinstance(u, str) and u.startswith("http"):
            src_url = u
            break
    if not src_url:
        nested = (((payload.get("data") or {}).get("media") or {}).get("url"))
        if isinstance(nested, str) and nested.startswith("http"):
            src_url = nested
    if not src_url:
        return jsonify({"error": "no source url in payload"}), 400

    uid = str(payload.get("uid") or payload.get("id") or payload.get("session_id") or uuid.uuid4())
    uid = uid.strip().replace(" ", "_").replace("/", "_")[:80]

    with tempfile.TemporaryDirectory() as td:
        base_mp4 = os.path.join(td, "base.mp4")
        out_mp4  = os.path.join(td, f"{uid}_final.mp4")
        try:
            download(src_url, base_mp4)
        except Exception as e:
            return jsonify({"error": "download failed", "detail": str(e)}), 502

        try:
            overlay_path = get_overlay_path(td)
        except Exception as e:
            return jsonify({"error": "overlay not available", "detail": str(e)}), 500

        try:
            run_ffmpeg(base_mp4, overlay_path, out_mp4)
        except subprocess.CalledProcessError as e:
            return jsonify({"error": "ffmpeg failed", "code": e.returncode}), 500

        key = f"{S3_PREFIX.rstrip('/')}/{uid}_final.mp4"
        try:
            final_url = s3_upload(out_mp4, key)
        except Exception as e:
            return jsonify({"error": "s3 upload failed", "detail": str(e)}), 502

    return jsonify({
        "status": "ok",
        "uid": uid,
        "processed_url": final_url,
        "width": OUTPUT_WIDTH,
        "height": OUTPUT_HEIGHT,
        "fps": OUTPUT_FPS
    }), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
