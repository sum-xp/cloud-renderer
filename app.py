import os, json, uuid, subprocess, tempfile
from flask import Flask, request, jsonify
import requests, boto3

app = Flask(__name__)

S3_BUCKET       = os.getenv("S3_BUCKET", "")
S3_PREFIX       = os.getenv("S3_PREFIX", "renders/")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "")    # optional CDN
AWS_REGION      = os.getenv("REGION", "us-east-1")
LOCAL_OVERLAY   = os.getenv("LOCAL_OVERLAY", "./assets/hand_overlay.mov")

s3 = boto3.client("s3", region_name=AWS_REGION)

def _download(url, dest_path):
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1<<20):
                if chunk:
                    f.write(chunk)

def _safe(name: str) -> str:
    return name.strip().replace(" ", "_").replace("/", "_")[:80]

def _run_ffmpeg(base_mp4, overlay_mov, out_mp4):
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-i", base_mp4, "-i", overlay_mov,
        "-filter_complex", "[0:v][1:v]overlay=x=0:y=0:format=auto[v]",
        "-map", "[v]", "-map", "0:a?",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
        "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        out_mp4
    ]
    subprocess.check_call(cmd)

def _s3_upload(local_path, key):
    # Option A (public prefix via bucket policy): no ACL here
    s3.upload_file(local_path, S3_BUCKET, key, ExtraArgs={"ContentType": "video/mp4"})
    if PUBLIC_BASE_URL:
        return f"{PUBLIC_BASE_URL.rstrip('/')}/{key}"
    return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"

@app.get("/health")
def health():
    return "ok", 200

@app.post("/webhook")
def webhook():
    try:
        payload = request.get_json(force=True)
    except Exception:
        return jsonify({"error":"invalid json"}), 400

    # find a source URL in common fields (Breeze Cloud will send one)
    src_url = None
    for k in ("media_url","file_url","asset_url","video_url","url"):
        u = payload.get(k)
        if isinstance(u, str) and u.startswith("http"):
            src_url = u
            break
    if not src_url:
        nested = (((payload.get("data") or {}).get("media") or {}).get("url"))
        if isinstance(nested, str) and nested.startswith("http"):
            src_url = nested
    if not src_url:
        return jsonify({"error":"no source url in payload"}), 400

    uid = _safe(str(payload.get("uid") or payload.get("id") or payload.get("session_id") or uuid.uuid4()))
    with tempfile.TemporaryDirectory() as td:
        base_mp4 = os.path.join(td, "base.mp4")
        out_mp4  = os.path.join(td, f"{uid}_final.mp4")
        _download(src_url, base_mp4)

        if not os.path.exists(LOCAL_OVERLAY):
            return jsonify({"error":"overlay not found in container"}), 500

        try:
            _run_ffmpeg(base_mp4, LOCAL_OVERLAY, out_mp4)
        except subprocess.CalledProcessError as e:
            return jsonify({"error":"ffmpeg failed", "code": e.returncode}), 500

        key = f"{S3_PREFIX.rstrip('/')}/{uid}_final.mp4"
        final_url = _s3_upload(out_mp4, key)

    return jsonify({"status":"ok", "uid":uid, "processed_url":final_url}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","8080")))