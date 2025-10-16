import os, re, json, time, logging, tempfile, subprocess
from typing import Any, Optional
from urllib.parse import urlparse, urljoin

import boto3, requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request

# =========================
# Config
# =========================
AWS_REGION     = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET      = os.getenv("S3_BUCKET", "sumxp-renders")
OUTPUT_PREFIX  = os.getenv("OUTPUT_PREFIX", "renders/").rstrip("/") + "/"

# Overlay sources (set ONE of these)
OVERLAY_URL    = os.getenv("OVERLAY_URL")        # https://.../hand_overlay.mov (presigned ok)
OVERLAY_S3_KEY = os.getenv("OVERLAY_S3_KEY")     # e.g., "hand_overlay.mov"

# Overlay behavior
OVERLAY_POS    = os.getenv("OVERLAY_POS", "0:0")       # e.g., "10:20"
OVERLAY_SCALE  = os.getenv("OVERLAY_SCALE")            # "match-base" or unset

# Output normalization (optional; Breeze is 960x1440 @ 20fps)
FORCE_SIZE     = os.getenv("FORCE_SIZE")               # e.g., "960x1440"
FORCE_FPS      = int(os.getenv("FORCE_FPS", "20"))

FFMPEG_LOGLEVEL = os.getenv("FFMPEG_LOGLEVEL", "error")  # quiet|panic|fatal|error|warning|info|debug

# AWS clients
session = boto3.session.Session(region_name=AWS_REGION)
s3 = session.client("s3")

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = app.logger

# =========================
# Helpers
# =========================
def looks_like_mp4(u: str) -> bool:
    if not isinstance(u, str) or not u: return False
    base = u.split("?", 1)[0].lower()
    return base.endswith(".mp4")

def _find_mp4_in_obj(obj: Any) -> Optional[str]:
    if isinstance(obj, str):
        m = re.search(r"https?://\S+?\.mp4(?:\?\S+)?", obj, re.IGNORECASE)
        return m.group(0) if m else None
    if isinstance(obj, dict):
        for k, v in obj.items():
            hit = _find_mp4_in_obj(v)
            if hit: return hit
    if isinstance(obj, (list, tuple)):
        for v in obj:
            hit = _find_mp4_in_obj(v)
            if hit: return hit
    return None

def extract_mp4_or_page_url(payload: dict) -> Optional[str]:
    # common top-level
    for key in ("media_url", "mp4_url", "video_url", "url", "image_url"):
        url = payload.get(key)
        if isinstance(url, str) and url.strip():
            return url
    # nested
    for key in ("data", "files", "media", "assets"):
        if key in payload:
            hit = _find_mp4_in_obj(payload[key])
            if hit: return hit
    # last resort: scan whole thing
    return _find_mp4_in_obj(payload)

def retry(n=3, backoff=0.7):
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
def http_get(url: str, stream=False):
    r = requests.get(url, headers={"User-Agent": "cloud-renderer/1.0"}, stream=stream, timeout=30)
    r.raise_for_status()
    return r

@retry(n=3, backoff=0.7)
def http_download(url: str, dst_path: str):
    with http_get(url, stream=True) as r:
        with open(dst_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk: f.write(chunk)

def s3_download(bucket: str, key: str, dst_path: str):
    s3.download_file(bucket, key, dst_path)

def s3_upload_file(src_path: str, bucket: str, key: str):
    s3.upload_file(src_path, bucket, key)

def presigned_get(bucket: str, key: str, seconds: int = 3600) -> str:
    return s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=seconds)

def resolve_microsite_to_mp4(url: str) -> Optional[str]:
    """Resolve Breeze microsite or CDN preview pages to a direct .mp4 if needed."""
    if looks_like_mp4(url): return url
    host = urlparse(url).netloc.lower()
    if any(x in host for x in ("share.", "sharethis.", "eventkite-", "b-cdn.net")):
        try:
            r = http_get(url, stream=False)
            if looks_like_mp4(r.url):  # redirect straight to mp4
                return r.url
            if "html" in (r.headers.get("Content-Type","").lower()):
                soup = BeautifulSoup(r.text, "html.parser")
                # video/source tags
                for tag in soup.find_all(["source", "video"]):
                    src = tag.get("src")
                    if src:
                        full = urljoin(r.url, src)
                        if looks_like_mp4(full): return full
                # anchors
                for a in soup.find_all("a", href=True):
                    full = urljoin(r.url, a["href"])
                    if looks_like_mp4(full): return full
                # plain text fallback
                m = re.search(r"https?://\S+?\.mp4(?:\?\S+)?", r.text, re.IGNORECASE)
                if m: return m.group(0)
        except Exception as e:
            log.warning("Microsite scrape failed: %s", e)
    return None

def maybe_download_overlay(tmpdir: str) -> Optional[str]:
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
        out = subprocess.check_output(
            ["ffprobe","-v","error","-select_streams","v:0","-show_entries","stream=r_frame_rate","-of","default=noprint_wrappers=1:nokey=1", path],
            text=True
        ).strip()
        if "/" in out:
            num, den = out.split("/")
            return float(num) / float(den)
        return float(out)
    except Exception:
        return None

def probe_meta(path: str) -> dict:
    try:
        out = subprocess.check_output(
            ["ffprobe","-v","error","-select_streams","v:0",
             "-show_entries","stream=width,height,r_frame_rate",
             "-of","json", path],
            text=True
        )
        j = json.loads(out); st = j["streams"][0]
        fr = st.get("r_frame_rate","20/1")
        fps = round(float(fr.split("/")[0]) / float(fr.split("/")[1])) if "/" in fr else round(float(fr))
        return {"width": st.get("width"), "height": st.get("height"), "fps": fps}
    except Exception:
        return {}

def build_ffmpeg_cmd(input_mp4: str, output_mp4: str, overlay_path: Optional[str]) -> list:
    """
    Build a filtergraph that:
      - (optional) scales base to FORCE_SIZE
      - (optional) scales overlay to match base if OVERLAY_SCALE == "match-base"
      - overlays at OVERLAY_POS
      - encodes yuv420p, +faststart, at FORCE_FPS (default 20)
    """
    inputs = ["-i", input_mp4]
    filters = []
    labels = {}

    # Base stream label
    base_label = "[base]"
    if FORCE_SIZE:
        w, h = FORCE_SIZE.split("x", 1)
        filters.append(f"[0:v]scale={w}:{h}:flags=bicubic,format=rgba{base_label}")
    else:
        filters.append(f"[0:v]format=rgba{base_label}")

    # Overlay stream
    overlay_label = None
    if overlay_path:
        inputs += ["-i", overlay_path]
        if OVERLAY_SCALE == "match-base":
            overlay_label = "[ol]"
            # scale overlay to base dims via scale2ref
            filters.append(f"[1:v][0:v]scale2ref=w=iw:h=ih[olpre][ref]")
            # ensure rgba on overlay
            filters.append(f"[olpre]format=rgba{overlay_label}")
            # re-define base_label to [ref] (same as base)
            base_label = "[ref]"
        else:
            overlay_label = "[ol]"
            filters.append(f"[1:v]format=rgba{overlay_label}")

        # Composite
        filters.append(f"{base_label}{overlay_label}overlay={OVERLAY_POS}[vout]")
        vmap = ["-map", "[vout]"]
    else:
        vmap = ["-map", base_label]

    filter_arg = ["-filter_complex", ";".join(filters)] if filters else []

    fps = int(round(ffprobe_fps(input_mp4) or FORCE_FPS))
    cmd = [
        "ffmpeg","-y","-loglevel", FFMPEG_LOGLEVEL,
        *inputs,
        *filter_arg,
        *vmap,
        "-c:v","libx264","-pix_fmt","yuv420p",
        "-r", str(fps),
        "-movflags","+faststart",
        "-c:a","aac","-b:a","128k",
        output_mp4
    ]
    return cmd

def run_ffmpeg(input_mp4: str, output_mp4: str, overlay_path: Optional[str]):
    cmd = build_ffmpeg_cmd(input_mp4, output_mp4, overlay_path)
    log.info("FFmpeg cmd: %s", " ".join(cmd))
    subprocess.check_call(cmd)

# =========================
# Routes
# =========================
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
        return jsonify({"status":"bad_request","reason":"invalid_json"}), 400

    uid = str(payload.get("id") or payload.get("uid") or int(time.time()))
    src = extract_mp4_or_page_url(payload)

    if src and not looks_like_mp4(src):
        resolved = resolve_microsite_to_mp4(src)
        if resolved:
            log.info("🔎 microsite resolved to MP4: %s", resolved)
            src = resolved

    if not src or not looks_like_mp4(src):
        log.warning("⚠️ No usable MP4 found; keys=%s", list(payload.keys()))
        return jsonify({"status":"ignored","reason":"no_mp4"}), 200

    with tempfile.TemporaryDirectory() as tmp:
        src_path = os.path.join(tmp, "source.mp4")
        out_path = os.path.join(tmp, f"{uid}_final.mp4")

        # 1) download source
        log.info("⬇️  downloading MP4: %s", src)
        http_download(src, src_path)

        # 2) overlay (if provided)
        overlay_local = maybe_download_overlay(tmp)

        # 3) render
        t0 = time.time()
        run_ffmpeg(src_path, out_path, overlay_local)
        elapsed = time.time() - t0

        # 4) probe BEFORE cleanup
        meta = probe_meta(out_path)

        # 5) upload
        size_mb = os.path.getsize(out_path) / (1024*1024)
        key = f"{OUTPUT_PREFIX}{uid}_final.mp4"
        log.info("⬆️  uploading to s3://%s/%s (%.2f MB)", S3_BUCKET, key, size_mb)
        s3_upload_file(out_path, S3_BUCKET, key)

        # 6) presign
        signed = presigned_get(S3_BUCKET, key, seconds=3600)

    log.info("✅ done uid=%s time=%.2fs meta=%s", uid, elapsed, meta or {})
    return jsonify({
        "status":"ok","uid":uid,
        "processed_url": signed,              # presigned (no 403)
        "s3_uri": f"s3://{S3_BUCKET}/{key}",
        **meta
    }), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","10000")))
