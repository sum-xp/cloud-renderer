from flask import Flask, request, jsonify

app = Flask(__name__)

@app.get("/")
def root():
    return jsonify(ok=True), 200

@app.get("/health")
def health():
    # also responds to Render's HEAD probe automatically
    return jsonify(status="ok"), 200

@app.post("/webhook")
def webhook():
    payload = request.get_json(silent=True)
    # echo back for now; we’ll add FFmpeg once this is stable
    return jsonify(ok=True, received=payload), 200
