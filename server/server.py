import os
import socket
import threading
from flask import Flask, send_from_directory
from flask_socketio import SocketIO

UDP_PORT = 3000

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR  = os.path.join(BASE_DIR, "../web")

app      = Flask(__name__, static_folder=WEB_DIR)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

latest_frame = None
frame_buffer = {}
frame_lock   = threading.Lock()


@app.route("/")
def index():
    return send_from_directory(WEB_DIR, "index.html")


@socketio.on("connect")
def on_connect():
    print("Viewer connected")
    if latest_frame:
        socketio.emit("frame", latest_frame)


@socketio.on("disconnect")
def on_disconnect():
    print("Viewer disconnected")


def reassemble_frame(chunks, total):
    if len(chunks) != total:
        return None
    return b"".join(chunks[i] for i in range(total))


def udp_listener():
    global latest_frame, frame_buffer

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Increase receive buffer to 4MB
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    sock.bind(("0.0.0.0", UDP_PORT))
    print(f"UDP ingest listening on :{UDP_PORT}")

    while True:
        data, addr = sock.recvfrom(2048)
        if len(data) < 6:
            continue

        frame_id     = (data[0] << 8) | data[1]
        chunk_index  = (data[2] << 8) | data[3]
        total_chunks = (data[4] << 8) | data[5]
        payload      = data[6:]

        with frame_lock:
            if frame_id not in frame_buffer:
                if len(frame_buffer) > 10:
                    oldest = min(frame_buffer.keys())
                    del frame_buffer[oldest]
                frame_buffer[frame_id] = {}

            frame_buffer[frame_id][chunk_index] = payload

            if len(frame_buffer[frame_id]) == total_chunks:
                frame = reassemble_frame(frame_buffer[frame_id], total_chunks)
                del frame_buffer[frame_id]

                if frame:
                    latest_frame = frame
                    socketio.emit("frame", frame)


# This runs when gunicorn imports the module, not just when run directly
udp_thread = threading.Thread(target=udp_listener, daemon=True)
udp_thread.start()


if __name__ == "__main__":
    print("Starting Flask-SocketIO server on :8080")
    socketio.run(app, host="0.0.0.0", port=8080, debug=False)