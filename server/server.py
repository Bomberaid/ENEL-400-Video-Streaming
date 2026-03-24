import os
import socket
import threading
import time
from flask import Flask, send_from_directory
from flask_socketio import SocketIO

UDP_PORT = 3000

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR  = os.path.join(BASE_DIR, "../web")

app      = Flask(__name__, static_folder=WEB_DIR)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

latest_frame     = None
frame_buffer     = {}
frame_lock       = threading.Lock()
frame_timestamps = {}
frames_completed = 0
frames_dropped   = 0

last_emit_time     = 0
MIN_FRAME_INTERVAL = 1 / 30


@app.route("/")
def index():
    return send_from_directory(WEB_DIR, "index.html")


@socketio.on("connect")
def on_connect():
    print("Viewer connected")
    if latest_frame:
        socketio.emit("frame", bytes(latest_frame))


@socketio.on("disconnect")
def on_disconnect():
    print("Viewer disconnected")


def reassemble_frame(chunks, total):
    if len(chunks) != total:
        return None
    return b"".join(chunks[i] for i in range(total))


def is_valid_jpeg(data: bytes) -> bool:
    return (
        len(data) > 4
        and data[:2] == b"\xff\xd8"
        and data[-2:] == b"\xff\xd9"
    )


def evict_stale_frames():
    global frames_dropped
    now = time.time()
    stale = [
        fid for fid, ts in frame_timestamps.items()
        if now - ts > 0.5
    ]
    for fid in stale:
        frame_buffer.pop(fid, None)
        frame_timestamps.pop(fid, None)
        frames_dropped += 1


def send_ack(sock, addr, frame_id: int):
    """Send a 2-byte ACK back to the ESP32 with the confirmed frame ID."""
    ack = bytes([(frame_id >> 8) & 0xFF, frame_id & 0xFF])
    sock.sendto(ack, addr)


def udp_listener():
    global latest_frame, frame_buffer, frames_completed, last_emit_time

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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
            evict_stale_frames()

            if frame_id not in frame_buffer:
                frame_buffer[frame_id]     = {}
                frame_timestamps[frame_id] = time.time()

            frame_buffer[frame_id][chunk_index] = payload

            if len(frame_buffer[frame_id]) == total_chunks:
                frame = reassemble_frame(frame_buffer[frame_id], total_chunks)
                del frame_buffer[frame_id]
                frame_timestamps.pop(frame_id, None)

                if frame and is_valid_jpeg(frame):
                    # Send ACK back to ESP32
                    send_ack(sock, addr, frame_id)

                    frames_completed += 1
                    if frames_completed % 100 == 0:
                        total = frames_completed + frames_dropped
                        loss  = (frames_dropped / total) * 100
                        print(f"Frames: {frames_completed} completed, "
                              f"{frames_dropped} dropped ({loss:.1f}% loss)")

                    latest_frame = bytes(frame)

                    now = time.time()
                    if now - last_emit_time >= MIN_FRAME_INTERVAL:
                        last_emit_time = now
                        socketio.emit("frame", latest_frame)


udp_thread = threading.Thread(target=udp_listener, daemon=True)
udp_thread.start()


if __name__ == "__main__":
    print("Starting Flask-SocketIO server on :8080")
    socketio.run(app, host="0.0.0.0", port=8080, debug=False)