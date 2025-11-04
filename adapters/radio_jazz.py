import requests
import datetime
import threading
import time
import json
import uuid
import copy
from zoneinfo import ZoneInfo
from flask import Flask, request

SONG_URL = "http://147.232.40.154:8000/current"
RADIO_NAME = "JAZZ"
ZONE = ZoneInfo("Europe/Bratislava")

def log(song_session_id, msg):
    now = datetime.datetime.now(ZONE).strftime('%d.%m.%Y %H:%M:%S')
    print(f"[{now}] [{RADIO_NAME} {song_session_id}] {msg}")

class RadioJazzWorker:
    def __init__(self, song_interval, listeners_cache, songs_cache):
        self.song_interval = song_interval
        self.songs_cache = songs_cache
        self.listeners_cache = listeners_cache
        self.current_song_id = None
        self.last_song = None
        self.running = True

    def validate_song(self, data):
        if (
            isinstance(data, dict) and
            "song" in data and
            isinstance(data["song"], dict)
        ):
            song_data = data["song"]
            if (
                set(song_data.keys()) == {"play_date", "play_time", "artist", "title"} and
                isinstance(song_data["artist"], list)
            ):
                return True
        return False

    def validate_listeners(self, data):
        if (
            isinstance(data, dict) and
            set(data.keys()) == {"timestamp", "listeners", "radio"} and
            data.get("radio") == "jazz" and
            isinstance(data["listeners"], int)
        ):
            return True
        return False

    def poll_song(self):
        while self.running:
            try:
                resp = requests.get(SONG_URL, timeout=5)
                now = datetime.datetime.now(ZONE).isoformat()
                if resp.status_code == 200:
                    data = resp.json()
                    is_valid = self.validate_song(data)
                    song_data = data["song"] if is_valid else data.get("song", {})
                    if is_valid and (self.last_song is None or song_data["artist"] != self.last_song["artist"] or song_data["title"] != self.last_song["title"]):
                        self.current_song_id = str(uuid.uuid4())
                        self.last_song = song_data.copy()
                        song_str = f"Nový song: {song_data.get('title', 'N/A')} | {', '.join(song_data.get('artist', []))} | {song_data.get('play_time', 'N/A')}"
                        log(self.current_song_id, song_str)
                        raw_entry = copy.deepcopy(data)
                        raw_entry["recorded_at"] = now
                        raw_entry["raw_valid"] = bool(is_valid)
                        raw_entry["song_session_id"] = self.current_song_id if self.current_song_id else ""
                        self.songs_cache.append(raw_entry)
                else:
                    log(self.current_song_id, f"Chyba HTTP song: {resp.status_code}")
            except Exception as e:
                log(self.current_song_id, f"Chyba song poll: {e}")
            time.sleep(self.song_interval)

# Flask app for listeners callback
app = Flask(__name__)
worker_instance = None

@app.route('/callback', methods=['POST'])
@app.route('/callback-jazz', methods=['POST'])

def callback():
    now = datetime.datetime.now(ZONE).isoformat()
    data = request.get_json(force=True)
    valid = worker_instance.validate_listeners(data)
    entry = copy.deepcopy(data)
    entry["recorded_at"] = now
    entry["raw_valid"] = bool(valid)
    entry["song_session_id"] = worker_instance.current_song_id if worker_instance.current_song_id else ""
    listeners_str = f"Počet poslucháčov: {entry.get('listeners', 'N/A')}"
    log(entry["song_session_id"], listeners_str)
    worker_instance.listeners_cache.append(entry)
    return {"status": "ok"}

def start_flask():
    app.run(host="0.0.0.0", port=8002)

def start_worker(song_interval, listeners_cache, songs_cache):
    global worker_instance
    worker_instance = RadioJazzWorker(song_interval, listeners_cache, songs_cache)
    threading.Thread(target=worker_instance.poll_song, daemon=True).start()
    threading.Thread(target=start_flask, daemon=True).start()
