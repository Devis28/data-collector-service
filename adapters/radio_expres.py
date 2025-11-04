import datetime
import threading
import time
import json
import uuid
import copy
from zoneinfo import ZoneInfo
import requests
from flask import Flask, request

SONG_WEBHOOK_URL = "/expres_webhook"
LISTENERS_URL = "http://147.232.205.56:5010/api/current_listeners"
RADIO_NAME = "EXPRES"
ZONE = ZoneInfo("Europe/Bratislava")

def log(song_session_id, msg):
    now = datetime.datetime.now(ZONE).strftime('%d.%m.%Y %H:%M:%S')
    print(f"[{now}] [{RADIO_NAME}{' ' * (8 - len(RADIO_NAME))} {song_session_id}] {msg}")

class RadioExpresWorker:
    def __init__(self, listeners_interval, listeners_cache, songs_cache):
        self.listeners_interval = listeners_interval
        self.songs_cache = songs_cache
        self.listeners_cache = listeners_cache
        self.current_song_id = None
        self.last_song = None
        self.running = True

    def validate_song(self, data):
        # Pravý formát skladby je objekt so správnymi atribútmi
        expected_keys = {"song", "artists", "isrc", "start_time", "radio"}
        if (
            isinstance(data, dict)
            and set(data.keys()) == expected_keys
            and isinstance(data["artists"], list)
            and data["radio"] == "expres"
        ):
            return True
        return False

    def validate_listeners(self, data):
        expected_keys = {"timestamp", "listeners", "radio"}
        if (
            isinstance(data, dict)
            and set(data.keys()) == expected_keys
            and data["radio"] == "expres"
            and isinstance(data["listeners"], int)
        ):
            return True
        return False

    def poll_listeners(self):
        while self.running:
            try:
                resp = requests.get(LISTENERS_URL, timeout=5)
                now = datetime.datetime.now(ZONE).isoformat()
                if resp.status_code == 200:
                    data = resp.json()
                    is_valid = self.validate_listeners(data)
                    listeners_entry = copy.deepcopy(data)
                    listeners_entry["recorded_at"] = now
                    listeners_entry["raw_valid"] = bool(is_valid)
                    listeners_entry["song_session_id"] = self.current_song_id if self.current_song_id else ""
                    log(listeners_entry["song_session_id"], f"Počet poslucháčov: {listeners_entry.get('listeners', 'N/A')}")
                    self.listeners_cache.append(listeners_entry)
                else:
                    log(self.current_song_id, f"Chyba HTTP listeners: {resp.status_code}")
            except Exception as e:
                log(self.current_song_id, f"Chyba listeners poll: {e}")
            time.sleep(self.listeners_interval)

# Flask app na príjem skladieb cez webhook
app = Flask(__name__)
worker_instance = None

@app.route(SONG_WEBHOOK_URL, methods=['POST'])
def webhook():
    now = datetime.datetime.now(ZONE).isoformat()
    data = request.get_json(force=True)
    valid = worker_instance.validate_song(data)
    entry = copy.deepcopy(data)
    entry["recorded_at"] = now
    entry["raw_valid"] = bool(valid)
    if valid:
        # Nový song s jedinečným ID
        worker_instance.current_song_id = str(uuid.uuid4())
        worker_instance.last_song = data.copy()
        entry["song_session_id"] = worker_instance.current_song_id
        song_str = f"Nový song: {data.get('song', 'N/A')} | {', '.join(data.get('artists', []))} | {data.get('start_time', 'N/A')}"
        log(worker_instance.current_song_id, song_str)
    else:
        entry["song_session_id"] = ""
        log("", f"Chybný formát song")
    worker_instance.songs_cache.append(entry)
    return {"status": "ok"}

def start_flask():
    app.run(host="0.0.0.0", port=8001)

def start_worker(listeners_interval, listeners_cache, songs_cache):
    global worker_instance
    worker_instance = RadioExpresWorker(listeners_interval, listeners_cache, songs_cache)
    threading.Thread(target=worker_instance.poll_listeners, daemon=True).start()
    threading.Thread(target=start_flask, daemon=True).start()
