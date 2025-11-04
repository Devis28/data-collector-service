import requests
import datetime
import threading
import time
import json
import uuid
import asyncio
import websockets
import copy
from zoneinfo import ZoneInfo

SONG_URL = "https://funradio-server.fly.dev/pull/playing"
LISTENERS_WS = "wss://funradio-server.fly.dev/ws/push/listenership"
RADIO_NAME = "FUNRADIO"
ZONE = ZoneInfo("Europe/Bratislava")

def log(song_session_id, msg):
    now = datetime.datetime.now(ZONE).strftime('%d.%m.%Y %H:%M:%S')
    print(f"[{now}] [{RADIO_NAME} {song_session_id}] {msg}")

class RadioFunradioWorker:
    def __init__(self, song_interval, listeners_interval, songs_cache, listeners_cache):
        self.song_interval = song_interval
        self.listeners_interval = listeners_interval
        self.songs_cache = songs_cache
        self.listeners_cache = listeners_cache
        self.current_song_id = None
        self.last_song = None
        self.running = True

    def validate_song(self, data):
        required_keys = {"musicAuthor", "musicTitle", "musicCover", "radio", "startTime"}
        if (
            isinstance(data, dict) and
            "song" in data and
            isinstance(data["song"], dict) and
            set(data["song"].keys()) == required_keys and
            "last_update" in data
        ):
            return True
        return False

    def validate_listeners(self, data):
        return (
            isinstance(data, dict) and set(data.keys()) == {"listeners"} and isinstance(data["listeners"], int)
        )

    def poll_song(self):
        while self.running:
            try:
                resp = requests.get(SONG_URL, timeout=5)
                now = datetime.datetime.now(ZONE).isoformat()
                if resp.status_code == 200:
                    data = resp.json()
                    is_valid = self.validate_song(data)
                    song_info = data["song"] if is_valid else data.get("song", {})
                    # Nová skladba podľa autora a názvu
                    if is_valid and (self.last_song is None or song_info["musicAuthor"] != self.last_song["musicAuthor"] or song_info["musicTitle"] != self.last_song["musicTitle"]):
                        self.current_song_id = str(uuid.uuid4())
                        self.last_song = song_info.copy()
                        song_str = f"Nový song: {song_info.get('musicTitle', 'N/A')} | {song_info.get('musicAuthor', 'N/A')} | {song_info.get('startTime', 'N/A')}"
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

    async def listen_listeners(self):
        while self.running:
            try:
                async with websockets.connect(LISTENERS_WS) as ws:
                    while self.running:
                        msg = await ws.recv()
                        now = datetime.datetime.now(ZONE).isoformat()
                        try:
                            listeners_data = json.loads(msg)
                            valid = self.validate_listeners(listeners_data)
                            listeners_entry = copy.deepcopy(listeners_data)
                            listeners_entry["recorded_at"] = now
                            listeners_entry["raw_valid"] = bool(valid)
                            listeners_entry["song_session_id"] = self.current_song_id if self.current_song_id else ""
                            listeners_str = f"Počet poslucháčov: {listeners_entry['listeners']}"
                            log(listeners_entry['song_session_id'], listeners_str)
                            self.listeners_cache.append(listeners_entry)
                        except Exception as ex:
                            log(self.current_song_id, f"Chyba parsovania listeners: {ex}")
                        await asyncio.sleep(self.listeners_interval)
            except Exception as e:
                log(self.current_song_id, f"WebSocket chyba: {e}")
                await asyncio.sleep(self.listeners_interval)

    def start(self):
        threading.Thread(target=self.poll_song, daemon=True).start()
        def run_ws():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.listen_listeners())
        threading.Thread(target=run_ws, daemon=True).start()
