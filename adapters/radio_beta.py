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

SONG_URL = "https://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/now-playing"
LISTENERS_WS = "wss://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/listeners"
RADIO_NAME = "BETA"
ZONE = ZoneInfo("Europe/Bratislava")

def log(song_session_id, msg):
    now = datetime.datetime.now(ZONE).strftime('%d.%m.%Y %H:%M:%S')
    print(f"[{now}] [{RADIO_NAME}{' ' * (8 - len(RADIO_NAME))} {song_session_id}] {msg}")

class RadioBetaWorker:
    def __init__(self, song_interval, listeners_interval, songs_cache, listeners_cache):
        self.song_interval = song_interval
        self.listeners_interval = listeners_interval
        self.songs_cache = songs_cache
        self.listeners_cache = listeners_cache
        self.current_song_id = None
        self.last_song = None
        self.running = True

    def validate_song(self, data):
        # Song playing
        song_fields = {"radio", "interpreters", "title", "start_time", "timestamp"}
        silence_fields = {"radio", "is_playing", "message", "timestamp"}
        # Song playing
        if isinstance(data, dict) and set(data.keys()) >= song_fields:
            # Musí sedieť na tvoje parametre, napr. radio == "Beta"
            return (
                data.get("radio") == "Beta" and
                isinstance(data.get("interpreters"), str) and
                isinstance(data.get("title"), str) and
                isinstance(data.get("start_time"), str) and
                isinstance(data.get("timestamp"), str)
            )
        # Silence object
        if isinstance(data, dict) and set(data.keys()) >= silence_fields:
            return (
                data.get("radio") == "Beta" and
                data.get("is_playing") is False and
                isinstance(data.get("message"), str) and
                isinstance(data.get("timestamp"), str)
            )
        return False

    def is_new_song(self, data):
        # Song playing
        if "interpreters" in data and "title" in data:
            if self.last_song is None:
                return True
            return (data["interpreters"] != self.last_song.get("interpreters") or data["title"] != self.last_song.get("title"))
        # Silence object as "Nothing is playing now"
        if data.get("is_playing") is False:
            if self.last_song is None or (self.last_song.get("is_playing") is not False or self.last_song.get("message") != data.get("message")):
                return True
        return False

    def validate_listeners(self, data):
        required_keys = {"listeners", "timestamp"}
        if isinstance(data, dict) and set(data.keys()) >= required_keys:
            return (
                isinstance(data.get("listeners"), int) and
                isinstance(data.get("timestamp"), str)
            )
        return False

    def poll_song(self):
        while self.running:
            try:
                resp = requests.get(SONG_URL, timeout=5)
                now = datetime.datetime.now(ZONE).isoformat()
                if resp.status_code == 200:
                    data = resp.json()
                    is_valid = self.validate_song(data)
                    if is_valid and self.is_new_song(data):
                        self.current_song_id = str(uuid.uuid4())
                        self.last_song = copy.deepcopy(data)
                        if data.get("is_playing") is False:  # Silence message
                            song_str = f"{data.get('message', 'Nothing is playing right now.')} | {data.get('timestamp', 'N/A')}"
                        else:
                            song_str = f"Nový song: {data.get('title', 'N/A')} | {data.get('interpreters', 'N/A')} | {data.get('start_time', 'N/A')}"
                        log(self.current_song_id, song_str)
                        raw_entry = copy.deepcopy(data)
                        raw_entry["recorded_at"] = now
                        raw_entry["raw_valid"] = True
                        raw_entry["song_session_id"] = self.current_song_id if self.current_song_id else ""
                        self.songs_cache.append(raw_entry)
                    elif not is_valid:
                        raw_entry = copy.deepcopy(data)
                        raw_entry["recorded_at"] = now
                        raw_entry["raw_valid"] = False
                        raw_entry["song_session_id"] = ""
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
                            listeners_str = f"Počet poslucháčov: {listeners_entry.get('listeners', 'N/A')}"
                            log(listeners_entry['song_session_id'], listeners_str)
                            self.listeners_cache.append(listeners_entry)
                        except Exception as ex:
                            log(self.current_song_id, f"Chyba parsovania listeners: {ex}")
                        await asyncio.sleep(self.listeners_interval)
            except Exception as e:
                log(self.current_song_id, f"WebSocket chyba: {e}")
                time.sleep(10)
                await asyncio.sleep(self.listeners_interval)

    def start(self):
        threading.Thread(target=self.poll_song, daemon=True).start()
        def run_ws():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.listen_listeners())
        threading.Thread(target=run_ws, daemon=True).start()
