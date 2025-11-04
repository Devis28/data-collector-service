import threading
import requests
import time
import json
import uuid
import datetime
import websockets
import asyncio

SONG_URL = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS = "wss://rock-server.fly.dev/ws/push/listenership"

class RadioRockAdapter:
    def __init__(self, song_poll_interval=30, listeners_interval=30, listeners_writer=None, song_writer=None):
        self.song_poll_interval = song_poll_interval
        self.listeners_interval = listeners_interval
        self.current_song_id = None
        self.last_song = None
        self.listeners_writer = listeners_writer
        self.song_writer = song_writer
        self.running = True

    def validate_song(self, data):
        required_keys = {"musicAuthor", "musicTitle", "musicCover", "radio", "startTime"}
        return (
            isinstance(data, dict) and
            "song" in data and
            isinstance(data["song"], dict) and
            set(data["song"].keys()) == required_keys and
            "last_update" in data
        )

    def validate_listeners(self, data):
        return (
            isinstance(data, dict) and
            set(data.keys()) == {"listeners"}
            and isinstance(data["listeners"], int)
        )

    def poll_song(self):
        while self.running:
            try:
                resp = requests.get(SONG_URL, timeout=5)
                now = datetime.datetime.utcnow().isoformat()
                if resp.status_code == 200:
                    data = resp.json()
                    is_valid = self.validate_song(data)
                    song_info = data["song"] if is_valid else data.get("song", {})
                    # Urči novú skladbu podľa autora a názvu
                    if (
                        is_valid and
                        (self.last_song is None or
                         song_info["musicAuthor"] != self.last_song["musicAuthor"] or
                         song_info["musicTitle"] != self.last_song["musicTitle"])
                    ):
                        self.current_song_id = str(uuid.uuid4())
                        self.last_song = song_info.copy()
                        print("Nová skladba:", song_info)
                    song_entry = {
                        **song_info,
                        "recorded_at": now,
                        "raw_valid": bool(is_valid),
                        "song_session_id": self.current_song_id if self.current_song_id else "",
                    }
                    if self.song_writer:
                        self.song_writer(song_entry)
                else:
                    print("Chyba HTTP song:", resp.status_code)
            except Exception as e:
                print("Chyba song poll:", e)
            time.sleep(self.song_poll_interval)

    async def listen_listeners(self):
        while self.running:
            try:
                async with websockets.connect(LISTENERS_WS) as ws:
                    print("WebSocket pripojený.")
                    while self.running:
                        msg = await ws.recv()
                        now = datetime.datetime.utcnow().isoformat()
                        try:
                            listeners_data = json.loads(msg)
                            valid = self.validate_listeners(listeners_data)
                            listeners_entry = {
                                "listeners": listeners_data.get("listeners"),
                                "recorded_at": now,
                                "raw_valid": bool(valid),
                                "song_session_id": self.current_song_id if self.current_song_id else ""
                            }
                            print("Listeners:", listeners_entry)
                            if self.listeners_writer:
                                self.listeners_writer(listeners_entry)
                        except Exception as ex:
                            print("Chyba parsovania listeners:", ex)
            except Exception as e:
                print("WebSocket chyba:", e)
            await asyncio.sleep(self.listeners_interval)

    def start(self):
        threading.Thread(target=self.poll_song, daemon=True).start()
        asyncio.get_event_loop().create_task(self.listen_listeners())
