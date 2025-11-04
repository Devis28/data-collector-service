from adapters.radio_rock import RadioRockWorker
import threading
import time
import os
import json
from writer import upload_json_to_r2

SONG_INTERVAL = 30
LISTENERS_INTERVAL = 30
UPLOAD_INTERVAL = 600  # 10 minút

DATA_DIR = "data"
os.makedirs(DATA_DIR + "/songs", exist_ok=True)
os.makedirs(DATA_DIR + "/listeners", exist_ok=True)

songs_cache = []
listeners_cache = []

def save_entries(entries, typ):
    dt = time.strftime("%d-%m-%Y")
    tm = time.strftime("%H-%M-%S")
    radio = "ROCK"
    folder = f"{DATA_DIR}/{typ}/{dt}"
    os.makedirs(folder, exist_ok=True)
    local_file_path = f"{folder}/{tm}.json"
    with open(local_file_path, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
    return local_file_path, dt, tm

def upload_worker():
    time.sleep(UPLOAD_INTERVAL)
    while True:
        songs_to_upload = songs_cache.copy()
        listeners_to_upload = listeners_cache.copy()
        songs_cache.clear()
        listeners_cache.clear()
        if songs_to_upload:
            local_file, dt, tm = save_entries(songs_to_upload, "song")
            r2_key = f"bronze/ROCK/song/{dt}/{tm}.json"
            upload_json_to_r2(local_file, r2_key)
        if listeners_to_upload:
            local_file, dt, tm = save_entries(listeners_to_upload, "listeners")
            r2_key = f"bronze/ROCK/listeners/{dt}/{tm}.json"
            upload_json_to_r2(local_file, r2_key)
        print(f"[{time.strftime('%d.%m.%Y %H:%M:%S')}] [ROCK ---] Dáta boli odoslané do Cloudflare R2.")
        time.sleep(UPLOAD_INTERVAL)

def main():
    worker = RadioRockWorker(
        SONG_INTERVAL,
        LISTENERS_INTERVAL,
        songs_cache,
        listeners_cache
    )
    worker.start()
    threading.Thread(target=upload_worker, daemon=True).start()
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
