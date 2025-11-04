from adapters.radio_rock import RadioRockWorker
import threading
import time
import os
import json
from writer import upload_json_to_r2

# Intervaly v sekundách (pre jednoduchosť môžeš zmeniť)
SONG_INTERVAL = 30
LISTENERS_INTERVAL = 30
UPLOAD_INTERVAL = 600  # 10 minút

DATA_DIR = "data"
os.makedirs(DATA_DIR + "/songs", exist_ok=True)
os.makedirs(DATA_DIR + "/listeners", exist_ok=True)

def save_entry(entry, typ):
    dt = time.strftime("%d-%m-%Y")
    tm = time.strftime("%H-%M-%S")
    radio = "ROCK"
    folder = f"{DATA_DIR}/{typ}/{dt}"
    os.makedirs(folder, exist_ok=True)
    local_file_path = f"{folder}/{tm}.json"
    with open(local_file_path, "w", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False, indent=2)

def song_writer(entry):
    save_entry(entry, "song")

def listeners_writer(entry):
    save_entry(entry, "listeners")

def upload_trigger(entries, typ):
    # Zoskupí aktuálne zaznamenané záznamy do jedného JSON file podľa Cloudflare štruktúry
    dt = time.strftime("%d-%m-%Y")
    tm = time.strftime("%H-%M-%S")
    radio = "ROCK"
    r2_key = f"bronze/{radio}/{typ}/{dt}/{tm}.json"
    local_file_path = f"data/upload_{radio}_{typ}_{dt}_{tm}.json"
    with open(local_file_path, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
    upload_json_to_r2(local_file_path, r2_key)

def start_radio_worker():
    worker = RadioRockWorker(
        SONG_INTERVAL,
        LISTENERS_INTERVAL,
        UPLOAD_INTERVAL,
        song_writer,
        listeners_writer,
        upload_trigger
    )
    thread = threading.Thread(target=worker.start, daemon=True)
    thread.start()
    return thread

def main():
    threads = []
    threads.append(start_radio_worker())
    # Priprav si do budúcnosti ďalšie rádiá: append(start_radio_worker(pre relevant adapter))
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
