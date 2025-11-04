import threading
import time
import os
import json
from writer import upload_json_to_r2
from adapters.radio_rock import RadioRockWorker
from adapters.radio_funradio import RadioFunradioWorker
from adapters.radio_jazz import start_worker as start_jazz_worker
from adapters.radio_beta import RadioBetaWorker
from adapters.radio_expres import start_worker as start_expres_worker

SONG_INTERVAL = 30
LISTENERS_INTERVAL = 30
UPLOAD_INTERVAL = 600  # 10 minút

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

RADIO_WORKERS = {
    "rock": {
        "worker_class": RadioRockWorker,
        "intervals": (SONG_INTERVAL, LISTENERS_INTERVAL),
        "upload_interval": UPLOAD_INTERVAL,
        "song_cache": [],
        "listeners_cache": [],
        "radio_name": "ROCK",
        "starter": None,
    },
    "funradio": {
        "worker_class": RadioFunradioWorker,
        "intervals": (SONG_INTERVAL, LISTENERS_INTERVAL),
        "upload_interval": UPLOAD_INTERVAL,
        "song_cache": [],
        "listeners_cache": [],
        "radio_name": "FUNRADIO",
        "starter": None,
    },
    "jazz": {
        "worker_class": None,
        "intervals": (SONG_INTERVAL, LISTENERS_INTERVAL),
        "upload_interval": UPLOAD_INTERVAL,
        "song_cache": [],
        "listeners_cache": [],
        "radio_name": "JAZZ",
        "starter": start_jazz_worker
    },
    "beta": {
        "worker_class": RadioBetaWorker,
        "intervals": (SONG_INTERVAL, LISTENERS_INTERVAL),
        "upload_interval": UPLOAD_INTERVAL,
        "song_cache": [],
        "listeners_cache": [],
        "radio_name": "BETA",
        "starter": None,
    },
    "expres": {
        "worker_class": None,
        "intervals": (SONG_INTERVAL, LISTENERS_INTERVAL),
        "upload_interval": UPLOAD_INTERVAL,
        "song_cache": [],
        "listeners_cache": [],
        "radio_name": "EXPRES",
        "starter": start_expres_worker,
    }
}

def save_entries(entries, typ, radio):
    dt = time.strftime("%d-%m-%Y")
    tm = time.strftime("%H-%M-%S")
    folder = f"{DATA_DIR}/{radio}/{typ}/{dt}"
    os.makedirs(folder, exist_ok=True)
    local_file_path = f"{folder}/{tm}.json"
    with open(local_file_path, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
    return local_file_path, dt, tm

def upload_worker(radio_key, radio_dict):
    time.sleep(radio_dict["upload_interval"])
    while True:
        songs_to_upload = radio_dict["song_cache"].copy()
        listeners_to_upload = radio_dict["listeners_cache"].copy()
        radio_dict["song_cache"].clear()
        radio_dict["listeners_cache"].clear()
        radio_name = radio_dict["radio_name"]
        if songs_to_upload:
            local_file, dt, tm = save_entries(songs_to_upload, "song", radio_name)
            r2_key = f"bronze/{radio_name}/song/{dt}/{tm}.json"
            upload_json_to_r2(local_file, r2_key)
        if listeners_to_upload:
            local_file, dt, tm = save_entries(listeners_to_upload, "listeners", radio_name)
            r2_key = f"bronze/{radio_name}/listeners/{dt}/{tm}.json"
            upload_json_to_r2(local_file, r2_key)
        print(f"[{time.strftime('%d.%m.%Y %H:%M:%S')}] [{radio_name} ---] Dáta boli odoslané do Cloudflare R2.")
        time.sleep(radio_dict["upload_interval"])

def start_radio_worker(radio_key, radio_dict):
    if radio_dict["starter"]:
        radio_dict["starter"](radio_dict["intervals"][1], radio_dict["listeners_cache"], radio_dict["song_cache"])
    else:
        worker = radio_dict["worker_class"](
            radio_dict["intervals"][0],
            radio_dict["intervals"][1],
            radio_dict["song_cache"],
            radio_dict["listeners_cache"]
        )
        worker.start()
    threading.Thread(target=upload_worker, args=(radio_key, radio_dict), daemon=True).start()

def main():
    for radio_key, radio_dict in RADIO_WORKERS.items():
        start_radio_worker(radio_key, radio_dict)
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
