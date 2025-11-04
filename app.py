from adapters.radio_rock import RadioRockAdapter
import threading
import time
import os
import json
from writer import upload_json_to_r2

DATA_DIR = "data"
os.makedirs(DATA_DIR + "/songs", exist_ok=True)
os.makedirs(DATA_DIR + "/listeners", exist_ok=True)

def save_and_upload(entry, typ):
    dt = time.strftime("%d-%m-%Y")
    tm = time.strftime("%H-%M-%S")
    radio = "ROCK"
    key = f"bronze/{radio}/{typ}/{dt}/{tm}.json"
    folder = f"{DATA_DIR}/{typ}"
    os.makedirs(f"{folder}/{dt}", exist_ok=True)
    local_file_path = f"{folder}/{dt}/{tm}.json"
    with open(local_file_path, "w", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False, indent=2)
    print(f"Zaznamenané {typ}: {entry}")
    # Upload súboru v intervale si ošetri thread/cron a použij upload_json_to_r2(local_file_path, key)

def song_writer(entry):
    save_and_upload(entry, "song")

def listeners_writer(entry):
    save_and_upload(entry, "listeners")

def main():
    rock_adapter = RadioRockAdapter(
        song_poll_interval=30,
        listeners_interval=30,
        listeners_writer=listeners_writer,
        song_writer=song_writer
    )
    rock_adapter.start()
    # Drž proces nažive
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
