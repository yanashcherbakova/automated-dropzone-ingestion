from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import os
from dotenv import load_dotenv

load_dotenv()

INCOMING_DIR = os.getenv("INCOMING_DIR")

def process_file(file_path):
    print("PROCESS:", file_path)

class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = event.src_path
        file_name = os.path.basename(file_path)

        if file_name.startswith(".") or file_name.endswith(".tmp"):
            return
        
        if not file_name.endswith(".csv"):
            return
        

if __name__ == "__main__":
    os.makedirs(INCOMING_DIR, exist_ok=True)

    observer = Observer()
    observer.schedule(FileHandler(), INCOMING_DIR, recursive=False)
    observer.start()
    print("Watching:", os.path.abspath(INCOMING_DIR))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    

