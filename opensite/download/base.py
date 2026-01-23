import os
import requests
from pathlib import Path
from opensite.logging.base import LoggingBase

class DownloadBase:
    def __init__(self):
        self.log = LoggingBase("Download-Base")
        self.base_path = ""

    def get(self, url: str, filename: str = None, subfolder: str = "", force: bool = False):
        """
        Downloads a file safely using a .tmp shadow file.
        Uses the URL's basename if filename is not provided.
        """

        self.log.info(f"Downloading: {url}")

        if not filename:
            filename = os.path.basename(url)
            # Basic cleanup in case of URL parameters like ?v=1.0
            filename = filename.split('?')[0]

        destination = self.base_path / subfolder / filename
        
        if destination.exists() and not force:
            self.log.info(f"File exists, skipping: {filename}")
            return destination

        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = destination.with_suffix(destination.suffix + '.tmp')

        try:
            self.log.info(f"Downloading: {url}")
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(tmp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 32):
                        if chunk:
                            f.write(chunk)
            
            os.replace(tmp_path, destination)
            return destination

        except Exception as e:
            self.log.error(f"Download failed: {e}")
            if tmp_path.exists():
                tmp_path.unlink()
            return None