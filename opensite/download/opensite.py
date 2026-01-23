from opensite.constants import OpenSiteConstants
from opensite.download.base import DownloadBase
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteDownloader(DownloadBase):
    def __init__(self):
        super().__init__()
        self.log = OpenSiteLogger("OpenSiteDownloader")
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER
