import logging
from pathlib import Path
from opensite.logging.base import LoggingBase
class ProcessBase:
    def __init__(self, node, log_level=logging.INFO):
        self.node = node
        self.log = LoggingBase("ProcessBase", log_level)
        self.base_path = ""

    def run(self):
        """Main entry point for the process."""
        raise NotImplementedError("Subclasses must implement run()")

    def ensure_output_dir(self, file_path):
        """Utility to make sure the destination exists."""
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    def get_full_path(self, path_str: str) -> Path:
        """Helper to resolve paths against the base_path."""
        path = Path(path_str)
        if not path.is_absolute() and self.base_path:
            return (Path(self.base_path) / path).resolve()
        return path.resolve()