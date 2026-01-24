import yaml
import os
import logging
from .base import BaseCLI
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteCLI(BaseCLI):
    def __init__(self, config_path: str = "defaults.yml", log_level=logging.INFO):
        super().__init__(description="OpenSite Project Processor", log_level=log_level)
        self.log = OpenSiteLogger("OpenSiteCLI", log_level)
        self.config_path = config_path
        self.defaults = {}
        self.overrides = {}
        self.sites = []
        # Load and filter immediately
        self._load_and_filter_defaults()
        self._incoporate_cli_switched()

    def add_standard_args(self):
        """Standard arguments used across the application."""

        # Override base function
        super().add_standard_args()
        self.parser.add_argument("sites", nargs="*", help="Site(s) to generate")
        self.parser.add_argument('--preview', action='store_true', help='Generate an interactive graph preview and skip database modifications')

    def _load_and_filter_defaults(self):
        """Loads the file and keeps only int, float, and str variables."""
        if not os.path.exists(self.config_path):
            return

        self.log.debug(f"Loading defaults from {self.config_path}")

        with open(self.config_path, 'r') as f:
            full_data = yaml.safe_load(f) or {}
            
        # Filter for 'simple' types only
        for key, value in full_data.items():
            if isinstance(value, (int, float, str)) and not isinstance(value, bool):
                self.log.debug(f"Adding default value from {self.config_path}: {key}={value}")
                self.defaults[key] = value

    def inject_dynamic_args(self):
        """Adds flags for the filtered simple variables."""
        for key, value in self.defaults.items():
            self.parser.add_argument(
                f"--{key}",
                type=type(value),
                default=None,
                help=f"Override {key} (Default: {value})"
            )

    def get_current_value(self, value):
        """Gets current value of a property"""

        if value in self.overrides: 
            if self.overrides[value] is not None: 
                return self.overrides[value]
        
        if value in self.defaults: 
            if self.defaults[value] is not None:
                return self.defaults[value]

        self.log.error(f"'{value}' does not exist in defaults or overrides")
        return None

    def get_defaults(self):
        """Gets current defaults"""
        return self.defaults

    def get_overrides(self):
        """Gets current overrides"""
        return self.overrides
    
    def get_sites(self):
        """Gets list of sites from CLI"""
        return self.sites

    def get_preview(self):
        """Gets status of --preview CLI switch"""
        return self.preview
    
    def _incoporate_cli_switched(self):
        """Standard execution flow."""
        self.add_standard_args()
        self.inject_dynamic_args()
        self.parse()

        # Boolean for preview
        self.preview = self.args.preview
        
        # Set sites to the list of sites provided in CLI
        self.sites = self.args.sites

        # Capture the final state of the simple variables
        overrides = {}
        for key in self.defaults.keys():
            safe_key = key.replace("-", "_")
            if hasattr(self.args, safe_key):
                overrides[key] = getattr(self.args, safe_key)
        self.overrides = overrides
        
        self.log.debug(f"Command line sites: {self.sites}")
        self.log.debug(f"Defaults: {self.defaults}")
        self.log.debug(f"Overrides: {self.overrides}")
        
