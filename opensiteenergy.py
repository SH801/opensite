import logging
from opensite.app.opensite import OpenSiteApplication

opensiteenergy = OpenSiteApplication(logging.INFO)
app = opensiteenergy.app

def main():
    # Run OpenSite application
    opensiteenergy.run()

if __name__ == "__main__":
    main()