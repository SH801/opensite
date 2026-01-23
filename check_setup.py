import sys
import os

def check_structure():
    print(f"Current Working Directory: {os.getcwd()}")
    print(f"Python Path: {sys.path[0]}\n")

    required_files = [
        'postgis/__init__.py',
        'postgis/base.py',
        'postgis/opensite.py',
        'tree/__init__.py',
        'tree/base.py',
        'tree/opensite.py'
    ]

    for f in required_files:
        status = "✅ Found" if os.path.exists(f) else "❌ MISSING"
        print(f"{status}: {f}")

    print("\nAttempting imports...")
    try:
        from postgis.opensite import OpenSitePostGIS
        print("✅ Import postgis.opensite successful!")
    except Exception as e:
        print(f"❌ Import postgis.opensite failed: {e}")

    try:
        from tree.opensite import OpenSiteTree
        print("✅ Import tree.opensite successful!")
    except Exception as e:
        print(f"❌ Import tree.opensite failed: {e}")

if __name__ == "__main__":
    check_structure()