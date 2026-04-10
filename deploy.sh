#!/bin/bash
# 03/30/26
# Gemini revised
# 04/10/26
# Cloude reviewed
#
# Exit immediately if a command exits with a non-zero status
set -e

echo "--- Starting Deployment ---"

# 1. Fetch the latest metadata
git fetch origin main

# 2. Hard Reset: This is the 'Secret Sauce'
# It forces the server to match GitHub exactly, throwing away
# any accidental local edits or merge conflicts.
echo "Syncing code with GitHub (Force Reset)..."
git reset --hard origin/main

# 3. Backend: create venv if needed, install dependencies
echo "Updating Python dependencies..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
venv/bin/pip install -q -r requirements.txt

# 4. Build SEC cache if not present (first deploy only, ~20 min)
if [ ! -f "sec_cache.json" ]; then
    echo "Building SEC cache (first-time only, ~20 min)..."
    venv/bin/python3 bsset.py --build-cache
fi

# 5. Restart app service (uncomment once systemd service is configured)
# echo "Restarting BSSET service..."
# sudo systemctl restart bsset
echo "--------------------------------------"
echo "✅ Deployment Successful at $(date)"
echo "--------------------------------------"
