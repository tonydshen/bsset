#!/bin/bash
# 03/30/26
# Gemini revised
#
set -e  # Exit on any error

# 1. Ensure we are on the master branch
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$BRANCH" != "main" ]; then
    echo "❌ Error: You are on branch '$BRANCH'. Please switch to main."
    exit 1
fi

# 2. Add a 'heartbeat' to the core file to ensure Git sees a change
# This solves the "nothing to commit" issue we encountered.
DATE="$(date +"%m/%d/%Y-%H:%M:%S")"
echo "# Last Update: $DATE" >> update_hist.txt
# echo "# Last Update: $DATE" >> backend/update_hist.txt
# echo "# Last Update: $DATE" >> src/update_hist.txt
# echo "# Last Update: $DATE" >> src/components/update_hist.txt

echo "Staging and committing..."
git add .

# 3. Only commit if there are actually changes to avoid script errors
if git diff-index --quiet HEAD --; then
    echo "nb. No changes detected to commit."
else
    git commit -m "Update BSSET $DATE from $(hostname)"
fi

# 4. Push to GitHub
echo "Pushing to origin main ..."
git push origin main

echo "✅ Update complete for $(hostname) at $DATE"
