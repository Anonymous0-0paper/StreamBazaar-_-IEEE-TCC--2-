#!/bin/bash
# Setup Kaggle credentials for StreamBazaar dataset downloads

set -e

KAGGLE_DIR="$HOME/.kaggle"
TEMPLATE_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.kaggle.json.template"

if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "Error: Template file not found at $TEMPLATE_FILE"
    exit 1
fi

mkdir -p "$KAGGLE_DIR"

if [ -f "$KAGGLE_DIR/kaggle.json" ]; then
    echo "Kaggle credentials file already exists at $KAGGLE_DIR/kaggle.json"
    echo "To update, remove it first: rm $KAGGLE_DIR/kaggle.json"
    exit 0
fi

echo "Setting up Kaggle credentials..."
echo ""
echo "Instructions:"
echo "1. Go to https://www.kaggle.com/settings/account"
echo "2. Scroll to 'API' section and click 'Create New API Token'"
echo "3. This downloads kaggle.json to your Downloads folder"
echo ""
read -p "Press Enter after downloading kaggle.json from Kaggle..."

DOWNLOAD_FILE="$HOME/Downloads/kaggle.json"
if [ ! -f "$DOWNLOAD_FILE" ]; then
    echo "Error: kaggle.json not found at $DOWNLOAD_FILE"
    echo "Please download it manually from https://www.kaggle.com/settings/account"
    exit 1
fi

cp "$DOWNLOAD_FILE" "$KAGGLE_DIR/kaggle.json"
chmod 600 "$KAGGLE_DIR/kaggle.json"

echo "✓ Kaggle credentials installed at $KAGGLE_DIR/kaggle.json"
echo ""
echo "Verify with:"
echo "  python3 scripts/prepare_datasets.py --json"
