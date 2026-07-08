#!/bin/bash

# Exit instantly if any command fails
set -e

echo "=============================================="
echo "🚀 Starting Data Warehouse Documentation Build"
echo "=============================================="

# 1. Run tbls to generate fresh documentation inside the /dwh subdirectory
echo "➔ Running tbls database schema inspection..."
tbls doc --rm-dist

# Define working paths relative to the /docs folder
DWH_DIR="dwh"
SUMMARY_FILE="$DWH_DIR/SUMMARY.md"

echo "➔ Generating dynamic Table of Contents (SUMMARY.md)..."

# 2. Initialize the structured SUMMARY.md file
echo "# Summary" > "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"
echo "[Database Overview](README.md)" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

# 3. Scan and append Fact Tables dynamically
echo "## Fact Tables" >> "$SUMMARY_FILE"
# Check if any Fact tables exist before looping to prevent literal string issues
if ls "$DWH_DIR"/Fact_*.md >/dev/null 2>&1; then
    for file in "$DWH_DIR"/Fact_*.md; do
        filename=$(basename "$file")
        title="${filename%.md}"
        echo "* [$title]($filename)" >> "$SUMMARY_FILE"
    done
else
    echo "* *No Fact Tables Found*" >> "$SUMMARY_FILE"
fi

echo "" >> "$SUMMARY_FILE"

# 4. Scan and append Dimension Tables dynamically
echo "## Dimension Tables" >> "$SUMMARY_FILE"
if ls "$DWH_DIR"/Dim_*.md >/dev/null 2>&1; then
    for file in "$DWH_DIR"/Dim_*.md; do
        filename=$(basename "$file")
        title="${filename%.md}"
        echo "* [$title]($filename)" >> "$SUMMARY_FILE"
    done
else
    echo "* *No Dimension Tables Found*" >> "$SUMMARY_FILE"
fi

echo "✓ SUMMARY.md generated successfully."

# 5. Execute mdbook compilation
echo "➔ Compiling documentation with mdbook..."
mdbook build

mv book dwh/.

echo "=============================================="
echo "🎉 SUCCESS: Documentation compiled inside docs/dwh/book/"
echo "=============================================="
