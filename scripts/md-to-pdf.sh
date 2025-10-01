#!/bin/bash
# Convert Markdown with Mermaid diagrams to professional PDF
# Usage: ./md-to-pdf.sh input.md [output.pdf] [title]

set -e

# Check dependencies
if ! command -v pandoc &> /dev/null; then
    echo "Error: pandoc is not installed"
    echo "Install with: sudo pacman -S pandoc texlive-basic texlive-latex texlive-latexextra"
    exit 1
fi

if ! command -v mmdc &> /dev/null; then
    echo "Error: mmdc (mermaid-cli) is not installed"
    echo "It should be in ~/.local/bin/mmdc"
    exit 1
fi

# Arguments
INPUT_FILE="$1"
OUTPUT_FILE="${2:-${INPUT_FILE%.md}.pdf}"
TITLE="${3:-$(basename "${INPUT_FILE%.md}" | sed 's/-/ /g' | sed 's/\b\(.\)/\u\1/g')}"

if [ -z "$INPUT_FILE" ]; then
    echo "Usage: $0 input.md [output.pdf] [title]"
    exit 1
fi

if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' not found"
    exit 1
fi

echo "Converting Markdown to PDF..."
echo "Input:  $INPUT_FILE"
echo "Output: $OUTPUT_FILE"
echo "Title:  $TITLE"

# Create temporary directory
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Create a simple pandoc filter to convert mermaid to images
FILTER_SCRIPT="$TEMP_DIR/mermaid-filter.sh"
cat > "$FILTER_SCRIPT" << 'FILTER_EOF'
#!/bin/bash
# Simple mermaid block processor
TEMP_DIR="$1"
COUNTER=0
IN_MERMAID=false
MERMAID_CONTENT=""

while IFS= read -r line; do
    if [[ "$line" =~ ^\`\`\`mermaid ]]; then
        IN_MERMAID=true
        MERMAID_CONTENT=""
    elif [[ "$IN_MERMAID" == true ]] && [[ "$line" =~ ^\`\`\`$ ]]; then
        # End of mermaid block - generate diagram
        COUNTER=$((COUNTER + 1))
        DIAGRAM_FILE="$TEMP_DIR/diagram_$COUNTER.png"

        echo "$MERMAID_CONTENT" | mmdc -i - -o "$DIAGRAM_FILE" -b transparent -s 3 >/dev/null 2>&1

        if [ -f "$DIAGRAM_FILE" ]; then
            echo ""
            echo "![]($DIAGRAM_FILE){width=90%}"
            echo ""
        else
            echo "\`\`\`mermaid"
            echo "$MERMAID_CONTENT"
            echo "\`\`\`"
        fi

        IN_MERMAID=false
        MERMAID_CONTENT=""
    elif [[ "$IN_MERMAID" == true ]]; then
        MERMAID_CONTENT="$MERMAID_CONTENT$line"$'\n'
    else
        echo "$line"
    fi
done
FILTER_EOF

chmod +x "$FILTER_SCRIPT"

# Process the markdown file
echo "Processing Mermaid diagrams..."
"$FILTER_SCRIPT" "$TEMP_DIR" < "$INPUT_FILE" > "$TEMP_DIR/processed.md"

# Remove the first H1 heading since the template will add the title
sed -i '0,/^# .*/d' "$TEMP_DIR/processed.md"

# Remove leading blank lines
sed -i '/./,$!d' "$TEMP_DIR/processed.md"

# Debug: save processed markdown for inspection
cp "$TEMP_DIR/processed.md" /tmp/last-processed.md 2>/dev/null || true

# Get template path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_PATH="$SCRIPT_DIR/../docs/templates/professional.latex"

# Convert to PDF with pandoc
echo "Generating PDF with Pandoc..."
pandoc "$TEMP_DIR/processed.md" \
    -o "$OUTPUT_FILE" \
    --template="$TEMPLATE_PATH" \
    --pdf-engine=pdflatex \
    --variable title="$TITLE" \
    --variable geometry:margin=1in \
    -V colorlinks=true \
    -V linkcolor=blue \
    -V urlcolor=blue

echo "PDF generated successfully: $OUTPUT_FILE"
