#!/bin/bash

# PDF export script for billing summaries
# Attempts automated PDF generation with pandoc/pdflatex
# Falls back to browser-based PDF creation if LaTeX not available

if [ $# -eq 0 ]; then
    echo "Usage: $0 <markdown-file>"
    echo "Example: $0 output/monthly_billing/reports/billing_summary_10_2025.md"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_PDF="${INPUT_FILE%.md}.pdf"

# Check if pandoc is installed
if ! command -v pandoc &> /dev/null; then
    echo "Error: pandoc is not installed"
    echo "Install with: brew install pandoc"
    exit 1
fi

# Add LaTeX to PATH if it exists
if [ -d "/Library/TeX/texbin" ]; then
    export PATH="/Library/TeX/texbin:$PATH"
fi

# Check if pdflatex is available for automated PDF generation
if command -v pdflatex &> /dev/null; then
    echo "🔄 Generating PDF..."

    # Get the script directory to find the template
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
    TEMPLATE="$PROJECT_ROOT/docs/templates/billing.latex"

    # Extract report period and generated date from markdown if present
    REPORT_PERIOD=$(grep -m1 "Report Period:" "$INPUT_FILE" | sed 's/.*Report Period:\*\* //' || echo "")
    GENERATED=$(grep -m1 "Generated:" "$INPUT_FILE" | sed 's/.*Generated:\*\* //' || echo "")

    # Try XeLaTeX first (handles Unicode better and required for custom fonts)
    if command -v xelatex &> /dev/null && [ -f "$TEMPLATE" ]; then
        pandoc "$INPUT_FILE" -o "$OUTPUT_PDF" \
            --pdf-engine=xelatex \
            --template="$TEMPLATE" \
            --metadata title="Billing Summary Report" \
            --metadata report-period="$REPORT_PERIOD" \
            --metadata generated="$GENERATED" \
            2>&1 | grep -E "(Error|Warning:)" | head -5
    elif command -v xelatex &> /dev/null; then
        # No custom template, use defaults with better settings
        pandoc "$INPUT_FILE" -o "$OUTPUT_PDF" \
            --pdf-engine=xelatex \
            -V geometry:margin=0.75in \
            -V mainfont="Arial" \
            -V fontsize=11pt \
            --metadata title="Billing Summary Report" \
            2>&1 | grep -E "(Error|Warning:)" | head -5
    else
        # Fall back to pdflatex
        pandoc "$INPUT_FILE" -o "$OUTPUT_PDF" \
            --pdf-engine=pdflatex \
            -V geometry:margin=0.75in \
            -V fontsize=11pt \
            --metadata title="Billing Summary Report" \
            2>&1 | grep -E "(Error|Warning:)" | head -5
    fi

    if [ -f "$OUTPUT_PDF" ]; then
        echo "✅ PDF generated successfully: $OUTPUT_PDF"
        exit 0
    else
        echo "❌ PDF generation failed, falling back to browser method..."
    fi
fi

# Fallback: Browser-based PDF creation
echo "⚠️  pdflatex not found - using browser method"
echo ""
echo "To install pdflatex for automated PDF generation:"
echo "  brew install basictex"
echo "  eval \"\$(/usr/libexec/path_helper)\""
echo ""

TEMP_HTML="/tmp/billing_summary_temp.html"

# Convert markdown to styled HTML
pandoc "$INPUT_FILE" -o "$TEMP_HTML" -s \
    --metadata title="Billing Summary Report" \
    --css <(cat <<'EOF'
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    max-width: 900px;
    margin: 40px auto;
    padding: 20px;
    line-height: 1.6;
    color: #333;
}
h1 {
    border-bottom: 3px solid #333;
    padding-bottom: 10px;
}
h2 {
    color: #2c3e50;
    margin-top: 30px;
    border-bottom: 2px solid #ecf0f1;
    padding-bottom: 8px;
}
h3 {
    color: #e74c3c;
}
table {
    width: 100%;
    border-collapse: collapse;
    margin: 20px 0;
}
th {
    background-color: #34495e;
    color: white;
    padding: 12px;
    text-align: left;
}
td {
    padding: 10px;
    border-bottom: 1px solid #ddd;
}
tr:hover {
    background-color: #f5f5f5;
}
strong {
    color: #2c3e50;
}
@media print {
    body {
        margin: 0;
        padding: 15px;
    }
}
EOF
)

echo "✓ HTML generated at: $TEMP_HTML"
echo ""
echo "📄 To create PDF:"
echo "   1. Opening in browser..."
open "$TEMP_HTML"
echo "   2. Press ⌘+P to print"
echo "   3. Save as: $OUTPUT_PDF"
