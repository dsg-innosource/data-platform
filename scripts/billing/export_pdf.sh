#!/bin/bash

# Simple PDF export script for billing summaries
# Uses pandoc to create a styled HTML that can be printed to PDF from browser

if [ $# -eq 0 ]; then
    echo "Usage: $0 <markdown-file>"
    echo "Example: $0 output/monthly_billing/reports/billing_summary_10_2025.md"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_PDF="${INPUT_FILE%.md}.pdf"
TEMP_HTML="/tmp/billing_summary_temp.html"

# Check if pandoc is installed
if ! command -v pandoc &> /dev/null; then
    echo "Error: pandoc is not installed"
    echo "Install with: brew install pandoc"
    exit 1
fi

# Convert markdown to HTML with styling
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
echo "To create PDF:"
echo "1. Open the HTML file in your browser:"
echo "   open $TEMP_HTML"
echo ""
echo "2. Print to PDF (⌘+P) and save as:"
echo "   $OUTPUT_PDF"
echo ""
echo "Or use this command to open directly:"
echo "   open $TEMP_HTML"
