# PDF Export Guide

This guide explains how to convert Markdown documentation (with Mermaid diagrams) into professional PDFs.

## Prerequisites

### Install Pandoc and LaTeX

On Arch Linux:
```bash
# Install pandoc-cli (if not already installed)
sudo pacman -S pandoc-cli

# Install LaTeX - choose one:
# Minimal (faster, smaller):
sudo pacman -S texlive-core

# Full (recommended for best results):
sudo pacman -S texlive-basic texlive-latex texlive-latexextra
```

On Ubuntu/Debian:
```bash
sudo apt install pandoc texlive-xetex texlive-latex-extra
```

On macOS:
```bash
brew install pandoc
brew install --cask mactex
```

### Install Mermaid CLI

Already installed in this project at `~/.local/bin/mmdc`:
```bash
npm install -g @mermaid-js/mermaid-cli --prefix ~/.local
```

## Usage

### Basic Usage

Convert any markdown file to PDF:
```bash
./scripts/md-to-pdf.sh path/to/file.md
```

This will create `path/to/file.pdf`

### Custom Output Path

Specify a custom output location:
```bash
./scripts/md-to-pdf.sh input.md output/custom-name.pdf
```

### Custom Title

Provide a custom document title:
```bash
./scripts/md-to-pdf.sh input.md output.pdf "My Custom Title"
```

## Examples

### Convert the Requisition Application Progress Model

```bash
./scripts/md-to-pdf.sh \
  scripts/metabase/diagrams/requisition-application-progress-model.md \
  docs/exports/requisition-application-progress.pdf \
  "Requisition Application Progress Data Model"
```

### Convert All Diagrams

```bash
for file in scripts/metabase/diagrams/*.md; do
  ./scripts/md-to-pdf.sh "$file"
done
```

## Features

- **Professional LaTeX template** with clean typography
- **Automatic Mermaid diagram rendering** to high-quality images
- **Customizable styling** via `docs/templates/professional.latex`
- **Header/footer** with document title and page numbers
- **Hyperlinked** table of contents and URLs

## Template Customization

Edit `docs/templates/professional.latex` to customize:
- Page margins and layout
- Font sizes and families
- Colors and styling
- Headers and footers
- Title page formatting

## Troubleshooting

### "pandoc: command not found"
Install Pandoc (see Prerequisites above)

### "mmdc: command not found"
Ensure `~/.local/bin` is in your PATH:
```bash
export PATH="$HOME/.local/bin:$PATH"
```

### Mermaid diagrams not rendering
Check that Mermaid CLI is working:
```bash
mmdc --version
```

### LaTeX errors
Ensure you have all required LaTeX packages:
```bash
sudo pacman -S texlive-most  # Arch - installs comprehensive set
```

## Output Quality

The generated PDFs feature:
- 300+ DPI vector graphics for Mermaid diagrams
- Professional typography with proper ligatures and kerning
- Clean, readable layout suitable for presentations or documentation
- Clickable links and cross-references
