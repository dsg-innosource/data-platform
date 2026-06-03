/**
 * InnoSource HTML → PDF Converter
 * Uses Playwright Chromium headless — no system Chromium required.
 *
 * Usage:
 *   node convert.js <input.html> <output.pdf> [scale] [format]
 *
 * Arguments:
 *   input.html  — absolute path to source HTML file
 *   output.pdf  — absolute path for output PDF
 *   scale       — optional, default 0.82 (for 960px InnoSource reports; tightened from 0.85 to fit single-page layouts)
 *   format      — optional, default 'Letter' (Letter | A4 | Legal)
 *
 * Examples:
 *   node convert.js /tmp/dagster-reports/recruiter-activity/activity-2026-04-27.html \
 *                   /tmp/dagster-reports/recruiter-activity/activity-2026-04-27.pdf
 *
 *   node convert.js /path/to/report.html /path/to/report.pdf 0.68 A4
 *
 * Scale reference (816px = Letter width at 96dpi):
 *   960px report  → 0.82   (all standard InnoSource reports)
 *   1200px report → 0.68
 *   800px report  → 1.00
 */

const { chromium } = require('playwright');
const path = require('path');

const INPUT  = process.argv[2];
const OUTPUT = process.argv[3];
const SCALE  = parseFloat(process.argv[4]) || 0.82;
const FORMAT = process.argv[5] || 'Letter';

if (!INPUT || !OUTPUT) {
  console.error('Usage: node convert.js <input.html> <output.pdf> [scale] [format]');
  process.exit(1);
}

// Print CSS injected at render time — handles page breaks and avoids mid-block splits.
// The .pb selector matches InnoSource report section dividers.
// NOTE: @page { margin } is intentionally omitted here — page margins are controlled
// exclusively via pdfOptions.margin so that single-page and multi-page modes can set
// different values without the CSS overriding Playwright's PDF-level margin option.
const PRINT_CSS = `
@media print {
  body  { margin: 0; }

  .pb {
    break-before: page !important;
    page-break-before: always !important;
    border-top: none !important;
    margin-top: 0 !important;
    padding-top: 44px !important;
  }

  .kpi-row, .activity-cards, .cc, .dt, table, figure, img,
  .client-block {
    break-inside: avoid !important;
    page-break-inside: avoid !important;
  }

  /* Universal multi-page helpers — safe defaults that are no-ops for
     single-page reports. Tables that span multiple pages automatically
     get their column headers repeated; section headings never end up
     orphaned at the bottom of a page. Reports that need finer control
     (e.g. position-status's per-client tbody grouping) override via
     higher-specificity class selectors in their own CSS. */
  thead { display: table-header-group; }

  .section-hdr, h4.block-head {
    break-after: avoid;
    page-break-after: avoid;
  }
}
`;

(async () => {
  console.log(`Converting: ${INPUT}`);
  console.log(`Output:     ${OUTPUT}`);
  console.log(`Scale:      ${SCALE} (format: ${FORMAT})`);

  const browser = await chromium.launch();
  const page    = await browser.newPage();
  await page.setViewportSize({ width: 1280, height: 900 });

  // networkidle waits for web fonts (Google Fonts) and CDN scripts (Chart.js)
  await page.goto('file://' + path.resolve(INPUT), {
    waitUntil: 'networkidle',
    timeout: 30000
  });

  // Extra wait for JS-rendered content (charts, dynamic tables)
  await page.waitForTimeout(2000);

  await page.addStyleTag({ content: PRINT_CSS });

  // Determine page sizing strategy:
  //   - If the HTML has .pb page-break divs, use fixed FORMAT (Letter/A4/etc.) so those
  //     breaks drive pagination at the correct paper size.
  //   - Otherwise, measure the actual rendered height and use a single PDF page that is
  //     exactly as tall as the content, ensuring the PDF always matches the HTML layout
  //     regardless of how much data is on the page.
  const hasPB = await page.evaluate(() => document.querySelector('.pb') !== null);

  let pdfOptions;
  if (hasPB) {
    // Multi-page: let explicit .pb breaks control pagination at the fixed paper size.
    // Use 0.25in top/bottom margins so content that lands at the top of a continuation
    // page has breathing room and won't be clipped by the printer's physical margin.
    // Left/right stay at 0 — the .page div's own padding handles horizontal whitespace.
    console.log(`Page mode:  multi-page (${FORMAT}, .pb breaks detected)`);
    pdfOptions = {
      path:            path.resolve(OUTPUT),
      format:          FORMAT,
      printBackground: true,
      scale:           SCALE,
      margin:          { top: '0.25in', bottom: '0.25in', left: 0, right: 0 }
    };
  } else {
    // Single-page: measure content height and produce a PDF page that fits it exactly.
    // scrollHeight is in CSS px (96 dpi); multiply by SCALE to get the rendered size,
    // then divide by 96 to convert to inches.
    const scrollHeight = await page.evaluate(() => {
      const el = document.querySelector('.page');
      return el ? el.scrollHeight : document.body.scrollHeight;
    });
    const pdfWidthIn  = 8.5;                                      // Letter width always
    const pdfHeightIn = ((scrollHeight * SCALE) / 96) + 0.05;    // +0.05in safety margin
    console.log(`Page mode:  single-page (content ${scrollHeight}px → ${pdfHeightIn.toFixed(3)}in tall)`);
    pdfOptions = {
      path:            path.resolve(OUTPUT),
      width:           `${pdfWidthIn}in`,
      height:          `${pdfHeightIn.toFixed(4)}in`,
      printBackground: true,
      scale:           SCALE,
      margin:          { top: 0, bottom: 0, left: 0, right: 0 }
    };
  }

  await page.pdf(pdfOptions);

  await browser.close();
  console.log('Done →', OUTPUT);
})();
