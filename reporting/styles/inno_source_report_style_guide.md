# InnoSource Data Governance — Report Style Guide

A reference for building consistent, professional HTML reports in the InnoSource DG (Data Governance) visual style. Use this guide when prompting Claude or building reports manually.

---

## 01 · Design Foundations

### Typography

Three fonts — each with a specific role. Never mix roles.

| Role | Font | Weight | Usage |
|------|------|--------|-------|
| Logo / Display | Syne | 800 | "InnoSource Data Governance" wordmark only |
| Cover Title (sans) | Inter | 800 | Main subject line of cover title |
| Cover Title (serif) | DM Serif Display | italic | Report type line of cover title (teal colored) |
| Body / UI | Inter | 300–700 | All body text, labels, callouts, tables, descriptions |
| Numbers / Code | JetBrains Mono | 400, 600 | Numeric values in tables, code snippets |

**Google Fonts import — include this in every report's `<head>`:**

```html
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;600&family=DM+Serif+Display:ital@0;1&display=swap" rel="stylesheet">
```

Also include Chart.js if the report contains charts:

```html
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
```

---

### Color Palette

All colors are defined as CSS custom properties. **Never hardcode hex values in components** — always use the variable name.

```css
:root {
  --ink:      #0F172A;  /* Primary text, headings, strong borders */
  --teal:     #0E7490;  /* Primary brand — logo, accents, cover serif title */
  --teal-lt:  #22D3EE;  /* Light teal accent */
  --green:    #059669;  /* Positive deltas, improvements (.pos) */
  --red:      #DC2626;  /* Negative deltas, declines (.neg) */
  --amber:    #B45309;  /* Warnings, watch items (.warn) */
  --amber-lt: #FCD34D;  /* Reference/target lines on charts */
  --slate:    #64748B;  /* Muted text, labels, subtitles (.neu) */
  --rule:     #E2E8F0;  /* All borders, dividers, table row lines */
  --page-bg:  #F8FAFC;  /* Outer page background */
  --white:    #FFFFFF;  /* Card and content area background */
}
```

| Variable | Hex | Used For |
|----------|-----|----------|
| `--ink` | #0F172A | All body text, headings, table headers, emphasis borders |
| `--teal` | #0E7490 | Logo, section rules, teal KPI tint, links, cover serif line |
| `--green` | #059669 | `.pos` — positive deltas, improvement indicators |
| `--red` | #DC2626 | `.neg` — negative deltas, decline indicators |
| `--amber` | #B45309 | `.warn` — caution callouts, watch items |
| `--slate` | #64748B | `.neu` — neutral/muted text, labels, subtitles, footer |
| `--rule` | #E2E8F0 | All borders, dividers, table row lines, card strokes |

---

## 02 · Page Structure

### Overall Page Setup

```css
body {
  background: var(--page-bg);
  font-family: 'Inter', sans-serif;
  color: var(--ink);
  font-size: 13px;
  line-height: 1.6;
}
.page {
  width: 960px;
  margin: 0 auto;
  background: var(--white);
  padding: 52px 64px;
}
@media print {
  body { background: #fff; }
  .page { width: 960px; max-width: 960px; margin: 0 auto; padding: 44px 54px; }
  .pb { page-break-before: always; }
}
```

### Cover Page Anatomy

Every report cover uses these five zones in order:

1. **Cover Top** — "InnoSource Data Governance" logo left (Syne 800, teal, uppercase) · report date right (Inter, slate, uppercase). Separated by a `3px solid var(--ink)` bottom border.

2. **Eyebrow Tag** — Client · Report Type · Date. Example: `"AEP OSO · Sidekick AI · March 2026"`. Inter 600, 11px, teal, `letter-spacing: .16em`, uppercase.

3. **Title Block** — Two lines stacked:
   - Line 1: **Inter 800**, ~50px, dark ink, `letter-spacing: -0.03em` — the subject/client name
   - Line 2: *DM Serif Display italic*, ~50px, teal — the report type (e.g. "Impact Report")

4. **Teal Rule** — `width: 48px; height: 3px; background: var(--teal); margin: 20px 0 22px` — visual separator between title and description.

5. **Description** — 1–2 sentences on scope. Max 520px wide. Inter 400, 15px, slate, `line-height: 1.7`.

6. **Badge Strip** *(optional)* — 2–4 period/category badges. Omit if the report is not comparative or time-based.

7. **Cover Footer** — Data source/scope note left · "Confidential — InnoSource Internal Use" right. 1px `--rule` top border, Inter 10.5px, slate.

### Section / Content Page Structure

Each content page follows this pattern:

1. **Page Break div** — `class="pb"` applies `page-break-before: always` and a top rule.
2. **Section Header** — Teal 4px left-border · section number (e.g. "01") in teal uppercase · bold title · slate subtitle.
3. **Content** — KPI rows, chart cards, callout boxes, tables in order of importance.
4. **Page Footer** — `.rft` class · "InnoSource Analytics · [Report Name]" left · "Page N" right. Always the last element on each page.

---

## 03 · Component Library

### KPI Stat Row

One continuous ruled panel with 4 cells. Use `.dark` (teal tint) and `.hi` (green tint) to emphasize 1–2 key numbers per row.

```html
<div class="kpi-row">
  <div class="kpi dark">
    <div class="kl">Metric Label</div>
    <div class="kv">84.2<span style="font-size:14px;font-weight:400;color:var(--slate)">%</span></div>
    <div class="ks pos">↑ improved vs prior period</div>
  </div>
  <div class="kpi hi">
    <div class="kl">Positive Result</div>
    <div class="kv">+9.1%</div>
    <div class="ks pos">Volume increase</div>
  </div>
  <div class="kpi">
    <div class="kl">Neutral Metric</div>
    <div class="kv">317s</div>
    <div class="ks neu">Within normal range</div>
  </div>
  <div class="kpi">
    <div class="kl">Watch Metric</div>
    <div class="kv">−0.8pp</div>
    <div class="ks neg">↓ declined</div>
  </div>
</div>
```

| Class | Background | Use When |
|-------|------------|----------|
| `.kpi` | White | Default, neutral metric |
| `.kpi.dark` | Soft teal `#F0FDFA` | Primary / most important metric on the row |
| `.kpi.hi` | Soft green `#F0FDF4` | A clearly positive result worth calling out |
| `.kpi.kwarn` | Soft amber `#FFFBEB` | A metric that needs attention or is below target |
| `.kpi.kdng` | Soft red `#FEF2F2` | A metric indicating a problem or critical flag |

> ⚠ **Class name collision — do not use `.kpi.warn` or `.kpi.dng`.**
> The global callout classes `.warn` and `.dng` apply `border-left: 4px solid` and `border-radius: 0 6px 6px 0` to **any** element carrying those class names — including KPI cells. This creates a visible left-border gap inside the card. Always use the prefixed variants `.kwarn` and `.kdng` on KPI cells, which are background-only and carry no border rules.

### Chart Card

Wraps every chart or visualization. Always include a title and subtitle.

```html
<div class="cc">
  <div class="cc-title">Chart Title — What is being shown</div>
  <div class="cc-sub">Time period · units · any important context</div>
  <div class="ch"><canvas id="myChart"></canvas></div>
</div>
```

Canvas height classes: `.ch` (300px) · `.ch-sm` (220px) · `.ch-lg` (320px)

Two-column chart layout: wrap two `.cc` divs in `<div class="two">`.

### Callout Boxes

Three variants — use the one that matches the message tone. Always `<strong>` the key phrase.

```html
<!-- Green: positive findings, key takeaways, bottom-line summaries -->
<div class="callout"><strong>Key finding:</strong> Your insight here.</div>

<!-- Blue: contextual info, methodology notes, neutral explanations -->
<div class="info"><strong>Note:</strong> Your context here.</div>

<!-- Amber: watch items, declining metrics, areas needing follow-up -->
<div class="warn"><strong>Watch item:</strong> Your concern here.</div>
```

**Order rule:** Lead with `.callout` (positive), follow with `.info` (context), close with `.warn` (concerns). Never open a section with bad news.

### Data Table

```html
<table class="dt">
  <thead>
    <tr>
      <th>Row Label</th>
      <th class="preh">Period A</th>   <!-- dark slate header -->
      <th class="posth">Period B</th>  <!-- teal header -->
      <th class="chgth">Change</th>    <!-- dark teal header -->
      <th>Assessment</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Metric Name</td>
      <td class="mono flt">100.0</td>
      <td class="mono imp">108.3</td>
      <td class="mono imp">+8.3%</td>
      <td><span class="pill pg">Improved</span></td>
    </tr>
  </tbody>
</table>
```

Table header color classes: `.preh` (dark slate `#334155`) · `.posth` (teal) · `.chgth` (dark teal `#0F766E`)

### Status Pills

Use inline in table cells or callouts to communicate status at a glance.

```html
<span class="pill pg">Improved</span>    <!-- green -->
<span class="pill pt">On Track</span>    <!-- teal -->
<span class="pill py">Watch Item</span>  <!-- amber -->
<span class="pill ps">Stable</span>      <!-- slate -->
<span class="pill pr">Below Target</span> <!-- red -->
```

| Class | Background | Text | Use For |
|-------|------------|------|---------|
| `.pill.pg` | `#D1FAE5` | `#065F46` | Positive results, improvements, targets met |
| `.pill.pt` | `#CCFBF1` | `#134E4A` | On track, within expectations |
| `.pill.py` | `#FEF3C7` | `#92400E` | Caution, slight decline, monitor closely |
| `.pill.ps` | `#F1F5F9` | `#475569` | Neutral, no change, stable |
| `.pill.pr` | `#FEE2E2` | `#991B1B` | Negative results, targets missed |

---

## 04 · Full HTML Templates

### Boilerplate Shell

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>[Client] — [Report Name]</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;600&family=DM+Serif+Display:ital@0;1&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
/* — paste full CSS block here (see CSS Reference section below) — */
</style>
</head>
<body>
<div class="page">

  <!-- COVER -->
  <!-- PAGE 2+ (add .pb divs) -->

</div>
<script>
/* — Chart.js code here — */
</script>
</body>
</html>
```

### Cover Page Template

```html
<div class="cover">
  <div class="cover-top">
    <div class="logo-name">InnoSource Data Governance</div>
    <div class="cover-date">[Month Year]</div>
  </div>

  <div class="cover-hero">
    <div class="cover-tag">[Client] · [Report Type] · [Date]</div>
    <div class="cover-h1">
      <span class="h1-sans">[Subject / Main Title]</span>
      <span class="h1-serif">[Report Type — e.g. "Impact Report"]</span>
    </div>
    <div class="cover-rule"></div>
    <p class="cover-desc">[1–2 sentence description of scope and data period.]</p>
  </div>

  <div class="cover-footer">
    <span>[Data source / scope note]</span>
    <span>Confidential — InnoSource Internal Use</span>
  </div>
</div>
```

### Section Page Template

```html
<div class="pb">
  <div class="section-hdr">
    <div class="section-num">01</div>
    <div class="section-title">[Section Title]</div>
    <div class="section-sub">[One-line description of this page's content]</div>
  </div>

  <!-- KPI Row -->
  <div class="kpi-row">
    <div class="kpi dark">
      <div class="kl">[Label]</div>
      <div class="kv">[Value]</div>
      <div class="ks pos">[Context]</div>
    </div>
    <div class="kpi"><div class="kl">[Label]</div><div class="kv">[Value]</div><div class="ks neu">[Context]</div></div>
    <div class="kpi"><div class="kl">[Label]</div><div class="kv">[Value]</div><div class="ks neu">[Context]</div></div>
    <div class="kpi"><div class="kl">[Label]</div><div class="kv">[Value]</div><div class="ks neg">[Context]</div></div>
  </div>

  <!-- Callout -->
  <div class="info"><strong>Bottom Line:</strong> [Key takeaway sentence here.]</div>

  <!-- Chart Card -->
  <div class="cc">
    <div class="cc-title">[Chart Title]</div>
    <div class="cc-sub">[Period · Units · Context]</div>
    <div class="ch"><canvas id="chartId"></canvas></div>
  </div>

  <!-- Page Footer -->
  <div class="rft">
    <span>InnoSource Data Governance · [Report Name]</span>
    <span>Page N</span>
  </div>
</div>
```

---

## 05 · Chart Standards

### Global Defaults

Set these once at the top of the `<script>` block — they apply to all charts.

```javascript
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.color       = '#64748B';  // --slate
Chart.defaults.borderColor = '#E2E8F0';  // --rule
Chart.defaults.font.size   = 11;

// Reusable color constants
const TEAL  = '#0E7490';
const SLATE = '#94A3B8';
const GREEN = '#059669';
const RED   = '#DC2626';
const AMBER = '#B45309';
const GRID  = '#F1F5F9';
```

### Color Assignment Rules

| Data Role | Color | Hex |
|-----------|-------|-----|
| Primary / current period | Teal | `#0E7490` |
| Prior / baseline period | Slate | `#94A3B8` |
| Reference line / target | Amber (dashed) | `#FCD34D` |
| Positive / above target | Green | `#059669` |
| Negative / below target | Red | `#DC2626` |
| Third period / secondary | Dark teal | `#0F766E` |

### Chart Dos and Don'ts

**Do:**
- Always set `responsive: true, maintainAspectRatio: false` on every chart
- Use `borderRadius: 4` on bar charts
- Set `grid: { color: '#F1F5F9' }` so gridlines are subtle
- Use `tooltip: { mode: 'index', intersect: false }` for multi-series charts

**Don't:**
- Never use the default Chart.js blue — always assign colors explicitly
- Don't put more than 2–3 datasets on one chart without a clear legend
- Don't use pie or donut charts — bar and line are always clearer for this data

---

## 06 · CSS Class Cheatsheet

### Text Color Utilities
| Class | Effect |
|-------|--------|
| `.pos` | Green text — positive delta |
| `.neg` | Red text — negative delta |
| `.neu` | Slate text — neutral / muted |
| `.imp` | Green bold — table cell improvement |
| `.dec` | Red bold — table cell decline |
| `.flt` | Slate — flat / no change |
| `.mono` | JetBrains Mono — numbers in tables |

### Layout
| Class | Effect |
|-------|--------|
| `.page` | 960px centered white content area |
| `.pb` | Page break + top rule (new content page) |
| `.two` | 2-column grid (50/50) |
| `.rft` | Page footer row |

### Components
| Class | Effect |
|-------|--------|
| `.cc` | Chart / content card with border and padding |
| `.cc-title` | Card title — Inter 600, 14px |
| `.cc-sub` | Card subtitle — Inter 400, 11px, slate |
| `.ch` | Canvas height 300px |
| `.ch-sm` | Canvas height 220px |
| `.ch-lg` | Canvas height 320px |
| `.callout` | Green callout box |
| `.info` | Teal/blue callout box |
| `.warn` | Amber callout box — **use on standalone callout divs only** |
| `.dt` | Styled data table |
| `.kpi-row` | 4-cell KPI stat strip |
| `.kpi` | Individual KPI cell (white) |
| `.kpi.dark` | KPI cell — teal tint |
| `.kpi.hi` | KPI cell — green tint |
| `.kpi.kwarn` | KPI cell — amber tint (**use `kwarn`, not `warn`**) |
| `.kpi.kdng` | KPI cell — red tint (**use `kdng`, not `dng`**) |
| `.kl` | KPI label (uppercase, 10px) |
| `.kv` | KPI value (Inter 800, 32px) |
| `.ks` | KPI subtext (11px) |

---

## 06b · Known CSS Class Collision Patterns

These collisions have been encountered in production. Follow the rules below to avoid them.

### `.warn` / `.dng` on non-callout elements

The `.warn` callout rule applies `border-left: 4px solid var(--amber)` and `border-radius: 0 6px 6px 0` to **any element** carrying the `warn` class — not just standalone callout boxes. The same applies to any other semantic color class that has border rules baked in.

**Affected patterns:**

| Incorrect | Problem | Correct |
|-----------|---------|---------|
| `<div class="kpi warn">` | `.warn` callout border-left creates a gap inside the KPI cell | `<div class="kpi kwarn">` |
| `<div class="kpi dng">` | Same issue with a red variant | `<div class="kpi kdng">` |
| `<div class="tl-dot warn">` | `.warn` callout styles applied to the tiny timeline dot, inflating its size | `<div class="tl-dot amber">` |

**Rule:** `.warn`, `.callout`, `.info`, `.alert` are **callout-box-only** classes. For any other element that needs amber/red/green/teal tinting, use a purpose-specific modifier class (`kwarn`, `kdng`, `amber`, etc.) that only sets background and/or color — no border or border-radius rules.

---

## 07 · Writing Style

- **Lead with the insight, not the data.** Say "Handle time fell 10% after launch" — not "The data shows a 10% reduction."
- **Be specific with numbers.** Always show before and after: `317s → 286s (−10%)`.
- **Frame positively for client delivery.** Lead with improvements; use `.warn` boxes for concerns at the end.
- **Keep callouts concise.** 2–3 sentences max. Use a table or list if you need more detail.
- **Section callout order:** Green (positive finding) → Blue (context/methodology) → Amber (watch items). Never open a section with bad news.

---

## 08 · Prompting Claude to Use This Style

When asking Claude to build a new report, include one of the following:

> *"Build an HTML report in the InnoSource Analytics report style."*

> *"Use the same design as the AEP OSO Sidekick Impact Report — Inter/DM Serif Display/JetBrains Mono fonts, teal `#0E7490` brand color, white page on `#F8FAFC` background, KPI stat strips, Chart.js charts, and the cover title style with Inter 800 sans + DM Serif italic teal."*

If this guide is attached as a project artifact, Claude will reference it automatically for any report request within that project.

---

*InnoSource Data Governance · Internal Reference · March 2026*
