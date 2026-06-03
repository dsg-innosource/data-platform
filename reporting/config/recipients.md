# Recipient Lists

Defines who receives each Cowork-delivered report or alert.
**Update this file when distribution lists change — never hardcode email addresses inside prompt files.**

Each prompt reads its named section and joins all addresses with `;` for the Power Automate `to` field.

---

## How to add or remove recipients

1. Find the section for the report/alert you want to change
2. Add or remove rows — one recipient per row
3. Save and commit — the change takes effect on the next run
4. No changes to prompt files, flows, or scheduled tasks required

---

## recruiter-activity-report

Receives the Daily Recruiter Activity Report (HTML → PDF) each morning.

| Name | Email |
|---|---|
| Sean Beal | sbeal@innosource.com |
| Whitney Diem | wdiem@innosource.com |
| Julia Stanton | jstanton@innosource.com |
| PH | ph@innosource.com |
| Chad Delligatti | cdell@innosource.com |
| Jourdan Powers | jpowers@innosource.com |
| Steve Smith | ssmith@innosource.com |
| Becky Henke | bhenke@innosource.com |
| Megan Wonderly | mwonderly@innosource.com |
| Kyle Culver | kculver@innosource.com |
| Jerad Kildoo | JKildoo@innosource.com |
| Mason Dail | mdail@innosource.com |
| Rebecca Heslep | RHeslep@innosource.com |
| Vince Dudinec | vdudinec@innosource.com |
| Abigail Ryan | aryan@innosource.com |
| Ronda Adams | radams@innosource.com |
| Lyndsay Spencer | lspencer@innosource.com |
| Sarah Kozubek | skozubek@innosource.com |
| Graham Eggenschwiler | geggenschwiler@innosource.com |

**To field (semicolon-joined):** `sbeal@innosource.com;wdiem@innosource.com;jstanton@innosource.com;ph@innosource.com;cdell@innosource.com;jpowers@innosource.com;ssmith@innosource.com;bhenke@innosource.com;mwonderly@innosource.com;kculver@innosource.com;JKildoo@innosource.com;mdail@innosource.com;RHeslep@innosource.com;vdudinec@innosource.com;aryan@innosource.com;radams@innosource.com;lspencer@innosource.com;skozubek@innosource.com;geggenschwiler@innosource.com`

---

## unmapped-departments-alert

Receives the daily unmapped departments data quality alert when issues are found.
Also receives connection failure alerts.

| Name | Email |
|---|---|
| Sean Beal | sbeal@innosource.com |
| Jourdan Powers | jpowers@innosource.com |
| Julia Stanton | jstanton@innosource.com |

**To field (semicolon-joined):** `sbeal@innosource.com;jpowers@innosource.com;jstanton@innosource.com`

---

## indeed-job-intelligence-report

Receives the bi-weekly Indeed Job Intelligence Report (HTML → PDF) every other Monday.

| Name | Email |
|---|---|
| Sean Beal | sbeal@innosource.com |
| Brandon Walker | bwalker@innosource.com |
| Brad Scott | bscott@innosource.com |
| PH | ph@innosource.com |
| Chad Dell | cdell@innosource.com |
| Steve Smith | ssmith@innosource.com |

**To field (semicolon-joined):** `sbeal@innosource.com;bwalker@innosource.com;bscott@innosource.com;ph@innosource.com;cdell@innosource.com;ssmith@innosource.com`

---

## terminations-report

Receives the daily Terminations Summary Report (HTML → PDF) each morning.
Same distribution as the recruiter activity report, plus Jerad Kildoo's Goodyear address.

| Name | Email |
|---|---|
| Sean Beal | sbeal@innosource.com |
| Whitney Diem | wdiem@innosource.com |
| Julia Stanton | jstanton@innosource.com |
| PH | ph@innosource.com |
| Chad Delligatti | cdell@innosource.com |
| Jourdan Powers | jpowers@innosource.com |
| Steve Smith | ssmith@innosource.com |
| Becky Henke | bhenke@innosource.com |
| Megan Wonderly | mwonderly@innosource.com |
| Kyle Culver | kculver@innosource.com |
| Jerad Kildoo (InnoSource) | JKildoo@innosource.com |
| Jerad Kildoo (Goodyear) | jerad_kildoo@goodyear.com |
| Mason Dail | mdail@innosource.com |
| Rebecca Heslep | RHeslep@innosource.com |
| Vince Dudinec | vdudinec@innosource.com |
| Abigail Ryan | aryan@innosource.com |
| Ronda Adams | radams@innosource.com |
| Lyndsay Spencer | lspencer@innosource.com |
| Sarah Kozubek | skozubek@innosource.com |
| Graham Eggenschwiler | geggenschwiler@innosource.com |

**To field (semicolon-joined):** `sbeal@innosource.com;wdiem@innosource.com;jstanton@innosource.com;ph@innosource.com;cdell@innosource.com;jpowers@innosource.com;ssmith@innosource.com;bhenke@innosource.com;mwonderly@innosource.com;kculver@innosource.com;JKildoo@innosource.com;jerad_kildoo@goodyear.com;mdail@innosource.com;RHeslep@innosource.com;vdudinec@innosource.com;aryan@innosource.com;radams@innosource.com;lspencer@innosource.com;skozubek@innosource.com;geggenschwiler@innosource.com`

---

## position-status-report

Receives the Daily Position Status Report (HTML → PDF) each morning.
Limited distribution while the report is in pilot.

| Name | Email |
|---|---|
| Sean Beal | sbeal@innosource.com |
| Jourdan Powers | jpowers@innosource.com |

**To field (semicolon-joined):** `sbeal@innosource.com;jpowers@innosource.com`

---

## dsg-monday-brief

Weekly executive summary from Sean Beal to InnoSource leadership.
Sent Monday mornings via Power Automate (sbeal@innosource.com account).

| Name | Email |
|---|---|
| Jourdan Powers | jpowers@innosource.com |
| Abigail Ryan | aryan@innosource.com |
| PH | ph@innosource.com |
| Steve Smith | ssmith@innosource.com |
| Joe Fusco | jfusco@innosource.com |
| Charlie Inkrott | cinkrott@innosource.com |

**To field (semicolon-joined):** `jpowers@innosource.com;aryan@innosource.com;ph@innosource.com;ssmith@innosource.com;jfusco@innosource.com;cinkrott@innosource.com`

---

## inno-ai-update

Weekly executive summary of the InnoSource AI portfolio (Alliant, AEP Sidekick, Connect
Housing, Minnie, Sidekick, etc.) from Sean Beal to leadership.
Sent Friday afternoons via Power Automate (sbeal@innosource.com account — uses the
**send-dsg-monday-brief** flow, which is authenticated as sbeal@; see
`workflows/send-dsg-monday-brief.md`).

**TO:**

| Name | Email |
|---|---|
| Chad Delligatti | cdell@innosource.com |
| PH | ph@innosource.com |
| Steve Smith | ssmith@innosource.com |
| Jake Sieving (RCO) | jsieving@innosource.com |

**TO field (semicolon-joined):** `cdell@innosource.com;ph@innosource.com;ssmith@innosource.com;jsieving@innosource.com`

**CC:**

| Name | Email |
|---|---|
| Zach Boerger | zach@jakib.ai |
| Tony Sackett | tsackett@innosource.com |
| Julia Stanton | jstanton@innosource.com |
| Brad Scott | bscott@innosource.com |
| Brandon Walker | bwalker@innosource.com |
| Tyler McRill | tyler@jakib.ai |
| Lauren Deegan | lauren@jakib.ai |

**CC field (semicolon-joined):** `zach@jakib.ai;tsackett@innosource.com;jstanton@innosource.com;bscott@innosource.com;bwalker@innosource.com;tyler@jakib.ai;lauren@jakib.ai`

**NEVER include:** eparks@innosource.com

---

## _(future reports go here)_

Copy this block when adding a new report or alert:

```
## your-report-id

Brief description of what this report covers and who should receive it.

| Name | Email |
|---|---|
| First Last | name@innosource.com |

**To field (semicolon-joined):** `name@innosource.com`
```

---

_Last updated: 2026-04-27 · Maintainer: Sean Beal_

