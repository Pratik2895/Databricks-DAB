# Databricks Community Update — Latest Highlights

An **idempotent Databricks notebook** that dynamically fetches the last 3 months of Databricks platform release notes and renders a shareable one-pager visual summary.

## What It Does

Every time you run this notebook, it:

1. **Fetches** release notes from `docs.databricks.com/aws/en/release-notes/product/` for the trailing 3 months (auto-determined from today's date)
2. **Categorizes** all updates into 9 areas: AI & Agents, Data Engineering, Governance & Sharing, Lakebase/OLTP, BI & Analytics, Compute & Runtime, SQL & Functions, Apps & Platform, and Other
3. **Renders a one-pager visual** with all updates grouped by category, color-coded headers, and dates
4. **Generates breakdown charts** — monthly update counts and category distribution
5. **Produces a detailed table** of all updates sortable by date and category
6. **Creates a shareable poster image** for social media with top highlights and a "How to Get Updates" guide

## Key Features

- **Idempotent**: No hardcoded data. Every run fetches fresh content from Databricks docs.
- **Self-updating**: The 3-month window shifts automatically as time passes.
- **Shareable**: Export the poster image or the full notebook as HTML for your community.
- **AI-powered**: A scheduled Genie Code task runs this notebook every Monday at 9 AM IST and produces a community-ready summary.

## How to Use

### Option 1: Run on Databricks
1. Import this `.ipynb` file into your Databricks workspace
2. Run all cells top to bottom
3. View the one-pager visual, charts, and detailed table
4. Screenshot the poster image (Cell 7) for sharing

### Option 2: Run locally with Jupyter
1. Install dependencies: `pip install requests matplotlib pandas`
2. Run all cells
3. The notebook fetches data from Databricks public docs — no authentication needed

## Data Source

All updates are fetched from the official Databricks product release notes:
`https://docs.databricks.com/aws/en/release-notes/product/`

## Scheduled AI Task

A weekly Genie Code scheduled task runs this notebook every Monday at 9 AM IST and produces a community-ready summary with:
- Top 5 most impactful updates
- Category breakdown
- GA, Beta, and deprecation highlights
- Link to the full notebook

## Author

**Pratik Bhikadiya** -- Data & Analytics Engineer | Databricks + PySpark + AWS  
[GitHub](https://github.com/Pratik2895) | [Portfolio](https://codebasics.io/portfolio/Bhikadiya-Pratik)

---

*This notebook auto-refreshes data on every run. No manual updates needed.*
