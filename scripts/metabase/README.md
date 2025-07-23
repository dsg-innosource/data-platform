# Metabase Models

This directory contains SQL files used to create Metabase models for reporting and analytics.

## Purpose

- SQL queries that power Metabase dashboards and questions
- Model definitions for consistent data views
- Reusable queries for common business metrics

## File Naming Convention

Use descriptive names that indicate the business function:
- `adp-tenure.sql` - Employee tenure data from ADP
- `ai-tenure-detail.sql` - Detailed AI scoring analysis

## Comment Header Standard

All Metabase model files should include this standardized header:

```sql
/*
 * Metabase Model: [Model Name]
 * URL: [Metabase dashboard/question URL]
 * Description: [Brief 1-2 line description]
 * Created: [YYYY-MM-DD]
 * Last Updated: [YYYY-MM-DD]
 */
```

## Best Practices

- Include the Metabase URL for easy navigation back to the dashboard
- Add clear descriptions explaining what business question the model answers
- Keep creation and update dates current
- Use meaningful model names that reflect the business purpose
- Document any complex logic or business rules in comments