# Documentation Directory

This directory contains all project documentation organized by purpose and audience.

## Documentation Structure

```
docs/
├── guides/           # User guides - How to run processes
├── technical/        # Technical documentation - How it works
├── database/         # Database schema documentation
└── templates/        # Document templates (if any)
```

## User Guides

**Audience**: Anyone running the monthly processes

**Purpose**: Step-by-step instructions for executing data workflows

**Available Guides**:
- **[ADP Headcount](guides/ADP_Headcount.md)** - Weekly ADP employee data loading process
- **[Monthly Billing](guides/Monthly_Billing.md)** - ClickUp time tracking to billing reports
- **[PDF Export Guide](PDF_EXPORT_GUIDE.md)** - Converting Markdown documentation to professional PDFs

## Technical Documentation

**Audience**: Developers, system administrators, data engineers

**Purpose**: Detailed architecture, configuration, troubleshooting, and implementation details

**Available Docs**:
- **[ADP Pipeline](technical/ADP_Pipeline.md)** - Complete technical documentation for ADP headcount process
- **[Billing Process](technical/Billing_Process.md)** - Technical details for monthly billing system

## Database Documentation

**Audience**: Database administrators, analysts, data engineers

**Purpose**: Schema definitions, relationships, and data dictionary

**Available Docs**:
- **[Database Schemas](database/schemas.md)** - All table definitions, columns, indexes, and relationships

## Documentation Standards

### User Guides
- Focus on "how to do it"
- Step-by-step instructions
- Minimal technical jargon
- Quick troubleshooting section
- Link to technical docs for details
- Keep to 2-3 pages maximum

### Technical Documentation
- Comprehensive architecture and design
- Detailed configuration options
- Complete troubleshooting guide
- Code examples and implementation details
- Performance considerations
- Security notes
- Can be as long as needed

### Database Documentation
- Complete schema definitions
- Column descriptions and data types
- Relationships and foreign keys
- Sample queries
- Indexing strategy
- Data retention policies

## Contributing to Documentation

When adding new features or processes:

1. **Create user guide** in `guides/` with simple step-by-step instructions
2. **Create technical doc** in `technical/` with comprehensive details
3. **Update database doc** in `database/` if schema changes
4. **Update this README** with links to new documentation
5. **Update project README** with references to new guides

### Documentation Template

**User Guide Structure**:
```markdown
# [Process Name]

Quick guide for running [process].

## Prerequisites
## Monthly Workflow
## Quick Troubleshooting
## File Locations
```

**Technical Doc Structure**:
```markdown
# [Process Name] - Technical Documentation

## Overview
## Architecture
## Configuration
## Data Transformation
## Error Handling
## Advanced Usage
## Troubleshooting
```

## Finding Documentation

**Need to run a process?** → Check `guides/`

**Need to configure or troubleshoot?** → Check `technical/`

**Need database information?** → Check `database/`

**Need to export PDFs?** → Check `PDF_EXPORT_GUIDE.md`

---

**Last Updated**: 2025-11-03
