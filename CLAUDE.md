# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data platform repository designed to serve as a centralized data warehouse and analytics platform. The project is currently in the initial setup phase, with the complete architecture defined in `data_platform_prompt.md`.

## Planned Architecture

The repository will follow this structure once fully implemented:
```
data-platform/
├── scripts/           # Miscellaneous SQL scripts for reporting
├── sqlmesh/          # SQL Mesh project (main data transformation engine)
├── transformations/   # Custom transformation layer (alternative to AirByte)
├── docs/             # Documentation
├── tests/            # Shared testing utilities
└── shared/           # Common macros, utilities
```

## Development Setup Commands

Since this is a new repository, the standard development workflow will involve:

1. **Initial Setup** (from data_platform_prompt.md):
   - Create Python virtual environment: `python -m venv venv`
   - Activate environment: `source venv/bin/activate` (Unix) or `venv\Scripts\activate` (Windows)
   - Install dependencies: `pip install -r requirements.txt` (once created)
   - Initialize SQL Mesh project in `sqlmesh/` directory

2. **SQL Mesh Commands** (once implemented):
   - Plan changes: `sqlmesh plan`
   - Apply changes: `sqlmesh apply`
   - Run transformations: `sqlmesh run`

## Key Technologies

- **SQL Mesh**: Primary data transformation and modeling framework
- **Python**: For custom transformations and utilities
- **SQL**: For data modeling and reporting scripts

## Repository Status

This repository is currently in the planning phase. The `data_platform_prompt.md` file contains the complete specification for setting up the initial structure, dependencies, and workflows. Future Claude instances should reference this file when implementing the planned architecture.