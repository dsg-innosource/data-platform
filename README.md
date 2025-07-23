# Data Platform

A centralized data warehouse and analytics repository built with SQL Mesh for scalable data transformations and reporting.

## Overview

This repository serves as the central hub for all data operations, including:
- Data transformations and modeling using SQL Mesh
- Custom transformation layers as an alternative to traditional ELT tools
- SQL scripts for reporting and analytics
- Shared utilities and macros for data engineering workflows

## Project Structure

```
data-platform/
├── scripts/           # Miscellaneous SQL scripts for reporting
├── sqlmesh/          # SQL Mesh project - main transformation engine
├── transformations/   # Custom transformation layer (alternative to AirByte)
├── docs/             # Documentation
├── tests/            # Shared testing utilities
└── shared/           # Common macros, utilities
```

## Getting Started

### Prerequisites
- Python 3.8+
- Git

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data-platform
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Initialize SQL Mesh project** (if not already done)
   ```bash
   cd sqlmesh
   sqlmesh init
   ```

### Common Commands

- **Plan changes**: `sqlmesh plan`
- **Apply changes**: `sqlmesh apply` 
- **Run transformations**: `sqlmesh run`
- **View project info**: `sqlmesh info`

## Documentation

- [SQL Mesh Documentation](https://sqlmesh.readthedocs.io/)
- [Getting Started with SQL Mesh](https://sqlmesh.readthedocs.io/en/stable/quickstart/)
- [SQL Mesh Concepts](https://sqlmesh.readthedocs.io/en/stable/concepts/overview/)

## Contributing

1. Create a feature branch
2. Make your changes
3. Test your transformations
4. Submit a pull request

## Directory Purposes

- **scripts/**: Ad-hoc SQL queries and reporting scripts
- **sqlmesh/**: Core data transformation logic using SQL Mesh framework
- **transformations/**: Custom Python-based data transformations
- **docs/**: Project documentation and data dictionaries
- **tests/**: Test utilities and validation scripts
- **shared/**: Reusable macros, functions, and utilities