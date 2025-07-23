# Data Platform Repository Setup

Please help me create a new data platform repository with the following requirements:

## Repository Setup
1. Create a new directory called `data-platform`
2. Initialize it as a git repository
3. Set up the following folder structure:
   ```
   data-platform/
   ├── scripts/           # Miscellaneous SQL scripts for reporting
   ├── sqlmesh/          # SQL Mesh project
   ├── transformations/   # Custom transformation layer (alternative to AirByte)
   ├── docs/             # Documentation
   ├── tests/            # Shared testing utilities
   └── shared/           # Common macros, utilities
   ```

## Files to Create
1. **README.md** - Include:
   - Project description as a centralized data warehouse/analytics repository
   - Explanation of folder structure
   - Basic getting started instructions
   - Links to SQL Mesh documentation

2. **.gitignore** - Appropriate for Python/SQL projects, including:
   - Python virtual environments
   - SQL Mesh cache/build directories
   - IDE files
   - OS-specific files

3. **requirements.txt** - With SQL Mesh, transformation dependencies (pandas, sqlalchemy, etc.), and common data tools

4. Add placeholder README files in each subdirectory explaining their purpose

## Python Environment Setup
1. Set up a Python virtual environment in the root `data-platform/` directory (using venv or conda)
2. Install the latest version of SQL Mesh and all transformation dependencies
3. Initialize a new SQL Mesh project within the `sqlmesh/` folder
4. Ensure all components can access the shared environment

## Additional Setup
1. Create a new repository on GitHub called `data-platform`
2. Add the GitHub remote to the local repository
3. Push the initial commit with all the setup files

## Custom Transformation Layer Setup
1. Create the `transformations/` directory (leave empty for now - existing codebase will be moved here later)
- Add any standard data engineering tooling configuration files you recommend
- Include a basic GitHub Actions workflow for SQL validation if applicable

Please walk me through each step and let me know if you need any additional information about my specific setup or preferences.