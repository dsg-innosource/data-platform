#!/usr/bin/env python3
"""
Test runner script for ADP pipeline using DuckDB.

This script runs all tests using DuckDB as the backend, allowing you to test
the complete pipeline without connecting to the production PostgreSQL database.

Usage:
    python transformations/run_tests.py
    python transformations/run_tests.py --verbose
    python transformations/run_tests.py --test-type unit
    python transformations/run_tests.py --test-type integration
"""

import subprocess
import sys
import argparse
from pathlib import Path


def run_tests(test_type="all", verbose=False):
    """Run the test suite."""
    transformations_dir = Path(__file__).parent
    tests_dir = transformations_dir / "tests"
    
    # Base pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Add test directory or specific test files
    if test_type == "unit":
        cmd.extend([
            str(tests_dir / "test_extract.py"),
            str(tests_dir / "test_transform.py"),
            str(tests_dir / "test_load.py")
        ])
    elif test_type == "integration":
        cmd.extend([
            str(tests_dir / "test_integration.py"),
            str(tests_dir / "test_monday_workflow.py")
        ])
    elif test_type == "performance":
        cmd.append(str(tests_dir / "test_performance.py"))
    elif test_type == "workflow":
        cmd.append(str(tests_dir / "test_monday_workflow.py"))
    else:  # all
        cmd.append(str(tests_dir))
    
    # Add verbose flag if requested
    if verbose:
        cmd.append("-v")
    
    # Add coverage if available
    try:
        import pytest_cov
        cmd.extend(["--cov=transformations.adp", "--cov-report=term-missing"])
    except ImportError:
        pass
    
    print(f"Running command: {' '.join(cmd)}")
    print("-" * 60)
    
    # Run the tests
    result = subprocess.run(cmd, cwd=transformations_dir.parent)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Run ADP pipeline tests with DuckDB")
    parser.add_argument(
        "--test-type", 
        choices=["all", "unit", "integration", "performance", "workflow"], 
        default="all",
        help="Type of tests to run (default: all)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Run tests in verbose mode"
    )
    
    args = parser.parse_args()
    
    print("🧪 ADP Pipeline Test Suite")
    print("=" * 40)
    print(f"Test Type: {args.test_type}")
    print(f"Database: DuckDB (test mode)")
    print("=" * 40)
    print()
    
    exit_code = run_tests(args.test_type, args.verbose)
    
    print()
    print("=" * 40)
    if exit_code == 0:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")
    print("=" * 40)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()