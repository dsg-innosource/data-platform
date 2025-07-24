"""
pytest configuration file for ADP pipeline tests.
"""
import sys
from pathlib import Path

# Add the transformations directory to Python path for imports
transformations_dir = Path(__file__).parent.parent
sys.path.insert(0, str(transformations_dir))