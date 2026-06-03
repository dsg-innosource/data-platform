# This file should not be imported directly.
#
# The actual `reporting` package lives at `src/reporting/` (src layout, see
# pyproject.toml). After running `pip install -e ./reporting/` from the
# data-platform repo root, `import reporting` resolves to src/reporting/.
#
# This stub exists only so that running Python directly from this directory
# (without installing) still gets a working import path. It redirects __path__
# to the src/reporting subdirectory.
#
# If you find yourself wanting to add code here, add it to src/reporting/__init__.py
# instead.

import os as _os

__path__ = [_os.path.join(_os.path.dirname(__file__), "src", "reporting")]
