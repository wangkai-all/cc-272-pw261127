#!/usr/bin/env python3 -I
"""Test runner that works under isolated mode (-I).

Isolated mode drops cwd/PYTHONPATH from sys.path, so we explicitly put the
project root on sys.path before handing control to unittest discovery.
"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(ROOT, "tests"), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
