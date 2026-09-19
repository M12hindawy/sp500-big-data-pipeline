"""
Comprehensive Test Suite Runner for S&P 500 Data Pipeline
Executes schema, transformation, data quality, consistency, and time model tests.
"""
import sys
import subprocess
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TESTS_DIR.parent

def main():
    print("============================================================")
    print("RUNNING ALL S&P 500 PIPELINE AUTOMATED TESTS")
    print(f"Project Directory: {PROJECT_ROOT}")
    print("============================================================")

    # Set PYTHONPATH to project root so tests can import producer and spark.apps
    env = dict(subprocess.os.environ)
    env["PYTHONPATH"] = str(PROJECT_ROOT)

    cmd = [sys.executable, "-m", "pytest", str(TESTS_DIR), "-v", "--tb=short"]
    res = subprocess.run(cmd, env=env)
    
    if res.returncode == 0:
        print("\n============================================================")
        print("ALL TESTS PASSED SUCCESSFULLY!")
        print("============================================================")
    else:
        print("\n============================================================")
        print("SOME TESTS FAILED! Inspect the output above.")
        print("============================================================")
    sys.exit(res.returncode)

if __name__ == "__main__":
    main()
