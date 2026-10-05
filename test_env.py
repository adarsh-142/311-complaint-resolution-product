import subprocess
import sys

# 1. Python executable correctness
print("PYTHON EXECUTABLE:")
print(sys.executable)
print("-" * 50)

# 2. subprocess stability test
print("SUBPROCESS TEST:")
result = subprocess.run(
    [sys.executable, "-c", "print('WORKER TEST OK')"], capture_output=True, text=True
)

print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
print("-" * 50)

print("DONE")
