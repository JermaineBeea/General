import sys

x = int(sys.argv[1]) if len(sys.argv) > 1 else 5
result = 2 * x
print(f"2 * {x} = {result}")