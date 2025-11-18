import os

x = int(os.environ.get('X_VALUE', '5'))
result = 2 * x
print(f"2 * {x} = {result}")