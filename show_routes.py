import re

with open("app.py", encoding="utf-8", errors="ignore") as f:
    lines = f.read().splitlines()

pattern = r'@app\.route\(\"/\"\)'
idx = [i for i, l in enumerate(lines) if re.search(pattern, l)]

print("matches:", [i + 1 for i in idx])

for i in idx:
    a = max(0, i - 2)
    b = min(len(lines), i + 25)
    print(f"\n--- app.py lines {a+1} to {b} ---")
    for n in range(a + 1, b + 1):
        print(f"{n:>4}: {lines[n-1]}")