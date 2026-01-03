#!/usr/bin/env python3

# Minimal debug script to test the random import issue

import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print("Testing random module...")
import random
print(f"✓ random module imported successfully")

print("\nTesting basic random functions...")
print(f"random.randint(0, 10): {random.randint(0, 10)}")
print(f"random.choice(['a', 'b', 'c']): {random.choice(['a', 'b', 'c'])}")
print("✓ Random functions work")

print("\nTesting datetime module...")
from datetime import datetime, timedelta
now = datetime.now()
delta = timedelta(minutes=30)
print(f"Now: {now}")
print(f"Now - 30min: {now - delta}")
print("✓ Datetime functions work")

print("\nTesting pandas module...")
import pandas as pd
data = {'col1': [1, 2, 3], 'col2': [4, 5, 6]}
df = pd.DataFrame(data)
print(f"✓ Pandas DataFrame created: {len(df)} rows")

print("\nAll basic modules work correctly!")
