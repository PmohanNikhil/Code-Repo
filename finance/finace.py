import pandas as pd
import uuid

# Create date range
dates = pd.date_range(start='2025-11-01', end='2026-12-31')

# Create dataframe
df = pd.DataFrame({
    'Date': dates,
    'unique_id': [str(uuid.uuid4()) for _ in range(len(dates))]
})

# Export to Excel
df.to_excel('Code-Repo/finance/dates_with_uuid.xlsx', index=False)
