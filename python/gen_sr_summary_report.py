import pandas as pd
from sqlalchemy import create_engine
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter
from datetime import datetime

# === 1. Connect to DB using SQLAlchemy ===
engine = create_engine("postgresql+psycopg2://your_user:your_password@your_host:5432/your_db")

# === 2. Call the PostgreSQL function ===
sql = "SELECT * FROM get_severity_summary();"
with engine.connect() as conn:
    df = pd.read_sql(sql, conn)

# === 3. Create breakdown string per severity bucket ===
df['breakdown'] = df['count'].astype(str) + ' P' + df['bucket']

# === 4. Collapse multiple severities per day into single cell ===
pivot_df = (
    df.groupby(['tech_id', 'first_name', 'state', 'city', 'zone', 'team', 'closed_day'])
      .agg({'breakdown': lambda x: ', '.join(sorted(x))})
      .reset_index()
      .pivot(index=['tech_id', 'first_name', 'state', 'city', 'zone', 'team'],
             columns='closed_day',
             values='breakdown')
      .reset_index()
)

# === 5. Calculate Total SRs ===
df_totals = df.groupby(['tech_id', 'first_name', 'state', 'city', 'zone', 'team'])['count'].sum().reset_index(name='Total SRs')

# === 6. Merge totals into pivot and move "Total SRs" to end ===
full_df = pd.merge(pivot_df, df_totals, on=['tech_id', 'first_name', 'state', 'city', 'zone', 'team'])

# Move "Total SRs" to the last column
cols = [col for col in full_df.columns if col != 'Total SRs'] + ['Total SRs']
full_df = full_df[cols]

# === 7. Export to Excel ===
excel_file = f"srs_severity_report_{datetime.now().strftime('%Y%m%d')}.xlsx"
sheet_name = "SR Report"
full_df.to_excel(excel_file, index=False, sheet_name=sheet_name)

# === 8. Style the Excel sheet as a table ===
wb = load_workbook(excel_file)
ws = wb[sheet_name]

end_col = get_column_letter(full_df.shape[1])
table_range = f"A1:{end_col}{full_df.shape[0] + 1}"

# Safe table name (must be alphanumeric, no spaces)
table = Table(displayName="SRReportTable", ref=table_range)

style = TableStyleInfo(name="TableStyleMedium9",
                       showFirstColumn=False,
                       showLastColumn=False,
                       showRowStripes=True,
                       showColumnStripes=False)

table.tableStyleInfo = style
ws.add_table(table)

wb.save(excel_file)

print(f"✅ Excel report generated: {excel_file}")
