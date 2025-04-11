import pandas as pd
import psycopg2
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from datetime import datetime

# === Connect to DB ===
conn = psycopg2.connect(
    dbname="your_db",          # Replace with your DB name
    user="your_user",          # Replace with your DB user
    password="your_password",  # Replace with your DB password
    host="your_host",          # Replace with your DB host
    port="5432"                # Replace with your DB port (default is 5432)
)

# === Call the Function ===
sql = "SELECT * FROM get_severity_summary();"
df = pd.read_sql(sql, conn)

# === Pivot the data ===
# Combine bucket + count as formatted strings (e.g., "2 P1, 3 P5+")
df['breakdown'] = df['count'].astype(str) + ' P' + df['bucket']

# Collapse multiple severities per day into single cell
pivot_df = (
    df.groupby(['tech_id', 'first_name', 'state', 'city', 'zone', 'team', 'closed_day'])
      .agg({'breakdown': lambda x: ', '.join(sorted(x))})
      .reset_index()
      .pivot(index=['tech_id', 'first_name', 'state', 'city', 'zone', 'team'],
             columns='closed_day',
             values='breakdown')
      .reset_index()
)

# Add "Total SRs" column (total SR count per tech)
df_totals = df.groupby(['tech_id', 'first_name', 'state', 'city', 'zone', 'team'])['count'].sum().reset_index(name='Total SRs')
pivot_df = pd.merge(df_totals, pivot_df, on=['tech_id', 'first_name', 'state', 'city', 'zone', 'team'])

# === Output to Excel ===
excel_file = f"srs_severity_report_{datetime.now().strftime('%Y%m%d')}.xlsx"
sheet_name = "SR Report"
pivot_df.to_excel(excel_file, index=False, sheet_name=sheet_name)

# Style the sheet as an Excel table
wb = load_workbook(excel_file)
ws = wb[sheet_name]
end_col = chr(64 + pivot_df.shape[1])  # Assumes <26 columns (A-Z); for more columns, use openpyxl.utils.get_column_letter
table_range = f"A1:{end_col}{pivot_df.shape[0]+1}"
table = Table(displayName="SRReport", ref=table_range)
style = TableStyleInfo(name="TableStyleMedium9", showFirstColumn=False,
                       showLastColumn=False, showRowStripes=True, showColumnStripes=False)
table.tableStyleInfo = style
ws.add_table(table)
wb.save(excel_file)

# Print the final file location
print(f"✅ Report ready: {excel_file}")
