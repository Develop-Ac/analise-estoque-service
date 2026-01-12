import main
import pyodbc
import sys
import os

# Ensure stdout is flushed
sys.stdout.reconfigure(line_buffering=True)

# Patch connection for Windows
original_get_connection = main.get_connection

def patched_get_connection():
    print("Using patched SQL Server driver for Windows...")
    server = os.getenv('SQL_HOST', '127.0.0.1')
    port = os.getenv('SQL_PORT', '1433')
    database = os.getenv('SQL_DATABASE', 'master')
    user = os.getenv('SQL_USER', 'sa')
    password = os.getenv('SQL_PASSWORD', 'senha_secreta')
    
    # Use SQL Server driver found on system
    conn_str = (
        "DRIVER={SQL Server};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
    )
    return pyodbc.connect(conn_str)

main.get_connection = patched_get_connection

print("Starting manual analysis run (Windows Patched)...")
try:
    main.run_job()
    print("Manual run completed successfully.")
except Exception as e:
    print(f"Error during manual run: {e}")
    import traceback
    traceback.print_exc()
