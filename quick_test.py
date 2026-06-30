# quick_test.py
from dotenv import load_dotenv
load_dotenv()

from stock import generate_stock_report

result = generate_stock_report("AAPL", "siddhant051231@gmail.com")
print(result)