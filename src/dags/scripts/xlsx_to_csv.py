import sys
import pandas as pd

input=sys.argv[1]
output=sys.argv[2]

def convert_xlsx_2_csv(input, output):
    df = pd.read_excel(input)
    df.to_csv(output, index = None, header=True)

convert_xlsx_2_csv(input, output)
