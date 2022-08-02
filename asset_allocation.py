import pandas as pd
import numpy as np
import yfinance as yahooFinance
import datetime


def compare_output(df1, df2):
    df_all = pd.concat([df1.set_index('Asset Class'), df2.set_index('Asset Class')],
                       axis='columns', keys=['First', 'Second'])
    df = df_all.swaplevel(axis='columns')[df1.columns[1:]]
    return df


def highlight_diff(data, color='yellow'):
    attr = 'background-color: {}'.format(color)
    other = data.xs('First', axis='columns', level=-1)
    return pd.DataFrame(np.where(data.ne(other, level=0), attr, ''),
                        index=data.index, columns=data.columns)



Output_1 = pd.read_excel(r'C:\Users\user\PycharmProjects\asset_allocation_report\ETF selection criteria (version 1).xlsx', sheet_name='Output_1')
Output_1 = pd.DataFrame(Output_1)


Output_2 = pd.read_excel(r'C:\Users\user\PycharmProjects\asset_allocation_report\ETF selection criteria (version 1).xlsx', sheet_name='Output_2')
Output_2 = pd.DataFrame(Output_2)


df = compare_output(Output_1, Output_2)
df_final = df.fillna('')

df_final = df_final.style.apply(highlight_diff, axis=None)
print(df_final)

df_final.to_excel('comparison_table.xlsx')








