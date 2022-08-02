import codecs

import pandas as pd
import numpy as np
import yfinance as yahooFinance
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


conn_string1 = "postgresql://postgres:Robo4Me12345@rap-pgdb.roboforus.com:5432/MODEL_PORTFOLIO"
conn_string2 = "postgresql://postgres:Robo4Me12345@rap-pgdb.roboforus.com:5432/REFERENCE_DATA"


db1 = create_engine(conn_string1)
conn1 = db1.raw_connection()

db2 = create_engine(conn_string2)
conn2 = db2.raw_connection()

def get_allocation_table(conn,tab_name,column_name,values_list):

    str_placeholders="'%s',"*len(values_list)

    str_placeholder =str_placeholders[:-1]

    select_query_str= "Select * From " +tab_name+ " where " + column_name+ " in ("+ str_placeholder +")"
    select_query= select_query_str % tuple(values_list)
    with conn.cursor() as cursor:
        try:
            cursor.execute(select_query) #executes select query
            data = cursor.fetchall()
            cols = []
            for elt in cursor.description:
                cols.append(elt[0])
            df = pd.DataFrame(data=data, columns=cols)
            return df
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
                print(40, "Unable to execute sql batch statements", error)
                conn.commit()




def compare_output(df1, df2):
    df_all = pd.concat([df1.set_index('Instrument Symbol'), df2.set_index('Instrument Symbol')],
                       axis='columns', keys=['March 31', 'Feb 28'])
    df = df_all.swaplevel(axis='columns')[df1.columns[1:]]
    return df



def highlight_diff(data, color='yellow'):
    attr = 'background-color: {}'.format(color)
    other = data.xs('March 31', axis='columns', level=-1)
    return pd.DataFrame(np.where(data.ne(other, level=0), attr, ''),
                        index=data.index, columns=data.columns).round(2)


def read_table(df):
    df_name = df + ".csv"
    table = pd.read_csv(df_name)
    DF = table.set_index('Model Portfolio ID')
    return DF

def get_table(df):
    #df_new = df.drop(['sl_key', 'date_of_allocation'], axis=1)
    df_rename = df.round(2)
    return df_rename



for i in range(1, 16):
    allocation_table = get_allocation_table(conn1, "asset_allocation.model_portfolio_allocation_curr","model_portfolio_id", [i])

    allocation_table = pd.DataFrame(allocation_table)
    allocation_table = allocation_table.drop(['csm_instrument_id', 'sl_key','date_of_allocation'], axis=1)
    allocation_table.rename(columns= {'model_portfolio_id': 'Model Portfolio ID', 'csm_instrument_symbol':'Instrument Symbol', 'instrument_weight':'Weights', 'date_of_portfolio':'Date'}, inplace=True)


    asset_allocation = allocation_table.style.hide(axis= 'index')

    asset_allocation_table = allocation_table.set_index('Model Portfolio ID')

    table_name = "asset_allocation_table" + str(i) + ".csv"
    asset_allocation_table.to_csv(table_name)



asset_allocation_table1 = read_table("asset_allocation_table1")
asset_allocation_table2 = read_table("asset_allocation_table2")
asset_allocation_table3 = read_table("asset_allocation_table3")
asset_allocation_table4 = read_table("asset_allocation_table4")
asset_allocation_table5 = read_table("asset_allocation_table5")
asset_allocation_table6 = read_table("asset_allocation_table6")
asset_allocation_table7 = read_table("asset_allocation_table7")
asset_allocation_table8 = read_table("asset_allocation_table8")
asset_allocation_table9 = read_table("asset_allocation_table9")
asset_allocation_table10 = read_table("asset_allocation_table10")
asset_allocation_table11 = read_table("asset_allocation_table11")
asset_allocation_table12 = read_table("asset_allocation_table12")
asset_allocation_table13 = read_table("asset_allocation_table13")
asset_allocation_table14 = read_table("asset_allocation_table14")
asset_allocation_table15 = read_table("asset_allocation_table15")




df1= compare_output(asset_allocation_table1, asset_allocation_table2)
df2= compare_output(asset_allocation_table4, asset_allocation_table4)
df3= compare_output(asset_allocation_table7, asset_allocation_table5)
df4= compare_output(asset_allocation_table10, asset_allocation_table6)
df5= compare_output(asset_allocation_table13, asset_allocation_table8)


df6= compare_output(asset_allocation_table2, asset_allocation_table2)
df7= compare_output(asset_allocation_table5, asset_allocation_table3)
df8= compare_output(asset_allocation_table8, asset_allocation_table4)
df9= compare_output(asset_allocation_table11, asset_allocation_table7)
df10= compare_output(asset_allocation_table14, asset_allocation_table14)


df11= compare_output(asset_allocation_table3, asset_allocation_table1)
df12= compare_output(asset_allocation_table6, asset_allocation_table5)
df13= compare_output(asset_allocation_table9, asset_allocation_table7)
df14= compare_output(asset_allocation_table12, asset_allocation_table6)
df15= compare_output(asset_allocation_table15, asset_allocation_table8)




df1.rename(columns={'Weights':'Risk Averse'}, inplace=True)
df2.rename(columns={'Weights':'Risk Moderate'}, inplace=True)
df3.rename(columns={'Weights':'Moderate'}, inplace=True)
df4.rename(columns={'Weights':'Moderate Aggressive'}, inplace=True)
df5.rename(columns={'Weights':'Aggressive'}, inplace=True)

df6.rename(columns={'Weights':'Risk Averse'}, inplace=True)
df7.rename(columns={'Weights':'Risk Moderate'}, inplace=True)
df8.rename(columns={'Weights':'Moderate'}, inplace=True)
df9.rename(columns={'Weights':'Moderate Aggressive'}, inplace=True)
df10.rename(columns={'Weights':'Aggressive'}, inplace=True)

df11.rename(columns={'Weights':'Risk Averse'}, inplace=True)
df12.rename(columns={'Weights':'Risk Moderate'}, inplace=True)
df13.rename(columns={'Weights':'Moderate'}, inplace=True)
df14.rename(columns={'Weights':'Moderate Aggressive'}, inplace=True)
df15.rename(columns={'Weights':'Aggressive'}, inplace=True)







df_final1= pd.concat([df1, df2, df3, df4, df5], axis=1)
df_final1= get_table(df_final1)
df_final1 = df_final1.style.apply(highlight_diff, axis=None).format("{:.2f}%")


df_final2= pd.concat([df6, df7, df8, df9, df10], axis=1)
df_final2= get_table(df_final2)
df_final2 = df_final2.style.apply(highlight_diff, axis=None).format("{:.2f}%")

df_final3= pd.concat([df11, df12, df13, df14, df15], axis=1)
df_final3 = get_table(df_final3)
df_final3 = df_final3.style.apply(highlight_diff, axis=None).format("{:.2f}%")


df1.plot(kind='bar').set_xlabel("Risk Averse")
plt.savefig("risk_averse.png")
df2.plot(kind='bar').set_xlabel("Risk Moderate")
plt.savefig("risk_moderate.png")
df3.plot(kind='bar').set_xlabel("Moderate")
plt.savefig("moderate.png")
df4.plot(kind='bar').set_xlabel("Moderate Aggressive")
plt.savefig("moderate_aggressive.png")
df5.plot(kind='bar').set_xlabel("aggressive")
plt.savefig("aggressive.png")


df6.plot(kind='bar').set_xlabel("Risk Averse")
plt.savefig("risk_averse_2.png")
df7.plot(kind='bar').set_xlabel("Risk Moderate")
plt.savefig("risk_moderate_2.png")
df8.plot(kind='bar').set_xlabel("Moderate")
plt.savefig("moderate_2.png")
df9.plot(kind='bar').set_xlabel("Moderate Aggressive")
plt.savefig("moderate_aggressive_2.png")
df10.plot(kind='bar').set_xlabel("aggressive")
plt.savefig("aggressive_2.png")


df11.plot(kind='bar').set_xlabel("Risk Averse")
plt.savefig("risk_averse_3.png")
df12.plot(kind='bar').set_xlabel("Risk Moderate")
plt.savefig("risk_moderate_3.png")
df13.plot(kind='bar').set_xlabel("Moderate")
plt.savefig("moderate_3.png")
df14.plot(kind='bar').set_xlabel("Moderate Aggressive")
plt.savefig("moderate_aggressive_3.png")
df15.plot(kind='bar').set_xlabel("aggressive")
plt.savefig("aggressive_3.png")









goal_table = get_allocation_table(conn2, "master.investment_goal","investment_goal_id", [1,2,3])
plan_table = get_allocation_table(conn2, "master.subscription_plan","subscription_plan_id", [1, 2, 3,4])
profile_table = get_allocation_table(conn2, "master.risk_profile","risk_profile_id", [1,2,3,4,5])
optimization_table = get_allocation_table(conn2, "master.investment_optimization","optimization_method_id", [1,2,3])


goal_table = pd.DataFrame(goal_table)
goal_table = goal_table.drop(['investment_goal_desc'], axis=1)
goal_table.rename(columns= {'investment_goal_id': 'Investment Goal ID', 'investment_goal_cd':'Investment Goal'}, inplace=True)


plan_table = pd.DataFrame(plan_table)
plan_table = plan_table.drop(['subscription_plan_desc'], axis=1)
plan_table.rename(columns= {'subscription_plan_id': 'Subsciption Plan ID', 'subsciption_plan_cd':'Subsciption Plan'}, inplace=True)



profile_table = pd.DataFrame(profile_table)
profile_table = profile_table.drop(['risk_profile_desc'], axis=1)
profile_table.rename(columns= {'risk_profile_id': 'Risk Profile ID', 'risk_profile_cd':'Risk Profile'}, inplace=True)


optimization_table = pd.DataFrame(optimization_table)
optimization_table.rename(columns= {'optimization_method_id': 'Optimization Method ID', 'optimization_method_cd':'Optimization Method'}, inplace=True)









from jinja2 import Environment, FileSystemLoader

# 2. Create a template Environment
env = Environment(loader=FileSystemLoader('templates'))

# 3. Load the template from the Environment
template = env.get_template('html_template.html')


# 4. Render the template with variables
html = template.render(page_title_text='Asset Allocation Report',
                        header_text= 'Portfolio Asset Allocation',


                        text1= 'BUILD WEALTH / BRONZE / MVO_MAX_SHARPE',
                        text2= 'BUILD WEALTH / BRONZE / MVO_MINIMIZE_VARIANCE',
                        text3= 'BUILD WEALTH / BRONZE / MVO_MAXIMIZE_RETURN_SUBJECT_TO',

                        goal_table= goal_table.to_html(index=False),
                       plan_table= plan_table.to_html(index=False),
                       profile_table = profile_table.to_html(index=False),
                       optimization_table = optimization_table.to_html(index=False),

                        asset_allocation_table1 = df_final1.to_html(),
                        asset_allocation_table2 = df_final2.to_html(),
                        asset_allocation_table3 = df_final3.to_html(),



                        image1 = 'risk_averse.png',
                        image2 = 'risk_moderate.png',
                       image3 = 'moderate.png',
                       image4 = 'moderate_aggressive.png',
                       image5 = 'aggressive.png',
                        image6 = 'risk_averse_2.png',
                        image7 = 'risk_moderate_2.png',
                        image8 = 'moderate_2.png',
                        image9 = 'moderate_aggressive_2.png',
                        image10 = 'aggressive_2.png',
                        image11 = 'risk_averse_3.png',
                        image12 = 'risk_moderate_3.png',
                        image13 = 'moderate_3.png',
                        image14 = 'moderate_aggressive_3.png',
                        image15 = 'aggressive_3.png')



# 5. Write the template to an HTML file
with open('asset_allocation_report_copy.html', 'w') as f:
    f.write(html)



