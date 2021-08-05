#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Date and Market added FINAL and data scraping is done for N days

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
    
f = int(input('Enter upto How many days You want to scrap data from Today: '))
dt_lst = []
today = datetime.date.today()
for d in range(0,f):
    start_date = today - datetime.timedelta(d)
    start_date_string = str(start_date.strftime("%d-%m-%Y"))
    dt_lst.append(start_date_string)


#p_1 = []
#for n in range(0,100):
#    p_1.append('//*[@id="ContentPlaceHolder1_DataList1_Button1_' + str(n) +'"]')

p_2 = []
for m in range(2,5):
    p_2.append('//*[@id="ContentPlaceHolder1_grdDaily"]/tbody/tr[' + str(m) +']/td[2]/a')

df = pd.DataFrame()
df_list = []

try:
    for dt in range(len(dt_lst)):
        print(dt_lst[dt])
        date_td = dt_lst[dt]
        p_1 = []
        for n in range(0, 100):
            p_1.append('//*[@id="ContentPlaceHolder1_DataList1_Button1_' + str(n) + '"]')

        for w in range(0,110):
            print(f'w value after: {w}')
            try:
                PATH = "C:\Program Files (x86)\chromedriver.exe"
                options = Options()
                options.add_argument("--headless")
                driver = webdriver.Chrome(chrome_options=options, executable_path=PATH)
                driver.get('http://tsmarketing.in/HomePageGe.aspx')


                dateclick = driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_txtDate"]')
                dateclick.click()
                dateclick.send_keys(Keys.CONTROL, 'a')
                #dateclick.send_keys(Keys.BACK_SPACE)
                dateclick.send_keys(dt_lst[dt])
                dateclick.send_keys(Keys.RETURN)

                for p1 in p_1:
                    search = driver.find_element_by_xpath(p1)
                    print(search.text)
                    p_1.remove(p1)
                    search.send_keys(Keys.RETURN)
                    for p2 in p_2:
                        try:
                            link_text = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH, p2)))
                            print(link_text.text)
                            mar = link_text.text
                            link_text.send_keys(Keys.RETURN)

                            soup_level3 = BeautifulSoup(driver.page_source, 'lxml')
                            table = soup_level3.find("table",{"id":"ContentPlaceHolder1_grdDaily"})
                            table_rows = table.find_all('tr')

                            test_list = []
                            for tr in table_rows:
                                td = tr.find_all('td')
                                row = [tr.text for tr in td]
                                test_list.append(row)

                            column_list = []
                            for tr in table_rows:
                                th = tr.find_all('th')
                                col = [tr.text for tr in th]
                                column_list.append(col)


                            test_df = pd.DataFrame(test_list)
                            test_df.columns = column_list[0]
                            test_df = test_df.apply(lambda line: line.str.strip().replace('\n', ''))
                            df = test_df
                            df['Market_Name'] = mar
                            df['Date'] = date_td
                            df_list.append(df)
                            print(len(df_list))
                            driver.back() # back from page 3 to 2 for all three market

                        except Exception as e:
                            #print(e)
                            driver.quit()
            except:
                pass
except Exception as e:
    print(e)
#print(len(df_list))


# In[ ]:


#print(len(df_list))
list_f = []
for t in range(len(df_list)):
    temp = "f" + str(t)
    list_f.append(temp)
    

final_df = df_list[0]
for u in range(1,len(df_list)):
    l = list_f[u]
    l = df_list[u]
    final_df = pd.concat([final_df,l])
#print(final_df)
try:
    final_df.drop(['Market Code', 'Market'], axis=1, inplace=True)
except:
    pass
final_df.dropna(inplace=True)
final_df = final_df[final_df['Commission Agent'] != '']
final_df.to_csv(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\generatedCSV\d11_06_21_notMapped.csv", index=False)


# ## Mapping Mandal and Village

# In[ ]:


#final_df = pd.read_csv(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\generatedCSV\d11_06_21_notMapped.csv")
final_df['Mandal'] = final_df['Mandal'].str.upper()
final_df['Village'] = final_df['Village'].str.upper()
final_df['Commodity'] = final_df['Commodity'].str.upper()
final_df['Vehicle No'] = final_df['Vehicle No'].str.upper()
final_df['Market_Name'] = final_df['Market_Name'].str.upper()
#final_df.head()


# In[ ]:


final_df.columns


# In[ ]:


final_df.columns = ['No', 'Commission_Agent', 'Farmer_Name', 'Quantity', 'Units', 'Mandal','Village', 'Vehicle', 'Vehicle_No', 'Commodity', 'Lot_Number', 'Market_Name', 'Date']


# In[ ]:


df_district = pd.read_excel(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\Gramoday_Datasets_market.xlsx", sheet_name=1, usecols=['name','loclevel3','loclevel2'])
df_district['name'] = df_district['name'].str.upper()
#df_district.head()

dict_3_to_2 = dict(zip(df_district['loclevel3'].tolist(), df_district['loclevel2'].tolist()))
#dict_3_to_2
dict_3_to_district = dict(zip(df_district['loclevel3'].tolist(), df_district['name'].tolist()))
#dict_3_to_district
dict_district_to_3 = dict(zip(df_district['name'].tolist(), df_district['loclevel3'].tolist()))
#dict_district_to_3


# In[ ]:


df_dist_to_dist = pd.read_excel(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\Gramoday_Datasets_market.xlsx", sheet_name=7)
df_dist_to_dist['name1'] = df_dist_to_dist['name1'].str.upper()
df_dist_to_dist['name2'] = df_dist_to_dist['name2'].str.upper()
#df_dist_to_dist.head()

dict_dist_to_dist = dict(zip(df_dist_to_dist['name1'].tolist(), df_dist_to_dist['name2'].tolist()))
#print(dict_dist_to_dist)


# In[ ]:


df_state = pd.read_excel(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\Gramoday_Datasets_market.xlsx", sheet_name=2)
df_state['name'] = df_state['name'].str.upper()
#df_state

dict_id_to_state = dict(zip(df_state['ID'].tolist(), df_state['name'].tolist()))
#print(dict_id_to_state)
dict_id_to_shortname = dict(zip(df_state['ID'].tolist(), df_state['shortName'].tolist()))
#print(dict_id_to_shortname)
dict_shortname_to_state = dict(zip(df_state['shortName'].tolist(), df_state['name'].tolist()))
#print(dict_shortname_to_state)


# In[ ]:


df_state_to_state = pd.read_excel(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\Gramoday_Datasets_market.xlsx", sheet_name=6)
df_state_to_state['name1'] = df_state_to_state['name1'].str.upper()
df_state_to_state['name2'] = df_state_to_state['name2'].str.upper()
#df_state_to_state.head()

dict_state_to_state = dict(zip(df_state_to_state['name1'].tolist(), df_state_to_state['name2'].tolist()))
#print(dict_state_to_state)


# In[ ]:


df_mandal = pd.read_excel(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\Gramoday_Datasets_market.xlsx", sheet_name=4, usecols=['name', 'loclevel3'])
df_mandal['name'] = df_mandal['name'].str.upper()
#print(df_mandal.head())

dict_mandal_to_3 = dict(zip(df_mandal['name'].tolist(), df_mandal['loclevel3'].tolist()))
#print(dict_mandal_to_3)


# In[ ]:


final_df['DistrictID_M'] = final_df.Mandal.map(dict_mandal_to_3)
final_df['DistrictM'] = final_df.DistrictID_M.map(dict_3_to_district)
final_df['ID2_3'] = final_df.DistrictID_M.map(dict_3_to_2)
final_df['StateM'] = final_df.ID2_3.map(dict_id_to_state)
#final_df['State_short_nameM'] = final_df.ID2_3.map(dict_id_to_shortname)

final_df.drop(['DistrictID_M', 'ID2_3'], axis=1, inplace=True)
#final_df.head()


# In[ ]:


final_df['DistrictID_V'] = final_df.Village.map(dict_mandal_to_3)
final_df['DistrictV'] = final_df.DistrictID_V.map(dict_3_to_district)
final_df['ID2_3_V'] = final_df.DistrictID_V.map(dict_3_to_2)
final_df['StateV'] = final_df.ID2_3_V.map(dict_id_to_state)
#final_df['State_short_nameV'] = final_df.ID2_3_V.map(dict_id_to_shortname)

final_df.drop(['DistrictID_V', 'ID2_3_V'], axis=1, inplace=True)
#final_df.tail(20)


# In[ ]:


final_df['State1'] = final_df.Village.map(dict_shortname_to_state)
final_df['State2'] = final_df.Village.map(dict_state_to_state)
final_df['District1'] = final_df.Village.map(dict_dist_to_dist)
final_df['level3'] = final_df.District1.map(dict_district_to_3)
final_df['level2'] = final_df.level3.map(dict_3_to_2)
final_df['State3'] = final_df.level2.map(dict_id_to_state)

final_df.drop(['level3', 'level2'], axis=1, inplace=True)


# In[ ]:


final_df.tail(20)


# # Truck Number Mapping

# In[ ]:


df_truck_no = pd.read_excel(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\Gramoday_Datasets_market.xlsx", sheet_name=9, usecols=['Code', 'Jurisdiction', 'State'])
df_truck_no['Code'] = df_truck_no['Code'].str.upper()
#print(df_truck_no.head())
#final_df['Vehicle No'].head()


# In[ ]:


df_truck_no['Code'] = df_truck_no['Code'].str.replace( "[-]", "")
final_df['Vehicle_No'] = final_df['Vehicle_No'].apply(lambda x:x[0:4])

#print(final_df['Vehicle No'].head())
#print(df_truck_no['Code'].head())


# In[ ]:


dict_code_to_juri = dict(zip(df_truck_no['Code'].tolist(), df_truck_no['Jurisdiction'].tolist()))
#print(dict_code_to_juri)
dict_code_to_state = dict(zip(df_truck_no['Code'].tolist(), df_truck_no['State'].tolist()))
#print(dict_code_to_state)


# In[ ]:


final_df['Vehicle_Jurisdiction'] = final_df.Vehicle_No.map(dict_code_to_juri)
final_df['Vehicle_State'] = final_df.Vehicle_No.map(dict_code_to_state)


# In[ ]:


final_df.head()


# # Mapping Commodity

# In[ ]:


df_commodity = pd.read_excel(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\Gramoday_Datasets_market.xlsx", sheet_name=8, usecols=['Commodity', 'Changes', 'Category'])
df_commodity['Commodity'] = df_commodity['Commodity'].str.upper()
df_commodity['Changes'] = df_commodity['Changes'].str.upper()
df_commodity['Category'] = df_commodity['Category'].str.upper()
#print(df_commodity.head(20))

dict_commodity_to_changes = dict(zip(df_commodity['Commodity'].tolist(), df_commodity['Changes'].tolist()))
#print(dict_commodity_to_changes)
dict_changes_to_category = dict(zip(df_commodity['Changes'].tolist(), df_commodity['Category'].tolist()))
#print(dict_changes_to_category)


# In[ ]:


final_df['Commodity'] = final_df.Commodity.map(dict_commodity_to_changes)
final_df['Category'] = final_df.Commodity.map(dict_changes_to_category)


# In[ ]:


final_df.head(10)


# In[ ]:


final_df.columns


# # Superimposing Columns

# In[ ]:

df = final_df[['StateM', 'StateV', 'State1', 'State2', 'State3', 'DistrictM', 'DistrictV', 'District1']]
#df = pd.read_csv(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\generatedCSV\d11_06_21_Mapped.csv", usecols=['StateM', 'StateV', 'State1', 'State2', 'State3', 'DistrictM', 'DistrictV', 'District1'])
df.replace(to_replace=np.nan, value='', inplace=True)
df.columns = ['DistrictA', 'StateA', 'DistrictB', 'StateB', 'StateC', 'StateD','DistrictC', 'StateE']
#print(df.head(10))

lst_index = []
lst_state = []
for index, row in df.iterrows():
    lst_state.append(row['StateA'] or row['StateB'] or row['StateC'] or row['StateD'] or row['StateE'])
    lst_index.append(index)
#print(len(lst_state))
df_state = pd.DataFrame()
df_state["State"] = lst_state
df_state["IndexNo"] = lst_index
#print(df_state.head(10))

lst_district = []
for index, row in df.iterrows():
    lst_district.append(row['DistrictA'] or row['DistrictB'] or row['DistrictC'])
#print(len(lst_district))
df_district = pd.DataFrame()
df_district["District"] = lst_district
df_district["IndexNo"] = lst_index
#print(df_district.head(10))

df["IndexNo"] = lst_index
final_df["IndexNo"] = lst_index
#print(df.head(10))

t1 = pd.merge(df_state,df_district,on='IndexNo')
#t2 = pd.merge(df,t1,on='IndexNo')
final_dataframe = pd.merge(final_df,t1,on='IndexNo')
final_dataframe.drop(['DistrictM', 'StateM', 'DistrictV', 'StateV', 'State1','State2', 'District1', 'State3', 'IndexNo'], axis=1,inplace=True)
#final_dataframe.head(10)


# In[ ]:


final_dataframe.to_csv(r"C:\Users\Sanjeeb\Desktop\Gramoday\2webScrap\generatedCSV\d11_06_21_Mapped.csv", index=False)


# In[ ]:




