#!/usr/bin/env python
# coding: utf-8

# # Börsenstrompreis Schweiz

# Grafik: https://app.datawrapper.de/chart/4z3Vb/publish

# In[1]:


import requests
import pandas as pd
from datetime import datetime
from time import sleep
from energy_settings import backdate, datawrapper_api_key, datawrapper_url, datawrapper_headers


# Die Datumsangaben kommen im Unixformat. Sie werden in dieser Funktion umgewandelt.

# In[2]:


def date_formatter(elem):
    date_time_raw = datetime.fromtimestamp(elem/1000)
    date_time = date_time_raw.strftime('%Y-%m-%d %H:%M')
    
    return date_time


# Die Daten werden für jedes Jahr separat beantragt. Für das Jahr 2022 liegen die Daten im Listenelement [4], für 2021 im [5]. Zuletzt werden die Datumsgangaben mit den Werten zusammengefügt und ausgegeben. Ob die werte den richtigen Daten zugeordnet werden, wurde zu Beginn stichprobenmässig überprüft.

# In[3]:


def data_requester(year):
    url = f'https://energy-charts.info/charts/price_spot_market/data/ch/year_{year}.json'
    
    r = requests.get(url)
    
    if r.status_code == 200:
        res = r.json()
    else:
        print(f'Status Code: {str(r.status_code)}' + r.text)
        
    general_data = res[0]
    
    dates = general_data['xAxisValues']
    values_import_saldo = general_data['data']
    
    if year == 2022:
        day_ahead = res[4]
    else:
        day_ahead = res[5]
    
    values_day_ahead = day_ahead['data']
    
    full_data = []

    for date, value_i, values_d in zip(dates, values_import_saldo, values_day_ahead):
        mini_dict = {'date_unix': date, 'value_import_saldo': value_i, 'values_day_ahead': values_d}
        full_data.append(mini_dict)
        
    return full_data


# **Funktion ausführen**
# 
# Für jedes gewünschte Jahr wird die Funktion angeworfen und die Daten dann in df vereint.

# In[4]:


df = pd.DataFrame()

for i in range(2021, 2023):
    raw_data = data_requester(i)
    
    df_temp = pd.DataFrame(raw_data)
    df = pd.concat([df, df_temp])
    
    sleep(3)


# Formatieren

# In[5]:


df['date_time'] = df['date_unix'].apply(date_formatter)

df = df[['date_time', 'values_day_ahead']].copy()

df['date_time'] = pd.to_datetime(df['date_time'])
df['date_only'] = df['date_time'].dt.date


# Für jeden Tag den Durchschnitt errechnen

# In[6]:


df_final = df.groupby('date_only')['values_day_ahead'].mean().to_frame()


# Export

# In[ ]:


#Backup
df_final.to_csv(f'/root/energiemonitor/backups/strom/preis_spotmarkt_{backdate(0)}.csv')

#Data
df_final.to_csv('/root/energiemonitor/data/strom/preis_spotmarkt.csv')


# **Datawrapper-Update**

# In[14]:


last_updated = df_final.index[-1]
last_updated = last_updated.strftime('%d. %B %Y')


# In[15]:


chart_id = '4z3Vb'


# In[20]:


def chart_updater(chart_id, last_updated):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': f'Aktualisiert am {last_updated}.'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=datawrapper_headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=datawrapper_headers)


# In[21]:


chart_updater(chart_id, last_updated)

