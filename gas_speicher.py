#!/usr/bin/env python
# coding: utf-8

# # Gasspeicher-Daten für Deutschland und Frankreich

# In[11]:


import requests
import pandas as pd
from time import sleep
from energy_settings import (
    api_key_agsi,
    backdate,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers
)


# In[2]:


headers = {'x-key': api_key_agsi}


# In[3]:


countries = ['de', 'fr']


# In[4]:


url = 'https://agsi.gie.eu/api?type=eu&country={}&size=300&page={}'


# In[5]:


def data_requester(country, num):
    r = requests.get(url.format(country, str(num)), headers=headers)
    
    if r.status_code == 200:
        return r.json()
    else:
        return f'Fehler beim request: {r.status_code}'


# In[6]:


def data_preparator(country_code):
    
    #Angaben zur Anzahl Seiten abholen
    r_check = data_requester(country_code, 1)
    
    num_of_pages = r_check['last_page']
    
    df_country = pd.DataFrame()

    for i in range(1, num_of_pages+1):
        res = data_requester(country_code, i)
        df_res = pd.DataFrame(res['data'])
        df_country = pd.concat([df_country, df_res])

        sleep(3)
        
    return df_country


# Beim Erschliessen der API hat (2.9.22) hat es einen Fehler in den Daten. Gemäss Datensatz liegt der Füllstand in Deutschland am 20. August 2022 bei 100 Prozent, am Tag vorher und nachher aber nur bei 78.9 resp. 80.2 Prozent. Bis eine Antwort der Datenherrin vorliegt, arbeiten wir mit dem Mittelwert der beiden umliegenden Tage weiter.

# In[7]:


def data_finalizer(country_code):

    df = data_preparator(country_code)

    df['gasDayStart'] = pd.to_datetime(df['gasDayStart'])
    df['year'] = df['gasDayStart'].dt.year
    df['date_datawrapper'] = df['gasDayStart'].dt.strftime('%m-%d')
    df['date_datawrapper'] = str('2022-') + df['date_datawrapper']

    if country_code == 'de':
        df.loc[(df['gasDayStart'] == '2022-08-20') & (df['name'] == 'Germany'), 'full'] = 79.55

    df['full'] = df['full'].astype(float)

    df_mean = df[df['gasDayStart'] <= '2021-12-31'].groupby('date_datawrapper')['full'].mean().to_frame()
    df_max = df[df['gasDayStart'] <= '2021-12-31'].groupby('date_datawrapper')['full'].max().to_frame()
    df_min = df[df['gasDayStart'] <= '2021-12-31'].groupby('date_datawrapper')['full'].min().to_frame()

    df22 = df[df['gasDayStart'] >= '2022-01-01'].copy()

    df_mean.rename(columns={'full': 'Mittelwert'}, inplace=True)
    df_max.rename(columns={'full': 'Maximum'}, inplace=True)
    df_min.rename(columns={'full': 'Minimum'}, inplace=True)
    df22.rename(columns={'full': '2022'}, inplace=True)

    df22.set_index('date_datawrapper', inplace=True)
    df22.sort_index(inplace=True)

    df_end = df22[['2022']].join([df_mean, df_max, df_min], how='outer')

    df_end.sort_index(inplace=True)
    df_end.reset_index(inplace=True)
    
    df_end.columns = ['date', '2022', 'Mittelwert', 'Maximum', 'Minimum']
   
    df_end = df_end[df_end['date'] != '2022-02-29'].copy()
    
    return df_end


# **Datawrapper-Update**

# In[8]:


chart_ids = {
    'de': 'i4ogB',
    'fr': 'uqAd8'
}


# In[9]:


def chart_updater(chart_id, title, last_updated):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

        'title': title,
        'metadata': {'annotate': {'notes': f'Datenstand: {last_updated}.'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=datawrapper_headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=datawrapper_headers)


# **Skript starten**

# In[10]:


for country, chart_id in chart_ids.items():
    
    df_export = data_finalizer(country)
    
    last_updated = df_export[df_export['2022'].notna()]['date'].tail(1).values[0]
    last_updated = pd.to_datetime(last_updated).strftime('%d. %B %Y')
    
    latest_value = df_export[df_export['2022'].notna()]['2022'].tail(1).values[0].round(1)
    latest_value = str(latest_value).replace('.', ',')
    
    if country == 'de':
        chart_title = f'Füllstand der deutschen Gasspeicher: <u>{latest_value} Prozent</u> 🇩🇪'
    elif country == 'fr':
        chart_title = f'Füllstand der französischen Gasspeicher: <u>{latest_value} Prozent</u> 🇫🇷'
    
    
    #Export
    #Backup
    df_export.to_csv(f'/root/energiemonitor/backups/gas/gas_speicher_{country}_{backdate(0)}.csv', index=False)
    
    #Data
    df_export.to_csv(f'/root/energiemonitor/data/gas/gas_speicher_{country}_{backdate(0)}.csv', index=False)
    
    chart_updater(chart_id, chart_title, last_updated)

