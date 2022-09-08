#!/usr/bin/env python
# coding: utf-8

# # Gasspeicher-Daten für Deutschland und Frankreich

# In[1]:


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


# In[7]:


def data_finalizer(country_code):

    df = data_preparator(country_code)

    df['gasDayStart'] = pd.to_datetime(df['gasDayStart'])
    df['year'] = df['gasDayStart'].dt.year
    df['date_datawrapper'] = df['gasDayStart'].dt.strftime('%m-%d')
    df['date_datawrapper'] = str('2022-') + df['date_datawrapper']
    df.sort_values(by='gasDayStart', inplace=True)
    df['full'] = df['full'].astype(float)
    df.reset_index(drop=True, inplace=True)

    df['full'] = df['full'].astype(float)
    
    #Zur Kontrolle, ob die Werte nicht zu stark vom Vortag abweichen
    
    df['full_control_diff'] = df['full'].diff()
    
    max_diff = df['full_control_diff'].max() * 1.25
    min_diff = df['full_control_diff'].min() * 1.5
    
    last_diff = df['full_control_diff'].tail(1).values[0]

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
        
    return df_end, max_diff, min_diff, last_diff


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
        'metadata': {'annotate': {'notes': f'Wird täglich aktualisiert. Datenstand: {last_updated}.'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=datawrapper_headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=datawrapper_headers)


# **Skript starten**

# Die Grafik wird nur aktualisiert, wenn der letzte Wert nicht grösser ist als das Maximum der ganzen Reihe resp. nicht kleiner als das doppelte Minimum der Reihe. Damit soll verhindert werden, dass falsche Daten in die Grafik drücken.

# In[ ]:


for country, chart_id in chart_ids.items():
    
    df_export, max_diff, min_diff, last_diff = data_finalizer(country)
    
    last_updated = df_export[df_export['2022'].notna()]['date'].tail(1).values[0]
    last_updated = pd.to_datetime(last_updated).strftime('%-d. %B %Y')
    
    latest_value = df_export[df_export['2022'].notna()]['2022'].tail(1).values[0].round(1)
    latest_value = str(latest_value).replace('.', ',')
    
    if country == 'de':
        chart_title = f'Füllstand der deutschen Gasspeicher: <u>{latest_value} Prozent</u> 🇩🇪'
    elif country == 'fr':
        chart_title = f'Füllstand der französischen Gasspeicher: <u>{latest_value} Prozent</u> 🇫🇷'
        
    #Export
    if max_diff > last_diff and min_diff < last_diff:
        
        #Backup
        df_export.to_csv(f'/root/energiemonitor/backups/gas/gas_speicher_{country}_{backdate(0)}.csv', index=False)

        #Data
        df_export.to_csv(f'/root/energiemonitor/data/gas/gas_speicher_{country}.csv', index=False)
        
        chart_updater(chart_id, chart_title, last_updated)
    else:
        print('Die aktuellen Daten scheinen fehlerhaft zu sein.')

