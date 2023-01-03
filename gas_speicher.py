#!/usr/bin/env python
# coding: utf-8

# # Gasspeicher-Daten für Deutschland und Frankreich

# In[3]:


import requests
import pandas as pd
from time import sleep
from energy_settings import (
    api_key_agsi,
    backdate,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers,
    curr_year
)
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')


# In[4]:


headers = {'x-key': api_key_agsi}


# In[5]:


countries = ['de', 'fr']


# In[6]:


url = 'https://agsi.gie.eu/api?type=eu&country={}&size=300&page={}'


# In[7]:


def data_requester(country, num):
    r = requests.get(url.format(country, str(num)), headers=headers)
    
    if r.status_code == 200:
        return r.json()
    else:
        return f'Fehler beim request: {r.status_code}'


# In[8]:


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


# In[9]:


def data_finalizer(country_code):

    df = data_preparator(country_code)

    df['gasDayStart'] = pd.to_datetime(df['gasDayStart'])
    df['year'] = df['gasDayStart'].dt.year
    df['date_datawrapper'] = df['gasDayStart'].dt.strftime('%m-%d')
    df['date_datawrapper'] = str(curr_year) + '-' + df['date_datawrapper']
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

    #Ein Dataframe für die Jahre seit 2022
    df_curr = df[df['year'] >= 2022].pivot(index='date_datawrapper', columns='year', values='full')

    df_mean.rename(columns={'full': 'Mittelwert'}, inplace=True)
    df_max.rename(columns={'full': 'Maximum'}, inplace=True)
    df_min.rename(columns={'full': 'Minimum'}, inplace=True)
    
    df_end = df_curr.join([df_mean, df_max, df_min], how='outer')

    df_end.reset_index(inplace=True)

    df_end = df_end[df_end['date_datawrapper'] != f'{str(curr_year)}-02-29'].copy()

    all_columns = ['date_datawrapper']

    curr_columns = df_curr.columns.tolist()
    curr_columns.sort(reverse=True)
    all_columns.extend(curr_columns)
    all_columns.extend(['Mittelwert', 'Maximum', 'Minimum'])

    df_end = df_end[all_columns].copy()

    return df_end, max_diff, min_diff, last_diff


# **Datawrapper-Update**

# In[ ]:


chart_ids = {
    'de': 'i4ogB',
    'fr': 'uqAd8'
}


# In[ ]:


def chart_updater(chart_id, title, note):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

        'title': title,
        'metadata': {'annotate': {'notes': note}}

    }

    res_update = requests.patch(url_update, json=payload, headers=datawrapper_headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=datawrapper_headers)


# **Skript starten**

# Die Grafik wird nur aktualisiert, wenn der letzte Wert nicht grösser ist als das Maximum der ganzen Reihe resp. nicht kleiner als das doppelte Minimum der Reihe. Damit soll verhindert werden, dass falsche Daten in die Grafik drücken.

# In[ ]:


for country, chart_id in chart_ids.items():
    
    df_export, max_diff, min_diff, last_diff = data_finalizer(country)
    
    last_updated = df_export[df_export[curr_year].notna()]['date_datawrapper'].tail(1).values[0]
    last_updated = pd.to_datetime(last_updated).strftime('%-d. %B %Y')
    
    note = f'''    Mittelwert, Maximum und Minimum der Jahre 2011 bis 2021.     Wird täglich aktualisiert. Datenstand: {last_updated}.    '''
    
    latest_value = df_export[df_export[curr_year].notna()][curr_year].tail(1).values[0].round(1)
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
        
        chart_updater(chart_id, chart_title, note)
    else:
        print('Die aktuellen Daten scheinen fehlerhaft zu sein.')

