#!/usr/bin/env python
# coding: utf-8

# In[4]:


import requests
import pandas as pd
from datetime import datetime
from time import sleep
from energy_settings import (
    backdate,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers,
    curr_year
)


# **Daten-Import** die url muss jeden Monat angepasst werden!

# In[10]:


url = 'https://dam-api.bfs.admin.ch/hub/api/dam/assets/23344572/master'

df_import = pd.read_excel(url, sheet_name='Monat - Mois')


# Formatieren

# In[11]:


df_import.columns = df_import.iloc[2].values

df_import.columns.values[0] = 'date'
df_import.columns.values[15] = 'Heizöl'


# Nur mit notwendigen Daten weiterfahren

# In[12]:


df = df_import[['date', 'Heizöl', 'Holz / Bois', 'Treibstoff / Carburants']].copy()


# Die Spalten mit den unteren Zeilen umbenennen

# In[13]:


col_names = []

for x, y in zip(df.iloc[3].values, df.iloc[4].values):
    col_name = '_'.join([str(x), str(y)])
    col_names.append(col_name)
    
df.columns = col_names

df.rename(columns={
    'Monat / Mois_nan': 'date',
    'Bezugsmenge / Quantité \n3\'001 - 6\'000 l_100 l': 'Heizöl',
    'Holzpellets / Pellets_6\'000 kg': 'Holzpellets',
    'Bleifrei 95 / sans plomb 95_1 l': 'Benzin'
}, inplace=True)


# Formatieren

# In[15]:


df = df.loc[5:].copy()

df = df[df['Heizöl'].notna()].copy()

df['date'] = pd.to_datetime(df['date'])


# Wir behalten nur die Daten seit Januar 2021

# In[13]:


df = df[df['date'] >= '2021-01-01'].copy()


# **Datawrapper-Update**

# In[14]:


chart_ids = {
    'Heizöl': {
        'chart_id': 'm2tee',
        'intro': 'Durchschnittspreis in Franken pro 100 Liter bei einer Bezugsmenge von 3001 bis 6000 Litern.'
    },
    'Holzpellets': {
        'chart_id': 'gulob',
        'intro': 'Durchschnittspreis in Franken pro 6000 Kilogramm Holz.'
    },
    'Benzin': {
        'chart_id': '7sDvn',
        'intro': 'Durchschnittspreis in Franken pro Liter Bleifrei 95.'
    }
}


# Daten in die Grafik laden

# In[15]:


def data_uploader(chart_id, df_func):
    dw_upload_url = datawrapper_url + chart_id +'/data'

    headers = {
        'Accept': '*/*',
        'Content-Type': 'text/csv',
        'Authorization': datawrapper_headers['Authorization']
    }
    
    #data is being transformed to a csv
    data = df_func.to_csv(index=False, encoding='utf-8')

    response = requests.put(dw_upload_url, data=data.encode('utf-8'), headers=datawrapper_headers)

    status_code = response.status_code
    if status_code > 204:
        print(chart_id + ': ' + str(status_code))
    
    sleep(3)
    
    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    res_publish = requests.post(url_publish, headers=datawrapper_headers)
    
    status_code2 = res_publish.status_code
    if status_code2 > 204:
        print(chart_id + ': ' + str(status_code2))


# Grafik-Beschreibung updaten

# In[16]:


date_today = datetime.today()
last_updated = date_today.strftime('%-d. %B %Y')
last_month = df['date'].tail(1).dt.month.values[0]


# Wenn der Dezember der letzte Monat war, müssen wir vom aktuellen Jahr eins subtrahieren

# In[17]:


if last_month == 12:
    curr_year -= 1
else:
    pass


# In[18]:


tick_string = f'2021-01-01, 2021-12-01, {curr_year}-{last_month}-01'


# In[19]:


def chart_updater(chart_id, tick_string, intro, last_updated):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {
                'visualize': {'custom-ticks-x': tick_string},
                'describe': {'intro': intro},
                'annotate': {'notes': f'Wird monatlich aktualisiert, zuletzt am {last_updated}.'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=datawrapper_headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=datawrapper_headers)


# **Die Funktionen starten**

# In[20]:


for carrier, chart_info in chart_ids.items():
    chart_id = chart_info['chart_id']
    intro = chart_info['intro']
    
    data_uploader(chart_id, df[['date', carrier]])
    
    sleep(2)
    
    chart_updater(chart_id, tick_string, intro, last_updated)

