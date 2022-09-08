#!/usr/bin/env python
# coding: utf-8

# # Benzinpreisentwicklung anhand Landesindex der Konsumentenpreise

# In[1]:


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


# **Daten-Import**

# In[2]:


url = 'https://dam-api.bfs.admin.ch/hub/api/dam/assets/23344559/master'


# In[3]:


df_import = pd.read_excel(url, sheet_name='INDEX_m')
df_import.columns = df_import.iloc[2].values


# In[4]:


df = df_import[(df_import['Position_D'].notna()) & (df_import['Position_D'].str.contains('Benzin'))].copy()

df.drop(['Code',
 'PosNo',
 'PosType',
 'Level',
 'COICOP',
 'Position_D',
 'Position_F',
 'PosTxt_F',
 'Posizione_I',
 'PosTxt_I',
 'Item_E',
 'PosTxt_E',
 '2022'
 ], axis=1, inplace=True)

df = df.T.drop('PosTxt_D').reset_index().copy()

df.columns = ['Datum', 'Benzinpreis']


# Wir nehmen nur die Daten seit Dezember 2020 (weil das der Indexmonat des BFS ist)

# In[5]:


df = df[df['Datum'] >= '2020-12-01'].copy()


# **Datawrapper-Update**

# In[6]:


chart_id = 'mQ9yW'


# Daten in die Grafik laden

# In[7]:


def data_uploader(chart_id, df):
    dw_upload_url = datawrapper_url + chart_id +'/data'

    headers = {
        'Accept': '*/*',
        'Content-Type': 'text/csv',
        'Authorization': datawrapper_headers['Authorization']
    }
    
    #data is being transformed to a csv
    data = df.to_csv(index=False, encoding='utf-8')

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


# In[8]:


data_uploader(chart_id, df)


# Grafik-Beschreibung updaten

# Lesebeispiel

# In[16]:


df_last = df.tail(1).copy()
last_month_str = df_last['Datum'].dt.strftime('%B').values[0]
last_value = round(df_last['Benzinpreis'].values[0] / 100,1)
last_value = str(last_value).replace('.', ',')

if df_last['Benzinpreis'].values[0] >= 100:
    attr = 'höher'
else:
    attr = 'tiefer'
    
intro = f'indexiert, Dezember 2020 = 100. Lesebeispiel: Der Benzinpreis war im {last_month_str} {curr_year} rund {last_value} Mal {attr} als im Dezember 2020.'


# Ticks

# In[17]:


date_today = datetime.today()
last_updated = date_today.strftime('%-d. %B %Y')
last_month = df_last['Datum'].dt.month.values[0]


# In[18]:


if last_month == 12:
    curr_year -= 1
else:
    pass


# In[19]:


tick_string = f'2020-12-01, 2021-12-01, {curr_year}-{last_month}-01'


# In[21]:


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


# In[22]:


chart_updater(chart_id, tick_string, intro, last_updated)

