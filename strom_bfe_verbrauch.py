#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from time import sleep
from datetime import datetime, timedelta
from energy_settings import (
    backdate,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers
)
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')


# **Landesverbrauch**

# In[2]:


data_url = 'https://energiedashboard.admin.ch/api/strom-verbrauch/historical-values'

r = requests.get(data_url)

r = r.json()


# In[3]:


df = pd.DataFrame(r['entries'])


# In[4]:


df['date'] = pd.to_datetime(df['date'])

df = df[['date', 'landesverbrauch', 'landesverbrauchGeschaetzt', 'landesverbrauchPrognose', 'fiveYearMittelwert', 'fiveYearMin', 'fiveYearMax']].copy()

df.columns = ['date', 'Verbrauch gemeldet', 'Verbrauch geschätzt', 'Prognose', 'Mittelwert', 'Minimum', 'Maximum']


# In[5]:


df = df[df['date'] >= '2022-09-01'].copy()


# In[6]:


last_updated = df[df['Verbrauch geschätzt'].notna()].tail(1)['date']
last_updated = (last_updated + timedelta(days=1)).dt.strftime('%d. %B %Y').values[0]


# **Endverbrauch**

# In[7]:


data_url_end = 'https://energiedashboard.admin.ch/api/strom-verbrauch/endverbrauch'

res = requests.get(data_url_end)

res = res.json()


# In[8]:


df_end = pd.DataFrame(res)


# In[9]:


df_end['date'] = pd.to_datetime(df_end['date'])


# In[10]:


df_end = df_end[['date', 'endverbrauch', 'prognoseMittelwert', 'fiveYearMittelwert', 'fiveYearMin', 'fiveYearMax']].copy()

df_end.columns = ['date', 'Endverbrauch gemeldet', 'Endverbrauch geschätzt', 'Mittelwert', 'Minimum', 'Maximum']


# In[11]:


df_end = df_end[df_end['date'] >= '2022-09-01'].copy()


# **Datawrapper-Grafiken updaten**

# In[12]:


anno = f'''Mittelwert, Mininum und Maximum beziehen sich auf die letzten fünf Jahre. Wird täglich aktualisiert, zuletzt am {last_updated}.
'''

anno_end = f'''Mittelwert, Mininum und Maximum beziehen sich auf die letzten fünf Jahre. An den Wochenenden wird weniger Strom verbraucht. Wird täglich aktualisiert, zuletzt am {last_updated}.
'''


# In[13]:


chart_ids = {
    'landesverbrauch': {
        'chart_id': 'vSb7c',
        'data': df,
        'annotation': anno
    },
    'endverbrauch': {
        'chart_id': '9b0i1',
        'data': df_end,
        'annotation': anno_end
    }
}


# Daten in Grafik laden

# In[14]:


def data_uploader(chart_id, df_func):
    dw_upload_url = datawrapper_url + chart_id +'/data'

    headers = {
        'Accept': '*/*',
        'Content-Type': 'text/csv',
        'Authorization': datawrapper_headers['Authorization']
    }
    
    #data is being transformed to a csv
    data = df_func.to_csv(encoding='utf-8', index=False)

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


# Grafik-Annotation updaten

# In[15]:


def chart_updater(chart_id, note):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': note}}

    }

    res_update = requests.patch(url_update, json=payload, headers=datawrapper_headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=datawrapper_headers)


# In[16]:


for usage, chart in chart_ids.items():
    chart_id = chart['chart_id']
    data = chart['data']
    anno = chart['annotation']
    
    data_uploader(chart_id, data)
    
    sleep(3)
    
    chart_updater(chart_id, anno)

