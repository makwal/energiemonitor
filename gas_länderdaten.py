#!/usr/bin/env python
# coding: utf-8

# # Gasimport aus diversen Ländern

# In[1]:


import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
from energy_settings import (
    backdate,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers
)
import dw
import locale
from bs4 import BeautifulSoup
import os
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')


# In[2]:


today = datetime.today().strftime('%Y-%m-%d')


# **Daten-Download**

# Aktuelle url scrapen

# In[3]:


res = requests.get('https://www.bruegel.org/dataset/european-natural-gas-imports')

soup = BeautifulSoup(res.text, 'html.parser')

download_url = 'https://www.bruegel.org' + soup.find('a', title='Download data')['href']


# Aktuelle url laden und Abgleich mit heruntergeladener url machen. Wenn heruntergeladene neu ist, als aktuelle speichern.

# In[7]:


with open('/root/energiemonitor/current_bruegel_url.txt', 'r') as file:
    current_url = file.read()


# In[9]:


if download_url != current_url:
    
    current_url = download_url
    
    with open('/root/energiemonitor/current_bruegel_url.txt', 'w') as file:
        file.write(download_url)


# Daten herunterladen und einlesen

# In[11]:


r = requests.get(current_url, stream=True)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall(f'/root/energiemonitor/Rohdaten/Bruegel/{today}')


# In[15]:


for document in os.listdir(f'/root/energiemonitor/Rohdaten/Bruegel/{today}/'):
    if 'country_data' in document and 'xlsx' in document:
        current_document = document


# In[4]:


df = pd.read_excel(f'/root/energiemonitor/Rohdaten/Bruegel/{today}/{current_document}')


# Formatieren

# In[5]:


df = df.iloc[1:].copy()


# Komma durch Punkt ersetzen

# In[6]:


df = df.replace(',', '', regex=True)


# Für jedes Lieferantenland ein eigenes df erstellen

# In[7]:


df_eu = df[['week', 'EU_2023', 'EU_2022', 'EU_max', 'EU_min']].copy()
df_rus = df[['week', 'Russia_2023', 'Russia_2022', 'Russia_max', 'Russia_min']].copy()
df_lng = df[['week', 'LNG_2023', 'LNG_2022', 'LNG_max', 'LNG_min']].copy()
df_nor = df[['week', 'Norway_2023', 'Norway_2022', 'Norway_max', 'Norway_min']].copy()
df_alg = df[['week', 'Algeria_2023', 'Algeria_2022', 'Algeria_max', 'Algeria_min']].copy()


# Spalten umbennen

# In[8]:


df_eu.rename(columns={
    'week': 'Woche',
    'EU_min': 'Minimum',
    'EU_max': 'Maximum',
    'EU_2022': '2022',
    'EU_2023': '2023'
}, inplace=True)

df_rus.rename(columns={
    'week': 'Woche',
    'Russia_min': 'Minimum',
    'Russia_max': 'Maximum',
    'Russia_2022': '2022',
    'Russia_2023': '2023'
}, inplace=True)

df_lng.rename(columns={
    'week': 'Woche',
    'LNG_min': 'Minimum',
    'LNG_max': 'Maximum',
    'LNG_2022': '2022',
    'LNG_2023': '2023'
}, inplace=True)

df_nor.rename(columns={
    'week': 'Woche',
    'Norway_min': 'Minimum',
    'Norway_max': 'Maximum',
    'Norway_2022': '2022',
    'Norway_2023': '2023'
}, inplace=True)

df_alg.rename(columns={
    'week': 'Woche',
    'Algeria_min': 'Minimum',
    'Algeria_max': 'Maximum',
    'Algeria_2022': '2022',
    'Algeria_2023': '2023'
}, inplace=True)


# **Datawrapper-Update**

# In[10]:


chart_ids = {
    'EU total': {'data': df_eu, 'chart_id': '4HTS3'},
    'Russland': {'data': df_rus, 'chart_id': 'o4qLp'},
    'Norwegen': {'data': df_nor, 'chart_id': 'Y83VN'},
    'Flüssigerdgas': {'data': df_lng, 'chart_id': '3FrFt'},
    'Algerien': {'data': df_alg, 'chart_id': 'm9zNp'}
}


# **Daten-Upload**

# In[11]:


last_updated = datetime.today()

last_updated_str = last_updated.strftime('%-d. %B %Y')
        
note = f'Minimum und Maximum beziehen sich auf den Zeitraum 2015 bis 2020. Wird wöchentlich aktualisiert, zuletzt am {last_updated_str}.'

payload = {

    'metadata': {'annotate': {'notes': note}
                }

    }

for country in chart_ids.keys():
    
    temp_dict = chart_ids[country]
    df_temp = temp_dict['data']
    chart_id = temp_dict['chart_id']
    
    df_temp.set_index('Woche', inplace=True)
    
    dw.chart_filler(chart_id=chart_id, df=df_temp)
    
    dw.chart_updater(chart_id=chart_id, payload=payload)

