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
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')


# In[2]:


today = datetime.today().strftime('%Y-%m-%d')


# **Daten-Download**

# In[3]:


url = 'https://www.bruegel.org/sites/default/files/2022-09/Gas_tracker_update.zip'

r = requests.get(url, stream=True)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall(f'Rohdaten/Bruegel/{today}')


# In[4]:


df = pd.read_csv(f'Rohdaten/Bruegel/2022-09-28/country_data_2022-09-27.csv')


# Formatieren

# In[5]:


df = df.iloc[1:].copy()


# Komma durch Punkt ersetzen

# In[6]:


df = df.replace(',', '', regex=True)


# Für jedes Lieferantenland ein eigenes df erstellen

# In[7]:


df_eu = df[['week', 'EU_2022', 'EU_2021', 'EU_max', 'EU_min']].copy()
df_rus = df[['week', 'Russia_2022', 'Russia_2021', 'Russia_max', 'Russia_min']].copy()
df_lng = df[['week', 'LNG_2022', 'LNG_2021', 'LNG_max', 'LNG_min']].copy()
df_nor = df[['week', 'Norway_2022', 'Norway_2021', 'Norway_max', 'Norway_min']].copy()
df_alg = df[['week', 'Algeria_2022', 'Algeria_2021', 'Algeria_max', 'Algeria_min']].copy()


# Spalten umbennen

# In[8]:


df_eu.rename(columns={
    'week': 'Woche',
    'EU_min': 'Minimum',
    'EU_max': 'Maximum',
    'EU_2021': '2021',
    'EU_2022': '2022'
}, inplace=True)

df_rus.rename(columns={
    'week': 'Woche',
    'Russia_min': 'Minimum',
    'Russia_max': 'Maximum',
    'Russia_2021': '2021',
    'Russia_2022': '2022'
}, inplace=True)

df_lng.rename(columns={
    'week': 'Woche',
    'LNG_min': 'Minimum',
    'LNG_max': 'Maximum',
    'LNG_2021': '2021',
    'LNG_2022': '2022'
}, inplace=True)

df_nor.rename(columns={
    'week': 'Woche',
    'Norway_min': 'Minimum',
    'Norway_max': 'Maximum',
    'Norway_2021': '2021',
    'Norway_2022': '2022'
}, inplace=True)

df_alg.rename(columns={
    'week': 'Woche',
    'Algeria_min': 'Minimum',
    'Algeria_max': 'Maximum',
    'Algeria_2021': '2021',
    'Algeria_2022': '2022'
}, inplace=True)


# Exportieren

# In[9]:


#Data
df_eu.to_csv('Results/importe_eu_total.csv', index=False)
df_rus.to_csv('Results/importe_russland.csv', index=False)
df_lng.to_csv('Results/importe_lng.csv', index=False)
df_nor.to_csv('Results/importe_norwegen.csv', index=False)
df_alg.to_csv('Results/importe_algerien.csv', index=False)


# In[10]:


#Backups
#df_eu.to_csv(f'/root/energiemonitor/backups/gas/importe_eu_total_{backdate(0)}.csv', index=False)
#df_rus.to_csv(f'/root/energiemonitor/backups/gas/importe_russland_{backdate(0)}.csv', index=False)
#df_lng.to_csv(f'/root/energiemonitor/backups/gas/importe_lng_{backdate(0)}.csv', index=False)
#df_nor.to_csv(f'/root/energiemonitor/backups/gas/importe_norwegen_{backdate(0)}.csv', index=False)
#df_alg.to_csv(f'/root/energiemonitor/backups/gas/importe_algerien_{backdate(0)}.csv', index=False)

#Data
#df_eu.to_csv('/root/energiemonitor/data/gas/importe_eu_total.csv', index=False)
#df_rus.to_csv('/root/energiemonitor/data/gas/importe_russland.csv', index=False)
#df_lng.to_csv('/root/energiemonitor/data/gas/importe_lng.csv', index=False)
#df_nor.to_csv('/root/energiemonitor/data/gas/importe_norwegen.csv', index=False)
#df_alg.to_csv('/root/energiemonitor/data/gas/importe_algerien.csv', index=False)


# **Datawrapper-Update**

# In[11]:


chart_ids = {
    'EU total': '4HTS3',
    'Russland': 'o4qLp',
    'Norwegen': 'Y83VN',
    'Flüssigerdgas': '3FrFt',
    'Algerien': 'm9zNp'
}


# Angaben zu letzter Aktualisierung

# In[12]:


last_updated = datetime.today()


# Um zu prüfen, ob die Daten tatsächlich aktualisiert wurden, wird das Datum vom Montag der Kalenderwoche eruiert, die den aktuellsten Wert im Datensatz hat.

# In[13]:


last_week = df_eu[df_eu['2022'].notna()]['Woche'].tail(1).values[0]


# In[14]:


year_week = str(datetime.today().year) + f'-W{last_week}'
monday_of_last_week = datetime.strptime(year_week + '-1', "%Y-W%W-%w")


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


# Die Funktion wird nur ausgeführt, wenn heute minus 16 Tage vor dem Montag der aktuellsten Kalenderwoche liegt. Der Datensatz wird einmal wöchentlich aktualisiert, am Dienstag. Der Montag der letzten Kalenderwoche kann also 15 Tage zurück liegen und die Daten sind dennoch aktuell.

# In[16]:


for country, chart_id in chart_ids.items():
    if last_updated - timedelta(days=16) <= monday_of_last_week:
        
        last_updated_str = last_updated.strftime('%-d. %B %Y')
        
        note = f'Minimum und Maximum beziehen sich auf den Zeitraum 2015 bis 2020. Wird wöchentlich aktualisiert, zuletzt am {last_updated_str}.'
        
        chart_updater(chart_id, note)

