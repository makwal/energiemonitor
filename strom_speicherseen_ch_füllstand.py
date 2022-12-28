#!/usr/bin/env python
# coding: utf-8

# # Speichersee-Daten Schweiz: Füllstand

# In[1]:


import requests
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
from energy_settings import (
    backdate,
    curr_year,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers
)
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')


# **Daten-Import**

# In[2]:


url = 'https://www.uvek-gis.admin.ch/BFE/ogd/17/ogd17_fuellungsgrad_speicherseen.csv'


# In[3]:


df = pd.read_csv(url)


# **Datenvearbeitung**

# Füllstand in Prozent errechnen

# In[4]:


df['Füllstand total'] = (df['TotalCH_speicherinhalt_gwh'] / df['TotalCH_max_speicherinhalt_gwh']) * 100


# Kalenderwochen-Angaben eruieren.

# In[5]:


df['Datum'] = pd.to_datetime(df['Datum'])

df['Kalenderwoche'] = df['Datum'].dt.isocalendar().week


# Für jede Kalenderwoche den Minimal-, Maximal- und mittleren Wert berechnen.

# In[6]:


df_mean = df[df['Datum'] < '2022-01-01'].groupby('Kalenderwoche')['Füllstand total'].mean().to_frame()
df_max = df[df['Datum'] < '2022-01-01'].groupby('Kalenderwoche')['Füllstand total'].max().to_frame()
df_min = df[df['Datum'] < '2022-01-01'].groupby('Kalenderwoche')['Füllstand total'].min().to_frame()


# Ein df für die letzten beiden Jahre.

# In[7]:


df['year'] = df['Datum'].dt.year


# In[8]:


df_curr = df[df['year'] >= 2022].pivot(index='Kalenderwoche', columns='year', values='Füllstand total')


# Vorbereitung für nachfolgenden Join

# In[9]:


df_mean.rename(columns={'Füllstand total': 'Mittelwert'}, inplace=True)
df_max.rename(columns={'Füllstand total': 'Maximum'}, inplace=True)
df_min.rename(columns={'Füllstand total': 'Minimum'}, inplace=True)


# Alle Daten zusammenfügen. Wichtig: Standard-Join, nicht how=outer, damit alle Kalenderwochen genommen werden (nicht jene des angebrochenen Jahres).

# In[10]:


df_final = df_mean.join([df_max, df_min, df_curr])

curr_columns = df_curr.columns.tolist()
curr_columns.sort(reverse=True)
curr_columns.extend(['Mittelwert', 'Maximum', 'Minimum'])

df_final = df_final[curr_columns].copy()

df_final = df_final.loc[:52].copy()


# **Export**

# In[ ]:


#Backups
df_final.to_csv(f'/root/energiemonitor/backups/strom/speicherseen_ch_füllstand_{backdate(0)}.csv')

#Data
df_final.to_csv('/root/energiemonitor/data/strom/speicherseen_ch_füllstand.csv')


# **Datawrapper-Update**

# In[ ]:


last_updated = datetime.today().strftime('%-d. %B %Y')

latest_value = df_final[df_final['2022'].notna()]['2022'].tail(1).values[0]
latest_value = str(latest_value.round(1)).replace('.', ',')


# In[ ]:


chart_id = 'MXmZp'


# In[ ]:


title = f'Füllstand der Schweizer Speicherseen: <u>{latest_value} Prozent</u> 🇨🇭'


# In[ ]:


note = f'''Mittelwert, Maximum und Minimum der Jahre 2000 bis 2021. Wird wöchentlich aktualisiert, zuletzt am {last_updated}.'''


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


# In[ ]:


chart_updater(chart_id, title, note)

