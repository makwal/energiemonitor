#!/usr/bin/env python
# coding: utf-8

# # Enstog-Daten zu Nordstream

# Grafik: https://app.datawrapper.de/chart/JkvZj/publish

# Hier holen wir die Durchfluss-Werte für die Pipeline Nord Stream 1 ab. Dazu schauen wir die Anlandestation Greifswald auf transparency.entsog.eu an. Wir holen die stündlichen Daten einmal für die letzten vier Wochen des laufenden Jahres und einmal für denselben Zeitraum vor einem Jahr ab. Anschliessend fügen wir die Daten zu einem Datensatz zusammen, der dann auf Datawrapper hochgeladen werden kann.

# In[1]:


import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from time import sleep
from energy_settings import (
    backdate,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers
)


# **Datumsangaben für url bereitmachen**
# 
# Die abzufragende url benötigt ein Start- und ein Enddatum. Wir bereiten die Daten für das laufende und das vergangene Jahr vor. Alle Datumsangaben werden in einem Dictionary gespeichert, über den später iteriert wird.

# In[2]:


date_today = datetime.today().date()
date_past = date_today - relativedelta(months=1)
date_from_curr = date_past.strftime('%Y-%m-%d')
date_to_curr = date_today.strftime('%Y-%m-%d')

date_past_prev = date_past - relativedelta(years=1)
date_today_prev = date_today - relativedelta(years=1)
date_from_prev = date_past_prev.strftime('%Y-%m-%d')
date_to_prev = date_today_prev.strftime('%Y-%m-%d')

date_dict = {
    'curr': [date_from_curr, date_to_curr],
    'prev': [date_from_prev, date_to_prev]
}


# **Daten-Abfrage**

# In[3]:


url = 'https://transparency.entsog.eu/api/v1/operationalData.csv?forceDownload=true&pointDirection=de-tso-0015itp-00250entry,de-tso-0005itp-00491entry,de-tso-0020itp-00454entry,de-tso-0016itp-00251entry,de-tso-0017itp-00247entry,de-tso-0018itp-00297entry&from={}&to={}&indicator=Physical%20Flow&periodType=hour&timezone=CET&limit=-1&dataset=1&directDownload=true'


# Die Funktion data_requester erhält eine Liste bestehend aus dem Start- und dem Enddatum. Beide Daten werden in die url eingefügt, welche dann direkt zur csv-Datei führt. Im DataFrame behalten wir nur die beiden relevanten Betreiber und summieren deren stündlichen Werte. Diese dividieren wir durch eine Million, um die Angaben in Gigawattstunden statt Kilowattstunden zu erhalten.

# In[4]:


def data_requester(date_list):
    url_temp = url.format(date_list[0], date_list[1])
    df_temp = pd.read_csv(url_temp)
    df_temp = df_temp[(df_temp['operatorLabel'] == 'OPAL Gastransport') | (df_temp['operatorLabel'] == 'NEL Gastransport')].copy()
    df_temp = (df_temp.groupby('periodTo')['value'].sum() / 10**6).to_frame()
    df_temp.reset_index(inplace=True)
    return df_temp


# **Daten-Verarbeitung**

# Durch Iteration über den Datums-Dictionary wird die Funktion aufgerufen und die Daten im Haupt-DataFrame "df" gespeichert.

# In[5]:


df = pd.DataFrame()

for frame, dates in date_dict.items():
    year = date_dict[frame][1].split('-')[0]
    df_single = data_requester(date_dict[frame])
    df = pd.concat([df, df_single])
        
    sleep(3)


# Der Datensatz wird so formatiert, dass er auf Datawrapper visualisiert werden kann. Die Spalte date_datawrapper dient Datawrapper als Datumsangabe auf der x-Achse. Damit Datawrapper die Werte als Daten erkennt, brauchen sie eine Jahresangabe (obwohl eigentlich nur Tag und Monat gebraucht werden). Darum wird dieser Spalte das Jahr 2022 eingefügt. Dies ist nur eine Helfersangabe für Datawrapper!

# In[6]:


df['periodTo'] = pd.to_datetime(df['periodTo'])
df['year'] = df['periodTo'].dt.year
df['date_datawrapper'] = df['periodTo'].dt.strftime('%m-%d %H:%M')
df['date_datawrapper'] = str('2022-') + df['date_datawrapper']
df = df.pivot(index='date_datawrapper', columns='year', values='value')


# **Export**

# In[7]:


df.to_csv('/root/energiemonitor/data/gas/nord_stream_stündlich.csv')


# **Datawrapper-Update**

# In[7]:


chart_id = 'JkvZj'


# In[8]:


last_updated = pd.to_datetime(df.index[-1]).strftime('%-d. %B %Y, %-H') + ' Uhr'


# In[10]:


ticks_start = df.index[0].split(' ')[0]
ticks_end = df.index[-1].split(' ')[0]


# In[11]:


tick_string = f'{ticks_start}, {ticks_end}'


# In[12]:


def chart_updater(chart_id, tick_string, last_updated):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {
        
        'metadata': {
                'visualize': {'custom-ticks-x': tick_string},
                'annotate': {'notes': f'Wird mehrmals täglich aktualisiert. Datenstand: {last_updated}.'}
        }

    }

    res_update = requests.patch(url_update, json=payload, headers=datawrapper_headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=datawrapper_headers)


# In[13]:


chart_updater(chart_id, tick_string, last_updated)

