#!/usr/bin/env python
# coding: utf-8

# - Gasdaten: https://opendata.swiss/de/dataset/energiedashboard-ch-tagliche-flusse-in-die-und-aus-der-schweiz-gas
# - Ampeldaten: https://opendata.swiss/de/dataset/energiedashboard-ch-energieversorgung-aktuelle-lage-ampelsystem
# - Wetterdaten: https://opendata.swiss/de/dataset/klimamessnetz-tageswerte

# In[1]:


import requests
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
import numpy as np
from energy_settings import (
    backdate,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers
)
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')


# Wir nehmen immer die Daten von vor drei Tagen resp. das 10-Tages-Mittel von einem Tag zuvor.

# In[2]:


three_days_raw = datetime.today() - timedelta(days=3)
three_days = three_days_raw.strftime('%Y-%m-%d')
three_days_str = three_days_raw.strftime('%-d. %B %Y')

four_days = (three_days_raw - timedelta(days=1)).strftime('%Y-%m-%d')


# **Datenimport Nettimport**

# In[7]:


data_url = 'https://energiedashboard.admin.ch/api/gas/gas-import/karte'

r = requests.get(data_url)

r = r.json()


# In[8]:


netto_import_ch = r['nettoImportCH']

trend_en = r['trend']


# In[9]:


def trend_setter(trend):
    if trend == 'up_mild':
        return 'leicht mehr als'
    elif trend == 'up_strong':
        return 'mehr als'
    elif trend == 'down_mild':
        return 'leicht weniger als'
    elif trend == 'down_strong':
        return 'weniger als'
    else:
        return ''


# In[10]:


trend_de = trend_setter(trend_en)


# In[11]:


if trend_de == '':
    raise ValueError('Unbekannter Trend, Übersetzung nicht möglich.')


# **Wetter**

# Wir lesen die Tageswerte von Meteo Schweiz ein. Als Referenzmessstation dient Zürich-Fluntern (SMA). Wir greifen das Tagesmittel ab, das sich in der Spalte tre200d0 befindet. Als Vergleichswert bilden wir das Mittel der vorherigen 10 Tage.

# In[12]:


weather_url = 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_SMA_current.csv'
df_weather = pd.read_csv(weather_url, delimiter=';')


# In[13]:


df_weather['date'] = pd.to_datetime(df_weather['date'], format='%Y%m%d')

df_weather['mean_10d'] = df_weather['tre200d0'].rolling(10).mean()


# In[14]:


temperature = df_weather[df_weather['date'] == three_days]['tre200d0'].values[0]

mean_temp_10d = df_weather[df_weather['date'] == four_days]['mean_10d'].values[0]


# Anhand dieser Funktion wird der textliche Vergleich der aktuellsten Temperatur mit dem 10-Tage-Mittel gemacht.

# In[15]:


def texter_temp(temperature, mean_temp_10d):
    diff = temperature - mean_temp_10d

    if diff >= 5:
        return 'deutlich höheren'
    elif diff >= 2:
        return 'höheren'
    elif diff >= 1:
        return 'leicht höheren'
    elif diff < 1 and diff > -1:
        return 'ungefähr gleichen'
    elif diff <= -5:
        return 'deutlich tieferen'
    elif diff <= -2:
        return 'tieferen'
    elif diff <= -1:
        return 'leicht tieferen'
    
est_temp = texter_temp(temperature, mean_temp_10d)


# Hier importieren wir die Ampelinformationen, Gefahrenstufe und dazugehöriger Text.

# In[16]:


ampel_url = 'https://bfe-energy-dashboard-ogd.s3.amazonaws.com/ogd108_stufen_energiemangellage.json'
r = requests.get(ampel_url)
r = r.json()

res = r['ampel_status_gas'][0]

level = res['level']
titel = res['titel@de']


# Hier werden die Texte zusammengesetzt und in ein DataFrame verpackt.

# In[17]:


icon_url = 'https://chm-editorial-data-static.s3.eu-west-1.amazonaws.com/red_mantel/energiedashboard/icons/erdgas.png'

text_net_import = f'<strong style="font-size:32px">{int(round(netto_import_ch))} GWh</strong>&nbsp <span style="letter-spacing:0.75px">ungefährer Wert</span>'
text_comparison = f'''So viel <b>Erdgas</b> importierte die Schweiz am {three_days_str} netto.<br><br> Das ist <b>{trend_de}</b> im Durchschnitt der vorherigen zehn Tage. Dies bei <strong>{est_temp}</strong> Temperaturen (Zürich).'''
text_bundesrat = f'Beurteilung Bundesrat: <strong>{titel}</strong> (Gefahrenstufe {level} von 5)'


# In[18]:


data = [
    {'icon': f'![]({icon_url})', 'text': text_net_import},
    {'icon': np.nan, 'text': text_comparison},
    {'icon': np.nan, 'text': text_bundesrat}
]


# In[19]:


df_final = pd.DataFrame(data)


# **Datawrapper-Update**

# In[20]:


chart_id = 'bRi1m'


# Daten in die Grafik laden

# In[21]:


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


# In[22]:


data_uploader(chart_id, df_final)


# **Definitionen**

# Vergleich Nettoimport:
# 
# Letzter Import...
# 
# - grösser gleich 15 Prozent grösser als mean_10d = "deutlich mehr"
# - grösser gleich 5 Prozent grösser als mean_10d = "mehr"
# - grösser gleich 2 Prozent grösser als mean_10d = "leicht mehr"
# - zwischen -2 und +2 Prozent von mean_10d = "im Bereich des Durchschnitts"
# - grösser gleich 2 Prozent kleiner als mean_10d = "leicht weniger"
# - grösser gleich 5 Prozent kleiner als mean_10d = "weniger"
# - grösser gleich 15 Prozent kleiner als mean_10d = "deutlich weniger"

# Vergleich Temperatur:
#     
# Letzte Temperatur...
# 
# - grösser gleich 5 Grad wärmer = "deutlich höheren"
# - grösser gleich 2 Grad wärmer = "höheren"
# - grösser gleich 1 Grad wärmer = "leicht höheren"
# - zwischen -1 und +1 Grad = "ungefähr gleichen"
# - grösser gleich 1 Grad kälter = "leicht tieferen"
# - grösser gleich 2 Grad kälter = "tieferen"
# - grösser gleich 5 Grad kälter = "deutlich tieferen"

# Mögliche Farben für die Ampel

# 1. #52cc83
# 2. #f7d77f
# 3. #e0a80c
# 4. #f75e05
# 5. #873101
