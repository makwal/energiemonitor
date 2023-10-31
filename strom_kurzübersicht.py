#!/usr/bin/env python
# coding: utf-8

# - Verbrauch: https://opendata.swiss/de/dataset/energiedashboard-ch-stromverbrauch-swissgrid/resource/271a6814-75ad-400b-824e-27043a60b294
# - Produktion: https://opendata.swiss/de/dataset/energiedashboard-ch-stromproduktion-swissgrid
# - Wetterdaten: https://opendata.swiss/de/dataset/klimamessnetz-tageswerte

# In[ ]:


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


# Wir beziehen immer die Daten von gestern.

# In[ ]:


yesterday_raw = datetime.today() - timedelta(days=1)
yesterday = yesterday_raw.strftime('%Y-%m-%d')
yesterday_str = yesterday_raw.strftime('%-d. %B %Y')

day_before_yesterday = (yesterday_raw - timedelta(days=1)).strftime('%Y-%m-%d')


# In[ ]:


r = requests.get('https://energiedashboard.admin.ch/api/strom/v2/strom-verbrauch/landesverbrauch-mit-prognose')

r = r.json()

df = pd.DataFrame(r['currentEntry'], index=[0])


# Verbrauch

# In[ ]:


df['date'] = pd.to_datetime(df['date'])


# In[ ]:


usage = df['landesverbrauchPrognose'].values[0]


# In[ ]:


date = df['date'].dt.strftime('%-d. %B %Y').values[0]


# In[ ]:


trend_en = df['trend'].values[0]


# In[ ]:


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

trend_de = trend_setter(trend_en)

if trend_de == '':
    raise ValueError('Unbekannter Trend, Übersetzung nicht möglich.')


# Hier importieren wir die Ampelinformationen, Gefahrenstufe und dazugehöriger Text.

# In[ ]:


ampel_url = 'https://bfe-energy-dashboard-ogd.s3.amazonaws.com/ogd108_stufen_energiemangellage.json'
r = requests.get(ampel_url)
r = r.json()

res = r['ampel_status_strom'][0]

level = res['level']
titel = res['titel@de']


# **Wetter**

# Wir lesen die Tageswerte von Meteo Schweiz ein. Als Referenzmessstation dient Zürich-Fluntern (SMA). Wir greifen das Tagesmittel ab, das sich in der Spalte tre200d0 befindet. Als Vergleichswert bilden wir das Mittel der vorherigen 10 Tage.

# In[ ]:


weather_url = 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_SMA_current.csv'

df_weather = pd.read_csv(weather_url, delimiter=';')

df_weather['date'] = pd.to_datetime(df_weather['date'], format='%Y%m%d')

df_weather['mean_10d'] = df_weather['tre200d0'].rolling(10).mean()


# Weil die Wetterdaten von gestern erst im Verlauf des Tages kommen, wir die Grafik aber schon am Morgen updaten, fügen wir die Wetterdaten dynamisch dazu.

# In[ ]:


try:
    temperature = df_weather[df_weather['date'] == yesterday]['tre200d0'].values[0]
    mean_temp_10d = df_weather[df_weather['date'] == day_before_yesterday]['mean_10d'].values[0]
except IndexError as e:
    temperature = 'nicht verfügbar'
    
try:
    mean_temp_10d = df_weather[df_weather['date'] == day_before_yesterday]['mean_10d'].values[0]
except IndexError as e:
    mean_temp_10d = 'nicht verfügbar'


# Anhand dieser Funktion wird der textliche Vergleich der aktuellsten Temperatur mit dem 10-Tage-Mittel gemacht.

# In[ ]:


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


# Hier werden die Texte zusammengesetzt und in ein DataFrame verpackt.

# In[ ]:


icon_url = 'https://chm-editorial-data-static.s3.eu-west-1.amazonaws.com/red_mantel/energiedashboard/icons/blitz.png'

text_usage = f'<strong style="font-size:32px">{int(round(usage))} GWh</strong>&nbsp <span style="letter-spacing:0.75px">geschätzter Wert</span>'

text_comparison = f'''So viel <b>Strom</b> verbrauchte die Schweiz am {date} (inkl. Speicherpumpen).<br><br> Das ist <b>{trend_de}</b> im Durchschnitt der vorherigen zehn Tage. '''

text_bundesrat = f'Beurteilung Bundesrat: <strong>{titel}</strong> (Gefahrenstufe {level} von 5)'


# Den Temperaturteil fügen wir nur dazu, wenn die Angaben vorhanden sind.

# In[ ]:


if temperature != 'nicht verfügbar' and mean_temp_10d != 'nicht verfügbar':
    est_temp = texter_temp(temperature, mean_temp_10d)
    
    text_comparison += f' Dies bei <strong>{est_temp}</strong> Temperaturen (Zürich).'


# In[ ]:


data = [
    {'icon': f'![]({icon_url})', 'text': text_usage},
    {'icon': np.nan, 'text': text_comparison},
    {'icon': np.nan, 'text': text_bundesrat}
]


# In[ ]:


df_final = pd.DataFrame(data)


# **Datawrapper-Update**

# In[ ]:


chart_id = 'YNAIQ'


# Daten in die Grafik laden.

# In[ ]:


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


# In[ ]:


data_uploader(chart_id, df_final)

