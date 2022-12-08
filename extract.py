import pandas as pd


nyt_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
jh_url_death = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
jh_url_case = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    
nyt_df = pd.read_csv(nyt_url)
jh_df_death = pd.read_csv(jh_url_death)
jh_df_case = pd.read_csv(jh_url_case)