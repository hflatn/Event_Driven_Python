import pandas as pd

nyt_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
jh_url_death = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
jh_url_case = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'

nyt_df = pd.read_csv(nyt_url)
jh_df_death = pd.read_csv(jh_url_death)
jh_df_case = pd.read_csv(jh_url_case)

print('Loading function')


def lambda_handler():

    # Renaming the columns to be properly capitalized 
    nyt_df.rename(columns={'date': 'Date', 'cases': 'Cases', 'deaths': 'Deaths'}, inplace = True)

    # Filters out countries that are not the US since these are global Covid statistics.
    jh_df_us_death = jh_df_death.loc[jh_df_death['Country/Region'] == 'US']
    # Drops unnecessary columns
    jh_df_us_death = jh_df_us_death.drop(['Country/Region', 'Province/State', 'Lat', 'Long'], axis=1)
    # Unpivots dataframe from wide to long format
    jh_df_us_death = jh_df_us_death.melt(var_name='Date', value_name='Deaths')

    # Repeats steps above for case statistics
    jh_df_us_case = jh_df_case.loc[jh_df_case['Country/Region'] == 'US']
    jh_df_us_case = jh_df_us_case.drop(['Country/Region', 'Province/State', 'Lat', 'Long'], axis=1)
    jh_df_us_case = jh_df_us_case.melt(var_name='Date', value_name='Cases')

    # Joining Hopkins Data Frames
    jh_df_us = pd.merge(jh_df_us_death, jh_df_us_case, on=['Date'])

    # Converts 'Date' columns to datetime data type
    nyt_df["Date"] = pd.to_datetime(nyt_df["Date"])
    jh_df_us["Date"] = pd.to_datetime(jh_df_us["Date"])

    

    print(nyt_df, "NYT Table")
    print(jh_df_us, "John Hopkins Table")


lambda_handler()

print("done")