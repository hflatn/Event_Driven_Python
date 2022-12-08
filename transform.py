import extract
from extract import pd


nyt_df = extract.nyt_df
jh_df_death = extract.jh_df_death
jh_df_case = extract.jh_df_case

def lambda_handler(event, context):
    
    global jh_df_us
    global nyt_df

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


lambda_handler("filler", "filler")

print("done")
print(nyt_df)
print(jh_df_us)