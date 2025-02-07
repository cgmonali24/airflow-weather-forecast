import pandas as pd


def process_data(export_path,**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data')
    df = pd.DataFrame(data)
    df['time'] = pd.to_datetime(df['dt'], unit='s').dt.time
    df['average_temperature'] = (df['min_temperature'] + df['max_temperature']) / 2
    
    def categorize_wind_speed(speed):
        if speed < 2:
            return 'Calm'
        elif 2<= speed < 4:
            return 'Breezy'
        else:
            return 'Very Windy'

    df['wind_category'] = df['wind_speed'].apply(categorize_wind_speed)

    df = df[['dt','date', 'time', 'min_temperature', 'max_temperature', 'average_temperature', 'humidity','wind_speed', 'description', 'wind_category']]
    df['temp_change'] = df['average_temperature'].diff()
    df['wind_change'] = df['wind_speed'].diff()
    df_copy = df.copy()
    df['average_temperature_celsius'] = df['average_temperature'] - 273.15
    json_data = df.to_json(orient='records')


    df_grouped = df_copy.groupby('date').agg(
        {   'min_temperature': 'mean',
            'max_temperature': 'mean',
            'humidity': lambda x: round(x.mean()),
            'average_temperature': 'mean',
            'description': lambda x: ', '.join(x.unique())
        }).reset_index()

    df_grouped = df_grouped[['date', 'min_temperature', 'max_temperature', 'average_temperature', 'humidity', 'description']]
    df_final = df[['dt','date','time', 'min_temperature', 'max_temperature', 'average_temperature','average_temperature_celsius','temp_change' ,'humidity', 'wind_speed', 'wind_category','description',]]
    json_data = df_final.to_json(orient='records')
    
    kwargs['ti'].xcom_push(key='processed_data', value=json_data)


    with pd.ExcelWriter(f'{export_path}', engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Original Data', index=False)
        df_grouped.to_excel(writer, sheet_name='Aggregated Data', index=False)

