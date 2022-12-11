import boto3
import json
from extract import pd

client = boto3.client('ssm')
target_arn = client.get_parameter(Name='target_arn')
target_arn = target_arn['Parameter']['Value']

def notify(nyt_df, jh_df):
    
    nyt_df_json = nyt_df.to_json()
    jh_df_json = jh_df.to_json()
    
    print(jh_df_json)
    print(nyt_df.shape[0])
    # print(os.environ[target_arn])
    
    message = {
        "New York Times Database Number of Rows Added": nyt_df.shape[0],
        'New York Times Database Rows Added': nyt_df_json,
        "John Hopkins Database Number of Rows Added": jh_df.shape[0],
        'John Hopkins Database Rows added': jh_df_json
    }
    
    
    client2 = boto3.client('sns')
    response = client2.publish(
        TargetArn = target_arn,
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure = 'json')