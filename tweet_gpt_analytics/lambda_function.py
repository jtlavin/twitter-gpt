from twython import Twython
import os
import openai
from dateutil import parser
from datetime import datetime
import logging
import json
import pytz
from typing import Optional
import re
import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
from botocore.exceptions import ClientError

openai.api_key = os.environ['OPENAI_API_KEY']

def get_db_connection() -> psycopg2.extensions.connection:
    # to connect to DB, use the parameters and password that define it
    conn = psycopg2.connect(
                            user="postgres",
                            password=os.environ['DB_PASSWORD'], #password
                            host=os.environ['DB_HOST'], #twitter.cblavhksmkyd.eu-central-1.rds.amazonaws.com
                            port="5432",
                            connect_timeout=1)
    return conn

def _time_parser(twitter_time: str) -> datetime:
    '''
    Parse string from twitter api like 'Sat Sep 02 14:25:02 +0000 2021'
    to a datetime object in utc time
    '''
    return parser.parse(twitter_time)


def is_recent(tweet: dict,
              max_time_interval_minutes: int = 20) -> bool:
    '''
    a tweet is recent if it is posted in the last x minutes'
    '''
    time_created = _time_parser(tweet['created_at'])
    now = datetime.now(tz=pytz.UTC)
    # converts time to minutes as the function takes minutes as argument
    seconds_diff = (now-time_created).seconds
    minutes_diff = seconds_diff/60
    is_recent_tweet = minutes_diff <= max_time_interval_minutes
    return is_recent_tweet


def extract_fields(tweet: dict) -> dict:
    '''
    Arbitrary decision to save only some fields of the tweet,
    store them in a different dictionary form which
    is convenient for saving them later
    '''
    author = tweet['user']['screen_name']
    time_created = _time_parser(tweet['created_at'])
    text = tweet['text']
    return dict(author=author,timestamp=time_created, text=text)


def upload_file_to_s3(local_file_name: str,
                      bucket: str,
                      s3_object_name: Optional[str]=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param s3_object_name: If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if s3_object_name is None:
        s3_object_name = local_file_name

    # Upload the file
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ['IAM_AWS_ACCESS_KEY'],
        aws_secret_access_key=os.environ['IAM_AWS_SECRET_ACCESS_KEY'],
    )
    try:
        s3_client.upload_file(local_file_name, bucket, s3_object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def convert_timestamp_to_int(tweet: dict) ->dict:
    '''datetime object are not serializable for json,
    so we need to convert them to unix timestamp'''
    tweet = tweet.copy()
    tweet['timestamp'] = tweet['timestamp'].timestamp()
    return tweet


def insert_data_in_db(df: pd.DataFrame,
                      conn: psycopg2.extensions.connection,
                      table_name: str = 'tweets_analytics') -> None:
    # you need data and a valid connection to insert data in DB
    are_data = len(df) > 0
    if are_data and conn is not None:
        try:
            cur = conn.cursor()
            # to perform a batch insert we need to reshape the data in 2 strings with the column names and their values
            df_columns = list(df.columns)
            columns = ",".join(df_columns)

            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

            # create INSERT INTO table (columns) VALUES('%s',...)
            # here the final 2 strings are created
            insert_string = "INSERT INTO {} ({}) {}"
            insert_stmt = insert_string.format(table_name, columns, values)
            psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
            conn.commit()
            print('succesful update')

        except psycopg2.errors.InFailedSqlTransaction:
            # if the transaction fails, rollback to avoid DB lock problems
            logging.exception('FAILED transaction')
            cur.execute("ROLLBACK")
            conn.commit()

        except Exception as e:
            # if the transaction fails, rollback to avoid DB lock problems
            logging.exception(f'FAILED  {str(e)}')
            cur.execute("ROLLBACK")
            conn.commit()
        finally:
            # close the DB connection after this
            cur.close()
            conn.close()
    elif conn is None:
        raise ValueError('Connection to DB must be alive!')
    elif len(df) == 0:
        raise ValueError('df has 0 rows!')
    
def ask_gpt(context, question, tweet, MODEL):
    response = openai.ChatCompletion.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": f"{context}"},
                {"role": "user", "content": f'{question}\n\Tweet:\n"""\n{tweet}'},
            ],
            temperature=0.5,
        )
    return response


def lambda_handler(event, context):
    try:
        python_tweets = Twython(os.environ['TWITTER_API_KEY'],
                            os.environ['TWITTER_API_SECRET'])
        persons = ['LavinJoaquin', 'gabrielboric', 'rodolfocarter', 'joseantoniokast', 'AXELKAISER', 'PamJiles', 'Orrego', 'carreragonzalo', 'Diego_Schalper' ,'GiorgioJackson', 'izkia'
                , 'Carolina_Toha', 'guidogirardi', 'Jou_Kaiser', 'MaiteOrsini', 'GmoRamirez', 'gonzalowinter']
        clean_timeline = []
        for p in persons:
            query = {'screen_name': p}
            tweets = python_tweets.get_user_timeline(**query)
            recent_tweets = [tweet for tweet in tweets
                            if is_recent(tweet)]   
            for tweet in recent_tweets:
                # Ignorar los retweets
                if 'retweeted_status' not in tweet:
                    # Ignorar los tweets con enlaces
                    if 'http' in tweet['text']:
                        tweet['text'] = re.sub(r"http\S+", "", tweet['text'])
                    else:
                        continue
                    # Agregar los tweets limpios a la lista
                    clean_timeline.append(tweet)
    
        clean_timeline = [extract_fields(tweet) for tweet in clean_timeline]
    
        context_gpt = {}
        question = {}
    
        question['summary'] = 'Me puedes decir cual es el tema central del siguiente tweet? responde en no mas de 3 palabras'
        question['intention'] = 'Crees que el siguiente tweet tiene una intención constructiva, destructiva o neutral? Tu respuesta debe ser una sola palabra'
        context_gpt['summary'] = 'Imagina que eres un experto en descubrir palabras clave y resumiendo contenido'
        context_gpt['intention'] = 'Imagina que eres un experto en politica y opinologia'
    
        MODEL = "gpt-3.5-turbo"
        columns = ['summary', 'intention']
    
        for tw in range(len(clean_timeline)):
            for col in columns:
                response = ask_gpt(context_gpt[col], question[col], clean_timeline[tw]['text'], MODEL)
                clean_timeline[tw][f'gpt_{col}'] = response['choices'][0]['message']['content']
        now_str = datetime.now(tz=pytz.UTC).strftime('%d-%m-%Y-%H:%M:%S')
        filename = f'{now_str}.json'
        output_path_file = f'/tmp/{filename}'
        # in lambda files need to be dumped into /tmp folder
        with open(output_path_file, 'w') as fout:
            tweets_to_save = [convert_timestamp_to_int(tweet)
                                for tweet in clean_timeline]
            json.dump(tweets_to_save , fout)
        upload_file_to_s3(local_file_name=output_path_file,
                            bucket=os.environ['TWITTER_BUCKET'],
                            s3_object_name=f'raw-messages/{filename}')
    
        tweets_df = pd.DataFrame(clean_timeline)
        conn = get_db_connection()
        insert_data_in_db(df=tweets_df, conn=conn, table_name='tweets_analytics')
    except Exception as e:
        logging.exception('Exception occured \n')    
    print('Lambda executed succesfully!')

if __name__ == "__main__":
    lambda_handler({}, {})