#Package to load your environment variables when deploying locally
# from dotenv import load_dotenv
# load_dotenv()
import os
import datetime
import numpy as np
import pandas as pd
import streamlit as st
from wordcloud import WordCloud
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
import psycopg2
import psycopg2.extras

def get_db_connection() -> psycopg2.extensions.connection:
    # to connect to DB, use the parameters and password that define it
    conn = psycopg2.connect(
                            user="postgres",
                            password=os.environ['DB_PASSWORD'], #password
                            host=os.environ['DB_HOST'], #twitter.cblavhksmkyd.eu-central-1.rds.amazonaws.com
                            port="5432",
                            connect_timeout=1)
    return conn

@st.cache(suppress_st_warning=True)
def get_data(start_date: str = '2020-01-01',
             end_date: str = '2025-01-01') -> pd.DataFrame:

    conn = get_db_connection()
    # query the database with start and end data
    sql = f"""select * from tweets_analytics
              where timestamp between date('{start_date}') and date('{end_date}')
              """
    print(sql)
    df = pd.read_sql_query(sql, conn)
    # add some metadata to the string to show more details
    now = str(datetime.datetime.now())[:-7]
    st.sidebar.markdown(f"""**Latest update data :**
                            {now}
                        Adjust starting date or ending date to refresh data""")
    return df

if __name__ == "__main__":

    # here we define the layout of the sidebar
    st.title('Tweet GPT analytics dashboard')
    # here we run the main 'functionality' of the app
    start_date = st.sidebar.text_input("Starting date", "2023-01-01")
    end_date = st.sidebar.text_input("End date", "2024-01-01")
    df = get_data(start_date=start_date, end_date=end_date)
    # error handling message
    if df.empty:
            st.error('Your search parameters resulted in no data!')

    st.sidebar.subheader('Explanation')
    st.sidebar.markdown('''
                        **Tweets for a given list of authors are being extracted constantly to feed a Database.
                        Then the tweets are being analyzed by gpt-3.5turbo in terms of sentiment
                        and summary of the text. You can adjust the query with the date parametres
                        and select the desired author.***
                         ''')
    
    # get the set of values from the 'author' column
    author_options = set(df['author'])
    # create a dropdown menu in the sidebar
    default_author = 'GabrielBoric'
    selected_option = st.sidebar.selectbox('Select an author:', author_options, index=0)

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    st.dataframe(df)


    df_filtered = df[df.author == selected_option]
    # count the number of tweets per day
    tweet_count = df_filtered[['text', 'timestamp']].groupby(pd.Grouper(key='timestamp', freq='D')).count()
    # display the chart
    st.line_chart(tweet_count)

    tweets = ' '.join(df_filtered['text'])
    stop_words = set(stopwords.words('spanish'))
    words = tweets.split()
    filtered_words = [word for word in words if word.lower() not in stop_words]
    filtered_tweets = ' '.join(filtered_words)

    # create the word cloud
    wc = WordCloud(width=800, height=400).generate(filtered_tweets)

    # display the word cloud
    st.image(wc.to_array())
