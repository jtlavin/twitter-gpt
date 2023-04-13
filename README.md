# Twitter GPT analytics
In this repository I've developed a Python app in Streamlit and deployed it with Docker on AWS. It takes
information from the Twitter API and uses gpt-3.5 to make analysis regarding keywords for every tweet and a summary. Currently it's extracting
tweets from chilean politicians via Lambda Function and storing them in a RDS.

## Architecture 
+ TODO

## Local setup
Not necessary but if we students want to run code locally it would be better to create a virtual environment with `conda` and install the following

If you are on windows 
- install anaconda3 at https://www.anaconda.com/products/individual
- start the Anaconda3 power shell
- set the environment variables:
```
[Environment]::SetEnvironmentVariable("TWITTER_API_KEY", "Your-api-key", "User")
[Environment]::SetEnvironmentVariable("TWITTER_API_SECRET", "Your-secret-key", "User")
[Environment]::SetEnvironmentVariable("S3_BUCKET_NAME", "my-tweet-analytics-storage", "User")
[Environment]::SetEnvironmentVariable("DB_PASSWORD", "password", "User")
[Environment]::SetEnvironmentVariable("DB_HOST", "your-RDS-endpoint", "User")
```
 
```sh
conda create --name tweet_analytics_py38 python=3.8 spyder
conda activate tweet_analytics_py38
conda install -c conda-forge poetry # dependency management
conda install -c conda-forge notebook # for jupyter
```

to install the project dependencies run
```sh
poetry install
```

After the tutorial and setting `environmental variables`, you can run locally the dashboard with
```
poetry run streamlit run src/app.py
```
