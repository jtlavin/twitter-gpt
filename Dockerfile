FROM python:3.8.16-buster
RUN pip3 install --upgrade pip && pip3 install poetry

COPY poetry.lock pyproject.toml ./
RUN poetry install

COPY . .
EXPOSE 8501
CMD poetry run streamlit run tweet_gpt_analytics/app.py
