# Yahoo-finance-airflow-scraper

This project uses airflow and selenium to scrape yahoo finance data and save it as a csv file.

# Technologies used

- airflow
- selenium
- beautifulsoup4
- docker-compose

# How to start

In the main folder using docker-compose run

```cmd
docker-compose up -d
```

This will start all the required containers. Then go to the airflow webserver at: http://localhost:8080/ and login with

- Username: airflow
- Password: airflow

Then go to "extract_financial_data" DAG and launch it with config. <br>

For example:

```json
{
  "from_date": "1/6/2022",
  "to_date": "1/1/2023",
  "time_interval": "1d",
  "tags": ["GOOG", "AAPL"]
}
```

After it will crate a csv file in the output folder for each tag provided. In the date interval with time interval (time_interval possible values: "1d" every day, "1w" every week, "1m" every month)
