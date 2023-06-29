from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.models.taskinstance import TaskInstance

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

import time
import numpy as np
import pandas as pd
import pendulum
import logging

with DAG(dag_id="extract_financial_data", tags=["scraper"],start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), schedule_interval=None,
    params={
        "from_date": Param("1/6/2022", type="string", title="from_date"),
        "to_date": Param("1/1/2023", type="string", title="to_date"),
        "time_interval": Param("1d", type="string", title="time_interval"),
        "tags": Param(["GOOG", "AAPL"], type="array", title="tags")
    }) as dag:
    
    @task(task_id="Extract_html")
    def extract_html(**kwargs):
        ti:TaskInstance = kwargs['ti']
        dag_run:dag_run = ti.dag_run
        conf = dag_run.conf
        
        urls = generate_urls(conf["from_date"], conf["to_date"], conf["time_interval"], conf["tags"])
        
        return get_html_from_url(urls)

    
    
    def generate_urls(from_date:str, to_date:str, time_interval:str, tags:list) -> list:

        from_date = int(time.mktime(datetime.strptime(from_date, "%d/%m/%Y").timetuple()))
        to_date = int(time.mktime(datetime.strptime(to_date, "%d/%m/%Y").timetuple()))

        urls = list()
        
        for tag in tags:
            urls.append(f"https://finance.yahoo.com/quote/{tag}/history?period1={from_date}&period2={to_date}&interval={time_interval}&filter=history&frequency={time_interval}&includeAdjustedClose=true")

        return urls
    
    def get_html_from_url(urls:str):
        html_list = list()
        
        for url in urls:
            # driver = webdriver.Firefox()
            remote_webdriver = 'remote_chromedriver'
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--disable-dev-shm-usage")  # overcome limited resource problems
            options.add_argument("--no-sandbox")  # Bypass OS security model
            with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
                driver.get(url)
                time.sleep(5)
                reject_button_clickable = driver.find_element(By.CLASS_NAME, "reject-all")
                ActionChains(driver)\
                    .click(reject_button_clickable)\
                        .perform()
                time.sleep(15)

                # scroll till end
                for i in range(15):
                    ActionChains(driver).scroll_by_amount(0,1000).perform()
                
                time.sleep(3)
                page_source = driver.page_source
            
            html_list.append(page_source)

        return html_list
            
            
    @task(task_id="Parse_html")
    def parse_table(html_list):
        tables = list()
        for html in html_list:
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("tbody")
            table_data = list()
            
            for row in table.find_all("tr"):
                row_data = list()     

                for cell in row:
                    invalid_row = False
                    if "Stock Split" in cell.text or "Dividend" in cell.text:
                        invalid_row = True
                    row_data.append(cell.text)
                if not invalid_row:
                    table_data.append(row_data)
            
            tables.append(table_data)
            
        return tables
    @task(task_id="Save_data")
    def print_data(tables, **kwargs):
        
        ti:TaskInstance = kwargs['ti']
        dag_run:dag_run = ti.dag_run
        conf = dag_run.conf
        
        
        for table, tag in zip(tables, conf["tags"]):
            table_data = np.array(table)
            logging.info("type", type(table_data))
            data_dict = pd.DataFrame({"date":table_data[:,0], "Open":table_data[:,1], "High":table_data[:,2], "Low":table_data[:,3],
                          "Close":table_data[:,4], "Volume":table_data[:,5]})
            
            data_dict.to_csv(f"outputs/{tag}.csv", index=False)
        
    get_html = extract_html()
    parse_html = parse_table(get_html)
    save_data = print_data(parse_html) 
    get_html >> parse_html >> save_data
    