import pandas as pd
import requests
from functools import reduce
import json


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, concat, lit, expr,DataFrame
from urllib.parse import quote

def fetch_and_process_data():

    # # Initialize Spark session
    # spark = SparkSession.builder.appName("PrescribingAnalysis").getOrCreate()
    # spark.conf.set("spark.sql.debug.maxToStringFields", -1)

    base_endpoint = 'https://opendata.nhsbsa.net/api/3/action/'
    package_list_method = 'package_list'
    package_show_method = 'package_show?id='
    action_method = 'datastore_search_sql?'
    
    dataset_id = "english-prescribing-data-epd"
    POSTCODE = 'WC'
    CHEMICAL_SUBSTANCE_BNF_DESCR = 'Colecalciferol'
    PRACTICE_CODE = ['F83652','F83004','F83033','F83064','F83624','F83012','F83008','Y01066','F83660','F83010','F83063','F83053','F83056','F83034','F83681','F83678','F83021','F83007','F83680','F83015','F83032','F83686','F83666','F83671','F83027','F83002','F83674','F83673','F83045','F83060','F83039','F83664']

    metadata_response = requests.get(f"{base_endpoint}{package_show_method}{dataset_id}")
    
    # Check if the response contains data
    if metadata_response.content:
        try:
            # Attempt to decode the response as JSON
            metadata_response = metadata_response.json()
            # Continue processing the JSON data as needed
        except json.decoder.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
    else:
        print("Empty response received.")
        # Handle the case of an empty response as needed
    
    resources_table = pd.json_normalize(metadata_response['result']['resources'])
    resource_name_list = resources_table[resources_table['name'].str.contains('2014|2015|2016|2017|2018|2019|2020|2021|2022|2023')]['name']
    
    # # Convert the list of resource names to a Spark DataFrame
    # resource_name_df = spark.createDataFrame([(x,) for x in resource_name_list], ["name"])

    # Asynchronous API Calls in PySpark
    async_queries = []

    for resource_name in resource_name_list:
        query = ("SELECT YEAR_MONTH, PRACTICE_NAME, PRACTICE_CODE, CHEMICAL_SUBSTANCE_BNF_DESCR, BNF_DESCRIPTION, "
                "BNF_CHAPTER_PLUS_CODE, QUANTITY, ITEMS, TOTAL_QUANTITY, ADQUSAGE, NIC, ACTUAL_COST, POSTCODE "
                "FROM `{}` WHERE "
                "CHEMICAL_SUBSTANCE_BNF_DESCR = '{}' "
                "AND PRACTICE_CODE IN ({})"
                .format(resource_name, CHEMICAL_SUBSTANCE_BNF_DESCR,
                        ", ".join(f"'{code}'" for code in PRACTICE_CODE)))
        async_queries.append((resource_name, query))

    # Create a DataFrame with the queries
    # resource_name_df = spark.createDataFrame(async_queries, ["encoded_name", "sql_query"])
    resource_name_df = pd.DataFrame(async_queries, columns=["encoded_name", "sql_query"])


    # Create a new column with the full API request URL
    # resource_name_df = resource_name_df.withColumn("api_url",
    #                                             concat(lit(f"{base_endpoint}{action_method}resource_id="),
    #                                                     col("encoded_name"),
    #                                                     lit("&sql="),
    #                                                     col("sql_query").cast("string")))
    resource_name_df["api_url"] = (
        f"{base_endpoint}{action_method}resource_id=" + resource_name_df["encoded_name"]
        + "&sql=" + resource_name_df["sql_query"].astype("string")
    )


    # Convert the Spark DataFrame to a Pandas DataFrame to get the list of API URLs
    # api_url_list = resource_name_df.select("api_url").toPandas()["api_url"].tolist()
    api_url_list = resource_name_df["api_url"].tolist()

    # Perform asynchronous API calls using Spark
    async_df_list = []
    
    for api_url in api_url_list:
        try:
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            if response.ok:
                tmp_response = response.json()

                if 'result' in tmp_response and 'result' in tmp_response['result'] and 'records' in tmp_response['result']['result']:
                    records = tmp_response['result']['result']['records']

                    # Check if there are records
                    if records:
                        # Process the records as needed
                        # tmp_df = spark.createDataFrame(records)
                        tmp_df = pd.DataFrame(records)
                        async_df_list.append(tmp_df)
                    else:
                        print("No records found in the response.")
            else:
                print(f"Failed to fetch data from API: {response.url}")
        
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data from API: {api_url}")
            print(f"Error: {e}")
        
    # Concatenate all DataFrames in async_df_list into one
    # async_df = reduce(DataFrame.union, async_df_list)   
    async_df = pd.concat(async_df_list, ignore_index=True)
            
    return async_df