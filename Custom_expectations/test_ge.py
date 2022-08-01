# Databricks notebook source
dbutils.fs.rm("/test/project/GE_Plugins/expectations/ge_custom_multi_col_sum_4.py")

code_text='''
import pyspark.sql.functions as F
from datetime import datetime, timedelta

from great_expectations.dataset import (
    MetaSparkDFDataset,
    SparkDFDataset
)

class CustomSparkDataset(SparkDFDataset):
    _data_asset_type = "CustomSparkDataset"
    
    @MetaSparkDFDataset.column_map_expectation
    def expect_multi_column_sum_values_to_be_equal_to_other(self,
        column,
        column_list,
        column_name,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exception=None,
        meta=None,
        ):
        self.spark_df=self.spark_df.withColumn("actual_total", sum([F.col(x) for x in column_list]))
        return self.spark_df.withColumn(
            "__success",
            F.when(F.col("actual_total") <= F.col(column_name), F.lit(True)).otherwise(F.lit(False)),
        )
    
'''

dbutils.fs.put("/test/project/GE_Plugins/expectations/ge_custom_multi_col_sum_4.py", code_text, True)

# COMMAND ----------

!pip install great_expectations

# COMMAND ----------

from great_expectations.data_context.types.base import DataContextConfig,DatasourceConfig,FilesystemStoreBackendDefaults
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView
from typing import List 



class CustomeGEValidations:
    
    def __init__(self):
        self.feed_file_path = dbutils.widgets.get("feed_file_path")
        self.ge_plugins_directory = dbutils.widgets.get("ge_plugins_directory")
        self.ge_root_directory = dbutils.widgets.get("ge_root_directory")
        self.feed_file_format = dbutils.widgets.get("feed_file_format")
        
        self.data_context_config_spark = DataContextConfig(
            plugins_directory=self.ge_plugins_directory,
            config_variables_file_path=None,
            datasources = {
                "ge_custom_df":DatasourceConfig(
                    class_name="SparkDFDatasource",
                    module_name="great_expectations.datasource",
                    execution_engine={
                        "class_name": "SparkDFExecutionEngine"
                    },
                    data_connectors={
                        "runtime_data_connector": {
                            "class_name": "RuntimeDataConnector",
                            "module_name": "great_expectations.datasource.data_connector",
                            "batch_identifiers": ["default_identifier_name"],
                        },
                    },
                    data_asset_type={
                        "class_name": "CustomSparkDataset",
                        "module_name": "ge_custom_multi_col_sum_4",
                    },
                )
            },
            store_backend_defaults=FilesystemStoreBackendDefaults(root_directory = self.ge_root_directory),
        )
        self.context = BaseDataContext(project_config=self.data_context_config_spark)
        
    def execute_validation(self):
        feed_df = spark.read.format(self.feed_file_format)\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .load(self.feed_file_path)

        print("File Size - ", feed_df.count())

        expectation_config = {
            "expectation_suite_name": "test_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": "Value",
                        "max_value": 20000,
                        "min_value": 0,
                        "result_format": "BASIC"
                    },
                    "meta":{}
                },
                {
                    "expectation_type": "expect_column_max_to_be_between",
                    "kwargs": {
                        "column": "Year",
                        "max_value": 3000,
                        "min_value": 1000,
                        "result_format": "BASIC"
                    },
                    "meta":{}
                },
                {
                    "expectation_type": "expect_multi_column_sum_values_to_be_equal_to_other",
                    "kwargs": {
                        "column": "Value",
                        "column_list": [
                            "Element Code",
                            "Item Code",
                            "Year Code"
                        ],
                        "column_name": "Value",
                        "result_format": "BASIC"
                    },
                    "meta":{}
                },
            ]
        }

        validation_suite = ExpectationSuite(**expectation_config)

        batch_kwargs = {
            'dataset': feed_df,
            'datasource': 'ge_custom_df'
        }

        batch = self.context.get_batch(batch_kwargs=batch_kwargs, expectation_suite_name=validation_suite)

        file_validation = batch.validate()
        print(file_validation)
        #validation_result_document_content = ValidationResultsPageRenderer().render(file_validation)
        #validation_result_HTML = DefaultJinnaPageView().render(validation_result_document_content)

        #displayHTML(validation_result_HTML)
            

# COMMAND ----------

if __name__ == '__main__':
    cgev=CustomeGEValidations()
    cgev.execute_validation()

# COMMAND ----------


