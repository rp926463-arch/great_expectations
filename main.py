# refer : https://towardsdatascience.com/great-expectations-always-know-what-to-expect-from-your-data-51214866c24
# Existing expectations : https://greatexpectations.io/expectations/
#                       : https://great-expectations.readthedocs.io/en/v0.3.2/glossary.html
# dataSource Name : my_pandas_datasource

"""
Data Source Config:
[{'class_name': 'Datasource',
  'data_connectors': {'default_inferred_data_connector_name': {'class_name': 'InferredAssetFilesystemDataConnector',
    'default_regex': {'group_names': ['data_asset_name'], 'pattern': '(.*)'},
    'base_directory': '..\\data',
    'module_name': 'great_expectations.datasource.data_connector'},
   'default_runtime_data_connector_name': {'class_name': 'RuntimeDataConnector',
    'assets': {'my_runtime_asset_name': {'class_name': 'Asset',
      'batch_identifiers': ['runtime_batch_identifier_name'],
      'module_name': 'great_expectations.datasource.data_connector.asset'}},
    'module_name': 'great_expectations.datasource.data_connector'}},
  'execution_engine': {'class_name': 'PandasExecutionEngine',
   'module_name': 'great_expectations.execution_engine'},
  'module_name': 'great_expectations.datasource',
  'name': 'my_pandas_datasource'}]
"""


def print_hi(name):
    pass


if __name__ == '__main__':
    print_hi('PyCharm')
