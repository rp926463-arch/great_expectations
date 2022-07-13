# great_expectations

https://towardsdatascience.com/great-expectations-always-know-what-to-expect-from-your-data-51214866c24

To install Great Expectations, type:

$ pip install great_expectations

<h2>Getting Started</h2>
<b>Create Data Context</b>
Data Context manages your project configuration. To create a new data context using the V3 (Batch Request) API, type:

$ great_expectations --v3-api init

And the new directory with the following structure will be created in your current directory!

Functions of these directories:

1.  The file great_expectations.yml contains the main configuration of your deployment.
2.  The directory expections stores all your Expectations as JSON files.
3.  The directory plugins holds code for any custom plugins you might have.
4.  The directory uncommitted contains files that shouldn’t be in version control.

Great Expectations Workflow(3 Steps)
*   Connect to data
*   Create expectations
*   Validate your data


1.  Connect to Data
As a demonstration, I will split the advertising dataset downloaded from Kaggle into two datasets: first_data.csv and second_data.csv . Specifically,

*   first_data.csv contains data from January 2016 to April 2016
*   second_data.csv contains data from May 2016 to July 2016

<br>These datasets are stored under the directory data .

To connect to our data, type:

$ great_expectations --v3-api datasource new
