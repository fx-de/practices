# New Practices
__

## Naming Convention
![python_naming_convention](D:\Projects\Fusionex\python_naming_convention.JPG)



## Global Variables Declaration
### Add variable "project"
Apply project name to global variable "application_name" as shown below.
Purpose: DO NOT hardcode the project name in anywhere of the notebook here:
(You can search the project name in the entire notebook and replace them with the variable "project")

``` python
global project
global application_name

args = {
    "debug": True,
    "log": False,
    "environment": "PRD"
}

project = "yoodo"
application_name = "{}.tripica.billing".format(project)
```
Apply project name to configuration_path
``` python
configuration_path = "/home/projects/{}/tripica/release/{}/config".format(project, ENVIRONMENT.lower())
```

Apply project name to command argument "description" as shown below.
``` python
# Main Function End

## Uncomment this block for production scripts
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="{}: {}".format(project, application_name))
    parser.add_argument("-e", "--environment", help = "Environment (DEV, SIT, PRD). (String)", required = False, type = str, default = "PRD", dest = "environment")
    parser.add_argument("-d", "--debug", help = "Debug flag. (set this flag for debug purpose)", action = "store_true", default = False, required = False, dest = "debug")
    parser.add_argument("-l", "--log", help = "Log flag. (set this flag for extract log purpose)", action = "store_true", default = False, required = False, dest = "log")
    args = vars(parser.parse_args())

    main(args)
```



# New methods in fusionex library
___

## New method: fusionex.configuration.Configuration.load_configuration()

#### Before:
``` python
configuration_path = "/home/projects/yoodo/tripica/release/{}/config".format(ENVIRONMENT.lower())
config = fusionex.configure_environment(configuration_path, IS_DEBUG)
```
#### After:
``` python
configuration_path = "/home/projects/{}/tripica/release/{}/config".format(project, ENVIRONMENT.lower())
config = fusionex.Configuration.load_configuration(configuration_path)
```
## Class: ConnectionConfig
### New method: from_config

#### Before:


``` python
target_connection = Connection_Config(
                    host = config.datamart["host"],
                    port = config.datamart["port"],
                    user = config.datamart["user"],
                    password = config.datamart["password"],
                    database = config.datamart["database"],
                    schema = config.datamart["schema"]
                    )

print("\nTarget Connection: ")
target_connection.print()
```
#### After:

```python
target_connection = ConnectionConfig.from_config(config.datamart)
print("\nTarget Connection: ")
target_connection.print()
```

## Class: DateTimeUtil
### New method: getDate
Get date part of datetime.

#### Example:

``` python
end_date_local = (DateTimeUtil.get_date(current_date) + timedelta(days = 1))
```



## Class: fusionex.pyspark.sql.DataFrame
### New method: 

#### read_json
Read json file.

##### Example:

``` python
# Load raw file from directory
file_path = "{}/structure/billing_account/".format(config.settings["source_path"])
df = FXDataFrame.read_json(spark, file_path)

# DEBUG: CHECKING PURPOSE
if IS_DEBUG:  
    df.print_info(1)
```

#### read_excel
Read excel file.
##### Example:

``` python
# Load raw file from directory
file_path = "/user/shek.hang.cheng/sample_xlsx"
dfs = FXDataFrame.read_excel(spark, file_path)

# DEBUG: CHECKING PURPOSE
if IS_DEBUG:
    for df in dfs.values():
        df.print_info(1)
```


#### from_utc_timestamp
Convert columns timestamp from UTC to specified time zone.

##### Example:

``` python
time_zone = "Asia/Kuala_Lumpur"

# Convert from UTC to local time.
df = (df.from_utc_timestamp("datetime_create", time_zone)
        .from_utc_timestamp("datetime_last_modif", time_zone)
        .from_utc_timestamp("end_datetime", time_zone)
        .from_utc_timestamp("start_datetime", time_zone))
```




#### from_unixtime
Convert columns long to timestamp.

##### Example:

``` python
df = (df.from_unixtime("datetime_create"))
```



#### window_first
Get the first row of the window.

##### Example:

``` python
df = (df.
# Remove the duplicate records based on datetime_last_modif
.window_first(Window.partitionBy(col("ouid")).orderBy(desc("datetime_last_modif"))))
```



#### write_to_hive
Write dataframe to hive table.

##### Example:

``` python
# Write to Hive
table = "billing_account"
df.write_to_hive(spark, target_connection.database, table)
```


#### write_to_database
Write dataframe to datamarts database.

##### Example:

``` python
table = "billing_account"
df.write_to_database(target_connection, table, metadata_connection)
```


#### write_to_destination
Write dataframe to datamarts database and hive database.

##### Example:

``` python
table = "billing_account"
df.write_to_destination(spark, target_connection, table, metadata_connection)
```


#### cal_dtd, cal_mtd, cal_ytd
Calculate the date to date, month to date, year to date. 
Prerequisites: columns "date", "month", "year", "year_month" are required for the functions

##### Example:

``` python
df = (mgm
      .cal_dtd("godfather")
      .cal_mtd("godfather")
      .cal_ytd("godfather")
      .cal_dtd("godchild")
      .cal_mtd("godchild")
      .cal_ytd("godchild")
      .cal_dtd("total_mgm")
      .cal_mtd("total_mgm")
      .cal_ytd("total_mgm")
     )

# DEBUG: CHECKING PURPOSE
if IS_DEBUG:
    df.print_info(1)
```


#### melt
Melt function. Transpose columns to rows.

##### Example:

``` python
df = (mgm
      .melt("year", ["godfather", "godchild"], var_name = "mgm_type", value_name = "mgm_count")
     )
```
