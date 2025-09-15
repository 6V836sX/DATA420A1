```python
# Run this cell to import pyspark and to define start_spark() and stop_spark()

import findspark

findspark.init()

import getpass
import random
import re

import pandas
import pyspark
from IPython.display import HTML, display
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Constants used to interact with Azure Blob Storage using the hdfs command or Spark

global username

username = re.sub("@.*", "", getpass.getuser())

global azure_account_name
global azure_data_container_name
global azure_user_container_name
global azure_user_token

azure_account_name = "madsstorage002"
azure_data_container_name = "campus-data"
azure_user_container_name = "campus-user"
azure_user_token = r"sp=racwdl&st=2025-08-01T09:41:33Z&se=2026-12-30T16:56:33Z&spr=https&sv=2024-11-04&sr=c&sig=GzR1hq7EJ0lRHj92oDO1MBNjkc602nrpfB5H8Cl7FFY%3D"


# Functions used below


def dict_to_html(d):
    """Convert a Python dictionary into a two column table for display."""

    html = []

    html.append(f'<table width="100%" style="width:100%; font-family: monospace;">')
    for k, v in d.items():
        html.append(f'<tr><td style="text-align:left;">{k}</td><td>{v}</td></tr>')
    html.append(f"</table>")

    return "".join(html)


def show_as_html(df, n=20):
    """Leverage existing pandas jupyter integration to show a spark dataframe as html.

    Args:
        n (int): number of rows to show (default: 20)
    """

    display(df.limit(n).toPandas())


def display_spark():
    """Display the status of the active Spark session if one is currently running."""

    if "spark" in globals() and "sc" in globals():

        name = sc.getConf().get("spark.app.name")

        html = [
            f"<p><b>Spark</b></p>",
            f'<p>The spark session is <b><span style="color:green">active</span></b>, look for <code>{name}</code> under the running applications section in the Spark UI.</p>',
            f"<ul>",
            f'<li><a href="http://localhost:{sc.uiWebUrl.split(":")[-1]}" target="_blank">Spark Application UI</a></li>',
            f"</ul>",
            f"<p><b>Config</b></p>",
            dict_to_html(dict(sc.getConf().getAll())),
            f"<p><b>Notes</b></p>",
            f"<ul>",
            f"<li>The spark session <code>spark</code> and spark context <code>sc</code> global variables have been defined by <code>start_spark()</code>.</li>",
            f"<li>Please run <code>stop_spark()</code> before closing the notebook or restarting the kernel or kill <code>{name}</code> by hand using the link in the Spark UI.</li>",
            f"</ul>",
        ]
        display(HTML("".join(html)))

    else:

        html = [
            f"<p><b>Spark</b></p>",
            f'<p>The spark session is <b><span style="color:red">stopped</span></b>, confirm that <code>{username} (notebook)</code> is under the completed applications section in the Spark UI.</p>',
            f"<ul>",
            f'<li><a href="http://mathmadslinux2p.canterbury.ac.nz:8080/" target="_blank">Spark UI</a></li>',
            f"</ul>",
        ]
        display(HTML("".join(html)))


# Functions to start and stop spark


def start_spark(
    executor_instances=2, executor_cores=1, worker_memory=1, master_memory=1
):
    """Start a new Spark session and define globals for SparkSession (spark) and SparkContext (sc).

    Args:
        executor_instances (int): number of executors (default: 2)
        executor_cores (int): number of cores per executor (default: 1)
        worker_memory (float): worker memory (default: 1)
        master_memory (float): master memory (default: 1)
    """

    global spark
    global sc

    cores = executor_instances * executor_cores
    partitions = cores * 4
    port = 4000 + random.randint(1, 999)

    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home=/tmp/{username}/spark/",
        )
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.executor.instances", str(executor_instances))
        .config("spark.executor.cores", str(executor_cores))
        .config("spark.cores.max", str(cores))
        .config("spark.driver.memory", f"{master_memory}g")
        .config("spark.executor.memory", f"{worker_memory}g")
        .config("spark.driver.maxResultSize", "0")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config(
            "spark.kubernetes.container.image",
            "madsregistry001.azurecr.io/hadoop-spark:v3.3.5-openjdk-8",
        )
        .config("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
        .config("spark.kubernetes.memoryOverheadFactor", "0.3")
        .config("spark.memory.fraction", "0.1")
        .config(
            f"fs.azure.sas.{azure_user_container_name}.{azure_account_name}.blob.core.windows.net",
            azure_user_token,
        )
        .config("spark.app.name", f"{username} (notebook)")
        .getOrCreate()
    )
    sc = SparkContext.getOrCreate()

    display_spark()


def stop_spark():
    """Stop the active Spark session and delete globals for SparkSession (spark) and SparkContext (sc)."""

    global spark
    global sc

    if "spark" in globals() and "sc" in globals():

        spark.stop()

        del spark
        del sc

    display_spark()


# Make css changes to improve spark output readability

html = [
    "<style>",
    "pre { white-space: pre !important; }",
    "table.dataframe td { white-space: nowrap !important; }",
    "table.dataframe thead th:first-child, table.dataframe tbody th { display: none; }",
    "</style>",
]
display(HTML("".join(html)))
```


<style>pre { white-space: pre !important; }table.dataframe td { white-space: nowrap !important; }table.dataframe thead th:first-child, table.dataframe tbody th { display: none; }</style>



```python
# Run this cell to start a spark session in this notebook

start_spark(executor_instances=4, executor_cores=2, worker_memory=4, master_memory=4)
```

    Warning: Ignoring non-Spark config property: fs.azure.sas.campus-user.madsstorage002.blob.core.windows.net
    Warning: Ignoring non-Spark config property: SPARK_DRIVER_BIND_ADDRESS
    25/09/11 22:13:42 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).



<p><b>Spark</b></p><p>The spark session is <b><span style="color:green">active</span></b>, look for <code>yxi75 (notebook)</code> under the running applications section in the Spark UI.</p><ul><li><a href="http://localhost:4051" target="_blank">Spark Application UI</a></li></ul><p><b>Config</b></p><table width="100%" style="width:100%; font-family: monospace;"><tr><td style="text-align:left;">spark.dynamicAllocation.enabled</td><td>false</td></tr><tr><td style="text-align:left;">spark.fs.azure.sas.uco-user.madsstorage002.blob.core.windows.net</td><td>"sp=racwdl&st=2024-09-19T08:00:18Z&se=2025-09-19T16:00:18Z&spr=https&sv=2022-11-02&sr=c&sig=qtg6fCdoFz6k3EJLw7dA8D3D8wN0neAYw8yG4z4Lw2o%3D"</td></tr><tr><td style="text-align:left;">spark.kubernetes.driver.pod.name</td><td>spark-master-driver</td></tr><tr><td style="text-align:left;">spark.executor.instances</td><td>4</td></tr><tr><td style="text-align:left;">spark.app.id</td><td>spark-1054e131d02d4bc6a6e42e1ed4b1e027</td></tr><tr><td style="text-align:left;">spark.kubernetes.executor.podNamePrefix</td><td>yxi75-notebook-344ce89938444bdb</td></tr><tr><td style="text-align:left;">spark.driver.memory</td><td>4g</td></tr><tr><td style="text-align:left;">spark.app.name</td><td>yxi75 (notebook)</td></tr><tr><td style="text-align:left;">spark.fs.azure.sas.campus-user.madsstorage002.blob.core.windows.net</td><td>"sp=racwdl&st=2024-09-19T08:03:31Z&se=2025-09-19T16:03:31Z&spr=https&sv=2022-11-02&sr=c&sig=kMP%2BsBsRzdVVR8rrg%2BNbDhkRBNs6Q98kYY695XMRFDU%3D"</td></tr><tr><td style="text-align:left;">spark.kubernetes.container.image.pullPolicy</td><td>IfNotPresent</td></tr><tr><td style="text-align:left;">spark.sql.shuffle.partitions</td><td>32</td></tr><tr><td style="text-align:left;">spark.kubernetes.namespace</td><td>yxi75</td></tr><tr><td style="text-align:left;">spark.serializer.objectStreamReset</td><td>100</td></tr><tr><td style="text-align:left;">spark.driver.maxResultSize</td><td>0</td></tr><tr><td style="text-align:left;">spark.app.submitTime</td><td>1757585623019</td></tr><tr><td style="text-align:left;">spark.submit.deployMode</td><td>client</td></tr><tr><td style="text-align:left;">spark.master</td><td>k8s://https://kubernetes.default.svc.cluster.local:443</td></tr><tr><td style="text-align:left;">spark.driver.extraJavaOptions</td><td>-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dderby.system.home=/tmp/yxi75/spark/</td></tr><tr><td style="text-align:left;">spark.fs.azure</td><td>org.apache.hadoop.fs.azure.NativeAzureFileSystem</td></tr><tr><td style="text-align:left;">spark.app.startTime</td><td>1757585623186</td></tr><tr><td style="text-align:left;">spark.memory.fraction</td><td>0.1</td></tr><tr><td style="text-align:left;">spark.executor.memory</td><td>4g</td></tr><tr><td style="text-align:left;">spark.executor.id</td><td>driver</td></tr><tr><td style="text-align:left;">spark.kubernetes.executor.container.image</td><td>madsregistry001.azurecr.io/hadoop-spark:v3.3.5-openjdk-8-1.0.16</td></tr><tr><td style="text-align:left;">spark.executor.cores</td><td>2</td></tr><tr><td style="text-align:left;">spark.kubernetes.memoryOverheadFactor</td><td>0.3</td></tr><tr><td style="text-align:left;">spark.driver.host</td><td>spark-master-svc</td></tr><tr><td style="text-align:left;">spark.ui.port</td><td>${env:SPARK_UI_PORT}</td></tr><tr><td style="text-align:left;">spark.kubernetes.container.image</td><td>madsregistry001.azurecr.io/hadoop-spark:v3.3.5-openjdk-8</td></tr><tr><td style="text-align:left;">spark.kubernetes.executor.podTemplateFile</td><td>/opt/spark/conf/executor-pod-template.yaml</td></tr><tr><td style="text-align:left;">fs.azure.sas.campus-user.madsstorage002.blob.core.windows.net</td><td>sp=racwdl&st=2025-08-01T09:41:33Z&se=2026-12-30T16:56:33Z&spr=https&sv=2024-11-04&sr=c&sig=GzR1hq7EJ0lRHj92oDO1MBNjkc602nrpfB5H8Cl7FFY%3D</td></tr><tr><td style="text-align:left;">spark.rdd.compress</td><td>True</td></tr><tr><td style="text-align:left;">spark.executor.extraJavaOptions</td><td>-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false</td></tr><tr><td style="text-align:left;">spark.cores.max</td><td>8</td></tr><tr><td style="text-align:left;">spark.driver.port</td><td>7077</td></tr><tr><td style="text-align:left;">spark.submit.pyFiles</td><td></td></tr><tr><td style="text-align:left;">spark.ui.showConsoleProgress</td><td>true</td></tr></table><p><b>Notes</b></p><ul><li>The spark session <code>spark</code> and spark context <code>sc</code> global variables have been defined by <code>start_spark()</code>.</li><li>Please run <code>stop_spark()</code> before closing the notebook or restarting the kernel or kill <code>yxi75 (notebook)</code> by hand using the link in the Spark UI.</li></ul>



```python
# Write your imports here or insert cells below

import re
import subprocess
from math import asin, cos, radians, sin, sqrt
from pprint import pprint

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
```


```python
# Paths global variables
DATA_ROOT = "wasbs://campus-data@madsstorage002.blob.core.windows.net/ghcnd/"
USER_ROOT = "wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/"

paths = {
    "daily": DATA_ROOT + "daily/",
    "stations": DATA_ROOT + "ghcnd-stations.txt",
    "countries": DATA_ROOT + "ghcnd-countries.txt",
    "states": DATA_ROOT + "ghcnd-states.txt",
    "inventory": DATA_ROOT + "ghcnd-inventory.txt",
}

paths

stations_enriched_savepath = USER_ROOT + "stations_enriched_parquet"
station_count_by_country_path = USER_ROOT + "station_count_by_country_parquet"
station_count_us_terri_path = USER_ROOT + "station_count_us_terri_parquet"
country_meta_with_station_num_path = USER_ROOT + "country_meta_with_station_num"
states_meta_with_station_num_path = USER_ROOT + "states_meta_with_station_num"
```

# Analysis

## Q1 Station Study

### (a)


```python
# load stations_enriched from saved path
stations_enriched = spark.read.parquet(stations_enriched_savepath)
stations_enriched.cache()
# check variable
show_as_html(stations_enriched)
```

                                                                                    


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>STATE</th>
      <th>COUNTRY_CODE</th>
      <th>LATITUDE</th>
      <th>LONGITUDE</th>
      <th>ELEVATION</th>
      <th>NAME</th>
      <th>GSN_FLAG</th>
      <th>HCN_CRN</th>
      <th>WMO_ID</th>
      <th>...</th>
      <th>STATE_NAME</th>
      <th>FIRSTYEAR_ANY</th>
      <th>LASTYEAR_ANY</th>
      <th>N_ELEMENTS</th>
      <th>TMAX</th>
      <th>TMIN</th>
      <th>PRCP</th>
      <th>SNOW</th>
      <th>SNWD</th>
      <th>N_CORE_ELEMENTS</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AE000041196</td>
      <td></td>
      <td>AE</td>
      <td>25.3330</td>
      <td>55.5170</td>
      <td>34.0</td>
      <td>SHARJAH INTER. AIRP</td>
      <td>GSN</td>
      <td></td>
      <td>41196</td>
      <td>...</td>
      <td>None</td>
      <td>1944</td>
      <td>2025</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AEM00041218</td>
      <td></td>
      <td>AE</td>
      <td>24.2620</td>
      <td>55.6090</td>
      <td>264.9</td>
      <td>AL AIN INTL</td>
      <td></td>
      <td></td>
      <td>41218</td>
      <td>...</td>
      <td>None</td>
      <td>1994</td>
      <td>2025</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AGE00147715</td>
      <td></td>
      <td>AG</td>
      <td>35.4200</td>
      <td>8.1197</td>
      <td>863.0</td>
      <td>TEBESSA</td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td>None</td>
      <td>1879</td>
      <td>1938</td>
      <td>3</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AGE00147717</td>
      <td></td>
      <td>AG</td>
      <td>35.2000</td>
      <td>0.6300</td>
      <td>476.0</td>
      <td>SIDI-BEL-ABBES</td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td>None</td>
      <td>1880</td>
      <td>1938</td>
      <td>3</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AGE00147719</td>
      <td></td>
      <td>AG</td>
      <td>33.7997</td>
      <td>2.8900</td>
      <td>767.0</td>
      <td>LAGHOUAT</td>
      <td></td>
      <td></td>
      <td>60545</td>
      <td>...</td>
      <td>None</td>
      <td>1888</td>
      <td>2025</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>AGM00060425</td>
      <td></td>
      <td>AG</td>
      <td>36.2130</td>
      <td>1.3320</td>
      <td>141.1</td>
      <td>ECH CHELIFF</td>
      <td></td>
      <td></td>
      <td>60425</td>
      <td>...</td>
      <td>None</td>
      <td>1943</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>6</th>
      <td>AGM00060430</td>
      <td></td>
      <td>AG</td>
      <td>36.3000</td>
      <td>2.2330</td>
      <td>721.0</td>
      <td>MILIANA</td>
      <td></td>
      <td></td>
      <td>60430</td>
      <td>...</td>
      <td>None</td>
      <td>1957</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>7</th>
      <td>AGM00060490</td>
      <td></td>
      <td>AG</td>
      <td>35.6240</td>
      <td>-0.6210</td>
      <td>89.9</td>
      <td>ES SENIA</td>
      <td></td>
      <td></td>
      <td>60490</td>
      <td>...</td>
      <td>None</td>
      <td>1957</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>8</th>
      <td>AGM00060514</td>
      <td></td>
      <td>AG</td>
      <td>35.1670</td>
      <td>2.3170</td>
      <td>801.0</td>
      <td>KSAR CHELLALA</td>
      <td></td>
      <td></td>
      <td>60514</td>
      <td>...</td>
      <td>None</td>
      <td>1995</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>9</th>
      <td>AGM00060515</td>
      <td></td>
      <td>AG</td>
      <td>35.3330</td>
      <td>4.2060</td>
      <td>459.0</td>
      <td>BOU SAADA</td>
      <td></td>
      <td></td>
      <td>60515</td>
      <td>...</td>
      <td>None</td>
      <td>1984</td>
      <td>2025</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>10</th>
      <td>AGM00060522</td>
      <td></td>
      <td>AG</td>
      <td>34.8200</td>
      <td>-1.7700</td>
      <td>426.0</td>
      <td>MAGHNIA</td>
      <td></td>
      <td></td>
      <td>60522</td>
      <td>...</td>
      <td>None</td>
      <td>1976</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>11</th>
      <td>AGM00060536</td>
      <td></td>
      <td>AG</td>
      <td>34.8670</td>
      <td>0.1500</td>
      <td>752.0</td>
      <td>SAIDA</td>
      <td></td>
      <td></td>
      <td>60536</td>
      <td>...</td>
      <td>None</td>
      <td>1981</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>12</th>
      <td>AGM00060555</td>
      <td></td>
      <td>AG</td>
      <td>33.0680</td>
      <td>6.0890</td>
      <td>85.0</td>
      <td>SIDI MAHDI</td>
      <td></td>
      <td></td>
      <td>60555</td>
      <td>...</td>
      <td>None</td>
      <td>1958</td>
      <td>2025</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>13</th>
      <td>AGM00060580</td>
      <td></td>
      <td>AG</td>
      <td>31.9170</td>
      <td>5.4130</td>
      <td>150.0</td>
      <td>OUARGLA</td>
      <td></td>
      <td></td>
      <td>60580</td>
      <td>...</td>
      <td>None</td>
      <td>1957</td>
      <td>2025</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>14</th>
      <td>AJ000037742</td>
      <td></td>
      <td>AJ</td>
      <td>40.9000</td>
      <td>47.3000</td>
      <td>313.0</td>
      <td>ORDJONIKIDZE,ZERNOSOVHOZ</td>
      <td></td>
      <td></td>
      <td>37742</td>
      <td>...</td>
      <td>None</td>
      <td>1955</td>
      <td>1987</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>15</th>
      <td>AJ000037747</td>
      <td></td>
      <td>AJ</td>
      <td>40.6170</td>
      <td>47.1500</td>
      <td>15.0</td>
      <td>EVLAKH AIRPORT</td>
      <td></td>
      <td></td>
      <td>37747</td>
      <td>...</td>
      <td>None</td>
      <td>1955</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>16</th>
      <td>AJ000037756</td>
      <td></td>
      <td>AJ</td>
      <td>40.5330</td>
      <td>48.9330</td>
      <td>755.0</td>
      <td>MARAZA</td>
      <td></td>
      <td></td>
      <td>37756</td>
      <td>...</td>
      <td>None</td>
      <td>1955</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>17</th>
      <td>AJ000037816</td>
      <td></td>
      <td>AJ</td>
      <td>40.5000</td>
      <td>46.1000</td>
      <td>1655.0</td>
      <td>DASHKESAN</td>
      <td></td>
      <td></td>
      <td>37816</td>
      <td>...</td>
      <td>None</td>
      <td>1963</td>
      <td>1991</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>18</th>
      <td>AJ000037899</td>
      <td></td>
      <td>AJ</td>
      <td>39.7670</td>
      <td>46.7500</td>
      <td>1355.0</td>
      <td>SHUSHA</td>
      <td></td>
      <td></td>
      <td>37899</td>
      <td>...</td>
      <td>None</td>
      <td>1936</td>
      <td>1991</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>19</th>
      <td>AJ000037914</td>
      <td></td>
      <td>AJ</td>
      <td>39.9000</td>
      <td>48.0000</td>
      <td>-1.0</td>
      <td>IMISLY</td>
      <td></td>
      <td></td>
      <td>37914</td>
      <td>...</td>
      <td>None</td>
      <td>1955</td>
      <td>1991</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
<p>20 rows × 21 columns</p>
</div>



```python
#  How many stations are there in total?
total_stations = stations_enriched.count()
print("total_stations: ", total_stations)

# How many stations were active so far in 2025
active_2025 = stations_enriched.filter(F.col("LASTYEAR_ANY") >= 2025).count()
print("active stations in 2025: ", active_2025)
```

    total_stations:  129657
    active stations in 2025:  38481



```python
# station network belongings analysis
network_counts = stations_enriched.select(
    F.when(F.col("GSN_FLAG").contains("GSN"), 1).otherwise(0).alias("is_GSN"),
    F.when(F.col("HCN_CRN").contains("HCN"), 1).otherwise(0).alias("is_HCN"),
    F.when(F.col("HCN_CRN").contains("CRN"), 1).otherwise(0).alias("is_CRN"),
).agg(
    F.sum("is_GSN").alias("GSN"),
    F.sum("is_HCN").alias("HCN"),
    F.sum("is_CRN").alias("CRN"),
)

show_as_html(network_counts)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>GSN</th>
      <th>HCN</th>
      <th>CRN</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>991</td>
      <td>1218</td>
      <td>234</td>
    </tr>
  </tbody>
</table>
</div>



```python
# Are there any stations that are in more than one of these networks?
# multi-network station counts query critera: net_count >= 2
flags = stations_enriched.select(
    "ID",
    F.when(F.col("GSN_FLAG").contains("GSN"), 1).otherwise(0).alias("is_GSN"),
    F.when(F.col("HCN_CRN").contains("HCN"), 1).otherwise(0).alias("is_HCN"),
    F.when(F.col("HCN_CRN").contains("CRN"), 1).otherwise(0).alias("is_CRN"),
)

multi_network = (
    flags.withColumn("net_count", F.col("is_GSN") + F.col("is_HCN") + F.col("is_CRN"))
    .filter(F.col("net_count") >= 2)
    .select("ID", "is_GSN", "is_HCN", "is_CRN", "net_count")
)
show_as_html(multi_network)

print(
    f"multi_network number is {multi_network.count()}, and they all overlap with GSN and HCN."
)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>is_GSN</th>
      <th>is_HCN</th>
      <th>is_CRN</th>
      <th>net_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>USW00012921</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>USW00024128</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>USW00024213</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>USW00012836</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>USW00093193</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>USW00014771</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>6</th>
      <td>USW00014922</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>USW00094008</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>8</th>
      <td>USW00024144</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>9</th>
      <td>USW00003870</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>10</th>
      <td>USW00023044</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>11</th>
      <td>USW00013782</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>12</th>
      <td>USW00023051</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>13</th>
      <td>USW00014742</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>14</th>
      <td>USW00093729</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>


    multi_network number is 15, and they all overlap with GSN and HCN.


### (b)


```python
# How many stations are there in the Southern_Hemisphere?
southern_hemisphere = stations_enriched.filter(F.col("LATITUDE") < 0).count()
print("southern_hemisphere count: ", southern_hemisphere)
```

    southern_hemisphere count:  25357



```python
# identify us_territories, excluding the United States it self
# How many stations are there in total in the territories of the United States, excluding the United States itself?
us_territories = (
    stations_enriched.filter(
        (F.col("COUNTRY_NAME").contains("United States"))
        & (F.trim(F.col("COUNTRY_NAME")) != "United States")
    )
    .groupBy("COUNTRY_NAME")
    .agg(F.count("*").alias("STATION_NUM"))
)


show_as_html(us_territories)
us_territories_station_num = us_territories.agg(
    F.sum("station_num").alias("us_territories_station_num")
)
show_as_html(us_territories_station_num)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>COUNTRY_NAME</th>
      <th>STATION_NUM</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Northern Mariana Islands [United States]</td>
      <td>11</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Puerto Rico [United States]</td>
      <td>260</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Guam [United States]</td>
      <td>34</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Johnston Atoll [United States]</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>American Samoa [United States]</td>
      <td>21</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Virgin Islands [United States]</td>
      <td>77</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Midway Islands [United States}</td>
      <td>3</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Palmyra Atoll [United States]</td>
      <td>3</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Wake Island [United States]</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>us_territories_station_num</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>414</td>
    </tr>
  </tbody>
</table>
</div>


### (c)


```python
# groupby country and states and count, save
station_num_by_country = (
    stations_enriched.groupBy("COUNTRY_CODE", "COUNTRY_NAME")
    .agg(F.count("*").alias("STATION_NUM"))
    .orderBy(F.desc("STATION_NUM"))
)

show_as_html(station_num_by_country)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>COUNTRY_CODE</th>
      <th>COUNTRY_NAME</th>
      <th>STATION_NUM</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>US</td>
      <td>United States</td>
      <td>75846</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AS</td>
      <td>Australia</td>
      <td>17088</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CA</td>
      <td>Canada</td>
      <td>9269</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BR</td>
      <td>Brazil</td>
      <td>5989</td>
    </tr>
    <tr>
      <th>4</th>
      <td>MX</td>
      <td>Mexico</td>
      <td>5249</td>
    </tr>
    <tr>
      <th>5</th>
      <td>IN</td>
      <td>India</td>
      <td>3807</td>
    </tr>
    <tr>
      <th>6</th>
      <td>SW</td>
      <td>Sweden</td>
      <td>1721</td>
    </tr>
    <tr>
      <th>7</th>
      <td>SF</td>
      <td>South Africa</td>
      <td>1166</td>
    </tr>
    <tr>
      <th>8</th>
      <td>RS</td>
      <td>Russia</td>
      <td>1123</td>
    </tr>
    <tr>
      <th>9</th>
      <td>GM</td>
      <td>Germany</td>
      <td>1123</td>
    </tr>
    <tr>
      <th>10</th>
      <td>FI</td>
      <td>Finland</td>
      <td>922</td>
    </tr>
    <tr>
      <th>11</th>
      <td>NO</td>
      <td>Norway</td>
      <td>461</td>
    </tr>
    <tr>
      <th>12</th>
      <td>NL</td>
      <td>Netherlands</td>
      <td>386</td>
    </tr>
    <tr>
      <th>13</th>
      <td>KZ</td>
      <td>Kazakhstan</td>
      <td>329</td>
    </tr>
    <tr>
      <th>14</th>
      <td>WA</td>
      <td>Namibia</td>
      <td>283</td>
    </tr>
    <tr>
      <th>15</th>
      <td>RQ</td>
      <td>Puerto Rico [United States]</td>
      <td>260</td>
    </tr>
    <tr>
      <th>16</th>
      <td>CH</td>
      <td>China</td>
      <td>228</td>
    </tr>
    <tr>
      <th>17</th>
      <td>SP</td>
      <td>Spain</td>
      <td>207</td>
    </tr>
    <tr>
      <th>18</th>
      <td>UP</td>
      <td>Ukraine</td>
      <td>204</td>
    </tr>
    <tr>
      <th>19</th>
      <td>JA</td>
      <td>Japan</td>
      <td>202</td>
    </tr>
  </tbody>
</table>
</div>



```python
us_territories.write.parquet(station_count_us_terri_path, mode="overwrite")
station_count_by_country.write.parquet(station_count_by_country_path, mode="overwrite")
```


```python
# check save result
!hdfs dfs -ls -h  {station_count_us_terri_path}
!hdfs dfs -ls -h  {station_count_by_country_path}
```

    Found 2 items
    -rw-r--r--   1 yxi75 supergroup          0 2025-09-11 22:33 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/station_count_us_terri_parquet/_SUCCESS
    -rw-r--r--   1 yxi75 supergroup      1.0 K 2025-09-11 22:33 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/station_count_us_terri_parquet/part-00000-d18e990b-4a7a-473d-9af2-026bd89f18b8-c000.snappy.parquet
    Found 2 items
    -rw-r--r--   1 yxi75 supergroup          0 2025-09-11 22:33 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/station_count_by_country_parquet/_SUCCESS
    -rw-r--r--   1 yxi75 supergroup      4.9 K 2025-09-11 22:33 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/station_count_by_country_parquet/part-00000-a9a82280-57a4-4320-b10d-01995f268f70-c000.snappy.parquet



```python
# load countries meta table for join
countries_df = (
    spark.read.text(paths["countries"])
    .withColumn("CODE", F.substring("value", 1, 2))
    .withColumn("COUNTRY_NAME", F.substring("value", 4, 61))
    .withColumnRenamed("CODE", "COUNTRY_CODE")
    .drop("value")
)

# join country counts onto countries
country_meta_with_station_num = countries_df.join(
    station_count_by_country.withColumnRenamed("count", "STATION_NUM"),
    on=["COUNTRY_NAME", "COUNTRY_CODE"],
    how="left",
).orderBy("COUNTRY_NAME")

show_as_html(country_meta_with_station_num)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>COUNTRY_NAME</th>
      <th>COUNTRY_CODE</th>
      <th>STATION_NUM</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Albania</td>
      <td>AL</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Algeria</td>
      <td>AG</td>
      <td>87</td>
    </tr>
    <tr>
      <th>3</th>
      <td>American Samoa [United States]</td>
      <td>AQ</td>
      <td>21</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Angola</td>
      <td>AO</td>
      <td>6</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Antarctica</td>
      <td>AY</td>
      <td>102</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Antigua and Barbuda</td>
      <td>AC</td>
      <td>2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Argentina</td>
      <td>AR</td>
      <td>101</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Armenia</td>
      <td>AM</td>
      <td>53</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Australia</td>
      <td>AS</td>
      <td>17088</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Austria</td>
      <td>AU</td>
      <td>13</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Azerbaijan</td>
      <td>AJ</td>
      <td>66</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Bahamas, The</td>
      <td>BF</td>
      <td>72</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Bahrain</td>
      <td>BA</td>
      <td>1</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Bangladesh</td>
      <td>BG</td>
      <td>10</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Barbados</td>
      <td>BB</td>
      <td>1</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Belarus</td>
      <td>BO</td>
      <td>51</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Belgium</td>
      <td>BE</td>
      <td>1</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Belize</td>
      <td>BH</td>
      <td>1</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Benin</td>
      <td>BN</td>
      <td>9</td>
    </tr>
  </tbody>
</table>
</div>



```python
# save to country_meta_with_station_num_path
country_meta_with_station_num.write.parquet(
    country_meta_with_station_num_path, "overwrite"
)
```

    25/09/11 22:55:21 WARN AzureFileSystemThreadPoolExecutor: Disabling threads for Delete operation as thread count 0 is <= 1



```python
!hdfs dfs -ls {country_meta_with_station_num_path}
```

    Found 2 items
    -rw-r--r--   1 yxi75 supergroup          0 2025-09-11 22:55 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/country_meta_with_station_num/_SUCCESS
    -rw-r--r--   1 yxi75 supergroup       4945 2025-09-11 22:55 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/country_meta_with_station_num/part-00000-c99a29ad-f800-4baf-983d-16f1b86a58a2-c000.snappy.parquet



```python
station_count_by_state = stations_enriched.groupBy("STATE", "STATE_NAME").agg(
    F.count("*").alias("STATION_NUM")
)
show_as_html(station_count_by_state, 3)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STATE</th>
      <th>STATE_NAME</th>
      <th>STATION_NUM</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NS</td>
      <td>NOVA SCOTIA</td>
      <td>398</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NE</td>
      <td>NEBRASKA</td>
      <td>2436</td>
    </tr>
    <tr>
      <th>2</th>
      <td>IA</td>
      <td>IOWA</td>
      <td>1106</td>
    </tr>
  </tbody>
</table>
</div>



```python
states_df = (
    spark.read.text(paths["states"])
    .withColumn("CODE", F.substring("value", 1, 2))
    .withColumn("STATE_NAME", F.substring("value", 4, 47))
    .drop("value")
)
show_as_html(states_df, 3)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>CODE</th>
      <th>STATE_NAME</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AB</td>
      <td>ALBERTA</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AK</td>
      <td>ALASKA</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AL</td>
      <td>ALABAMA</td>
    </tr>
  </tbody>
</table>
</div>



```python
states_meta_with_station_num = (
    states_df.withColumnRenamed("CODE", "STATE_CODE")
    .join(
        station_count_by_state.withColumnRenamed("STATE", "STATE_CODE"),
        on=["STATE_NAME", "STATE_CODE"],
        how="left",
    )
    .orderBy(F.desc("STATION_NUM"))
)

show_as_html(states_meta_with_station_num)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STATE_NAME</th>
      <th>STATE_CODE</th>
      <th>STATION_NUM</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>TEXAS</td>
      <td>TX</td>
      <td>6472</td>
    </tr>
    <tr>
      <th>1</th>
      <td>COLORADO</td>
      <td>CO</td>
      <td>4784</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CALIFORNIA</td>
      <td>CA</td>
      <td>3166</td>
    </tr>
    <tr>
      <th>3</th>
      <td>NORTH CAROLINA</td>
      <td>NC</td>
      <td>2747</td>
    </tr>
    <tr>
      <th>4</th>
      <td>MINNESOTA</td>
      <td>MN</td>
      <td>2675</td>
    </tr>
    <tr>
      <th>5</th>
      <td>NEBRASKA</td>
      <td>NE</td>
      <td>2436</td>
    </tr>
    <tr>
      <th>6</th>
      <td>KANSAS</td>
      <td>KS</td>
      <td>2401</td>
    </tr>
    <tr>
      <th>7</th>
      <td>NEW MEXICO</td>
      <td>NM</td>
      <td>2295</td>
    </tr>
    <tr>
      <th>8</th>
      <td>FLORIDA</td>
      <td>FL</td>
      <td>2244</td>
    </tr>
    <tr>
      <th>9</th>
      <td>ILLINOIS</td>
      <td>IL</td>
      <td>2234</td>
    </tr>
    <tr>
      <th>10</th>
      <td>OREGON</td>
      <td>OR</td>
      <td>2031</td>
    </tr>
    <tr>
      <th>11</th>
      <td>ONTARIO</td>
      <td>ON</td>
      <td>2021</td>
    </tr>
    <tr>
      <th>12</th>
      <td>INDIANA</td>
      <td>IN</td>
      <td>2020</td>
    </tr>
    <tr>
      <th>13</th>
      <td>NEW YORK</td>
      <td>NY</td>
      <td>1912</td>
    </tr>
    <tr>
      <th>14</th>
      <td>TENNESSEE</td>
      <td>TN</td>
      <td>1755</td>
    </tr>
    <tr>
      <th>15</th>
      <td>BRITISH COLUMBIA</td>
      <td>BC</td>
      <td>1720</td>
    </tr>
    <tr>
      <th>16</th>
      <td>WASHINGTON</td>
      <td>WA</td>
      <td>1694</td>
    </tr>
    <tr>
      <th>17</th>
      <td>ARIZONA</td>
      <td>AZ</td>
      <td>1692</td>
    </tr>
    <tr>
      <th>18</th>
      <td>PENNSYLVANIA</td>
      <td>PA</td>
      <td>1641</td>
    </tr>
    <tr>
      <th>19</th>
      <td>MISSOURI</td>
      <td>MO</td>
      <td>1624</td>
    </tr>
  </tbody>
</table>
</div>



```python
states_meta_with_station_num.write.parquet(
    states_meta_with_station_num_path, "overwrite"
)
```

    25/09/11 23:18:14 WARN AzureFileSystemThreadPoolExecutor: Disabling threads for Delete operation as thread count 0 is <= 1



```python
# check USER_ROOT storage status
!hdfs dfs -ls -h {USER_ROOT}
```

    Found 5 items
    drwxr-xr-x   - yxi75 supergroup          0 2025-09-11 22:55 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/country_meta_with_station_num
    drwxr-xr-x   - yxi75 supergroup          0 2025-09-11 23:18 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/states_meta_with_station_num
    drwxr-xr-x   - yxi75 supergroup          0 2025-09-11 22:33 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/station_count_by_country_parquet
    drwxr-xr-x   - yxi75 supergroup          0 2025-09-11 22:33 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/station_count_us_terri_parquet
    drwxr-xr-x   - yxi75 supergroup          0 2025-09-11 18:30 wasbs://campus-user@madsstorage002.blob.core.windows.net/yxi75/stations_enriched_parquet


## Q2 Distance User Define Function

### (a) User Define Function: Haversine Distance

## 🌍 Haversine Distance Formula

The **Haversine formula** is used to calculate the great-circle distance between two points 
on a sphere (such as the Earth), given their latitude and longitude.

---

### 1. Definitions

Let two points be defined as:

- Point 1: $(\phi_1, \lambda_1)$  
- Point 2: $(\phi_2, \lambda_2)$  

where:
- $\phi$ = latitude (in radians)  
- $\lambda$ = longitude (in radians)  
- $R$ = radius of the Earth (mean radius ≈ 6,371 km)

---

### 2. Formula

The haversine function is defined as:

$$
\text{hav}(\theta) = \sin^2\!\left(\frac{\theta}{2}\right)
$$

Using this, the central angle $c$ between two points is:

$$
a = \sin^2\!\left(\frac{\Delta \phi}{2}\right) + 
    \cos(\phi_1) \cdot \cos(\phi_2) \cdot \sin^2\!\left(\frac{\Delta \lambda}{2}\right)
$$

$$
c = 2 \cdot \arcsin(\sqrt{a})
$$

Finally, the distance $d$ is:

$$
d = R \cdot c
$$

where:
- $\Delta \phi = \phi_2 - \phi_1$  
- $\Delta \lambda = \lambda_2 - \lambda_1$

---

### 3. Notes

- The Haversine formula accounts for Earth’s curvature, making it more accurate 
  than a simple Euclidean distance in geographic applications.  
- It is sufficiently precise for most data science and GIS tasks.  
- For centimeter-level accuracy (e.g., geodesy, GPS), ellipsoidal models such as 
  **Vincenty’s formula** or the **WGS84 geodesic** method should be used.


  (This part is generated by chatGPT)

### Haversine UDF


```python
# Haversine UDF
from math import asin, cos, radians, sin, sqrt

from pyspark.sql.functions import udf


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088  # mean Earth radius in km
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    c = 2 * asin(sqrt(a))
    return float(R * c)


# register UDF
haversine_udf = udf(haversine_km, DoubleType())
```

### test udf within subset stations


```python
# test udf within subset stations
test_stations = stations_enriched.limit(10)
show_as_html(test_stations)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>STATE</th>
      <th>COUNTRY_CODE</th>
      <th>LATITUDE</th>
      <th>LONGITUDE</th>
      <th>ELEVATION</th>
      <th>NAME</th>
      <th>GSN_FLAG</th>
      <th>HCN_CRN</th>
      <th>WMO_ID</th>
      <th>...</th>
      <th>STATE_NAME</th>
      <th>FIRSTYEAR_ANY</th>
      <th>LASTYEAR_ANY</th>
      <th>N_ELEMENTS</th>
      <th>TMAX</th>
      <th>TMIN</th>
      <th>PRCP</th>
      <th>SNOW</th>
      <th>SNWD</th>
      <th>N_CORE_ELEMENTS</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AE000041196</td>
      <td></td>
      <td>AE</td>
      <td>25.3330</td>
      <td>55.5170</td>
      <td>34.0</td>
      <td>SHARJAH INTER. AIRP</td>
      <td>GSN</td>
      <td></td>
      <td>41196</td>
      <td>...</td>
      <td>None</td>
      <td>1944</td>
      <td>2025</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AEM00041218</td>
      <td></td>
      <td>AE</td>
      <td>24.2620</td>
      <td>55.6090</td>
      <td>264.9</td>
      <td>AL AIN INTL</td>
      <td></td>
      <td></td>
      <td>41218</td>
      <td>...</td>
      <td>None</td>
      <td>1994</td>
      <td>2025</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AGE00147715</td>
      <td></td>
      <td>AG</td>
      <td>35.4200</td>
      <td>8.1197</td>
      <td>863.0</td>
      <td>TEBESSA</td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td>None</td>
      <td>1879</td>
      <td>1938</td>
      <td>3</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AGE00147717</td>
      <td></td>
      <td>AG</td>
      <td>35.2000</td>
      <td>0.6300</td>
      <td>476.0</td>
      <td>SIDI-BEL-ABBES</td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td>None</td>
      <td>1880</td>
      <td>1938</td>
      <td>3</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AGE00147719</td>
      <td></td>
      <td>AG</td>
      <td>33.7997</td>
      <td>2.8900</td>
      <td>767.0</td>
      <td>LAGHOUAT</td>
      <td></td>
      <td></td>
      <td>60545</td>
      <td>...</td>
      <td>None</td>
      <td>1888</td>
      <td>2025</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>AGM00060425</td>
      <td></td>
      <td>AG</td>
      <td>36.2130</td>
      <td>1.3320</td>
      <td>141.1</td>
      <td>ECH CHELIFF</td>
      <td></td>
      <td></td>
      <td>60425</td>
      <td>...</td>
      <td>None</td>
      <td>1943</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>6</th>
      <td>AGM00060430</td>
      <td></td>
      <td>AG</td>
      <td>36.3000</td>
      <td>2.2330</td>
      <td>721.0</td>
      <td>MILIANA</td>
      <td></td>
      <td></td>
      <td>60430</td>
      <td>...</td>
      <td>None</td>
      <td>1957</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>7</th>
      <td>AGM00060490</td>
      <td></td>
      <td>AG</td>
      <td>35.6240</td>
      <td>-0.6210</td>
      <td>89.9</td>
      <td>ES SENIA</td>
      <td></td>
      <td></td>
      <td>60490</td>
      <td>...</td>
      <td>None</td>
      <td>1957</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>8</th>
      <td>AGM00060514</td>
      <td></td>
      <td>AG</td>
      <td>35.1670</td>
      <td>2.3170</td>
      <td>801.0</td>
      <td>KSAR CHELLALA</td>
      <td></td>
      <td></td>
      <td>60514</td>
      <td>...</td>
      <td>None</td>
      <td>1995</td>
      <td>2025</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>9</th>
      <td>AGM00060515</td>
      <td></td>
      <td>AG</td>
      <td>35.3330</td>
      <td>4.2060</td>
      <td>459.0</td>
      <td>BOU SAADA</td>
      <td></td>
      <td></td>
      <td>60515</td>
      <td>...</td>
      <td>None</td>
      <td>1984</td>
      <td>2025</td>
      <td>4</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
<p>10 rows × 21 columns</p>
</div>



```python
# generate station pairs

# SELF CROSS JOIN within test_stations
left = test_stations.select(
    F.col("ID").alias("ID_A"),
    F.col("NAME").alias("NAME_A"),
    F.col("LATITUDE").alias("LAT_A"),
    F.col("LONGITUDE").alias("LON_A"),
)
right = test_stations.select(
    F.col("ID").alias("ID_B"),
    F.col("NAME").alias("NAME_B"),
    F.col("LATITUDE").alias("LAT_B"),
    F.col("LONGITUDE").alias("LON_B"),
)

test_pairs = left.crossJoin(right).filter(F.col("ID_A") < F.col("ID_B"))

test_pairs = test_pairs.withColumn(
    "DIST_KM", haversine_udf("LAT_A", "LON_A", "LAT_B", "LON_B")
)
show_as_html(test_pairs)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID_A</th>
      <th>NAME_A</th>
      <th>LAT_A</th>
      <th>LON_A</th>
      <th>ID_B</th>
      <th>NAME_B</th>
      <th>LAT_B</th>
      <th>LON_B</th>
      <th>DIST_KM</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AG000060680</td>
      <td>TAMANRASSET</td>
      <td>22.800</td>
      <td>5.4331</td>
      <td>AGE00147794</td>
      <td>BEJAIA-CAP CARBON</td>
      <td>36.780</td>
      <td>5.100</td>
      <td>1554.836252</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AG000060680</td>
      <td>TAMANRASSET</td>
      <td>22.800</td>
      <td>5.4331</td>
      <td>AGM00060351</td>
      <td>JIJEL</td>
      <td>36.795</td>
      <td>5.874</td>
      <td>1556.750841</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AG000060680</td>
      <td>TAMANRASSET</td>
      <td>22.800</td>
      <td>5.4331</td>
      <td>AGM00060353</td>
      <td>JIJEL-PORT</td>
      <td>36.817</td>
      <td>5.883</td>
      <td>1559.219777</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AG000060680</td>
      <td>TAMANRASSET</td>
      <td>22.800</td>
      <td>5.4331</td>
      <td>AGM00060419</td>
      <td>MOHAMED BOUDIAF INTL</td>
      <td>36.276</td>
      <td>6.620</td>
      <td>1502.817981</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AG000060680</td>
      <td>TAMANRASSET</td>
      <td>22.800</td>
      <td>5.4331</td>
      <td>AGM00060550</td>
      <td>EL-BAYADH</td>
      <td>33.667</td>
      <td>1.000</td>
      <td>1283.611711</td>
    </tr>
    <tr>
      <th>5</th>
      <td>AG000060680</td>
      <td>TAMANRASSET</td>
      <td>22.800</td>
      <td>5.4331</td>
      <td>AGM00060563</td>
      <td>HASSIR'MEL</td>
      <td>32.933</td>
      <td>3.283</td>
      <td>1146.297735</td>
    </tr>
    <tr>
      <th>6</th>
      <td>AG000060680</td>
      <td>TAMANRASSET</td>
      <td>22.800</td>
      <td>5.4331</td>
      <td>AGM00060670</td>
      <td>TISKA</td>
      <td>24.293</td>
      <td>9.452</td>
      <td>442.002990</td>
    </tr>
    <tr>
      <th>7</th>
      <td>AG000060680</td>
      <td>TAMANRASSET</td>
      <td>22.800</td>
      <td>5.4331</td>
      <td>AJ000037639</td>
      <td>AGSTAPHA AIRPORT</td>
      <td>41.133</td>
      <td>45.417</td>
      <td>4236.617857</td>
    </tr>
    <tr>
      <th>8</th>
      <td>AG000060680</td>
      <td>TAMANRASSET</td>
      <td>22.800</td>
      <td>5.4331</td>
      <td>AJ000037656</td>
      <td>ADJINAURSKAYA_STEP'</td>
      <td>41.200</td>
      <td>46.800</td>
      <td>4350.094399</td>
    </tr>
    <tr>
      <th>9</th>
      <td>AGE00147794</td>
      <td>BEJAIA-CAP CARBON</td>
      <td>36.780</td>
      <td>5.1000</td>
      <td>AGM00060351</td>
      <td>JIJEL</td>
      <td>36.795</td>
      <td>5.874</td>
      <td>68.946174</td>
    </tr>
    <tr>
      <th>10</th>
      <td>AGE00147794</td>
      <td>BEJAIA-CAP CARBON</td>
      <td>36.780</td>
      <td>5.1000</td>
      <td>AGM00060353</td>
      <td>JIJEL-PORT</td>
      <td>36.817</td>
      <td>5.883</td>
      <td>69.838733</td>
    </tr>
    <tr>
      <th>11</th>
      <td>AGE00147794</td>
      <td>BEJAIA-CAP CARBON</td>
      <td>36.780</td>
      <td>5.1000</td>
      <td>AGM00060419</td>
      <td>MOHAMED BOUDIAF INTL</td>
      <td>36.276</td>
      <td>6.620</td>
      <td>146.921796</td>
    </tr>
    <tr>
      <th>12</th>
      <td>AGE00147794</td>
      <td>BEJAIA-CAP CARBON</td>
      <td>36.780</td>
      <td>5.1000</td>
      <td>AGM00060550</td>
      <td>EL-BAYADH</td>
      <td>33.667</td>
      <td>1.000</td>
      <td>508.348378</td>
    </tr>
    <tr>
      <th>13</th>
      <td>AGE00147794</td>
      <td>BEJAIA-CAP CARBON</td>
      <td>36.780</td>
      <td>5.1000</td>
      <td>AGM00060563</td>
      <td>HASSIR'MEL</td>
      <td>32.933</td>
      <td>3.283</td>
      <td>458.743957</td>
    </tr>
    <tr>
      <th>14</th>
      <td>AGE00147794</td>
      <td>BEJAIA-CAP CARBON</td>
      <td>36.780</td>
      <td>5.1000</td>
      <td>AGM00060670</td>
      <td>TISKA</td>
      <td>24.293</td>
      <td>9.452</td>
      <td>1449.209308</td>
    </tr>
    <tr>
      <th>15</th>
      <td>AGE00147794</td>
      <td>BEJAIA-CAP CARBON</td>
      <td>36.780</td>
      <td>5.1000</td>
      <td>AJ000037639</td>
      <td>AGSTAPHA AIRPORT</td>
      <td>41.133</td>
      <td>45.417</td>
      <td>3488.136797</td>
    </tr>
    <tr>
      <th>16</th>
      <td>AGE00147794</td>
      <td>BEJAIA-CAP CARBON</td>
      <td>36.780</td>
      <td>5.1000</td>
      <td>AJ000037656</td>
      <td>ADJINAURSKAYA_STEP'</td>
      <td>41.200</td>
      <td>46.800</td>
      <td>3602.576212</td>
    </tr>
    <tr>
      <th>17</th>
      <td>AGM00060351</td>
      <td>JIJEL</td>
      <td>36.795</td>
      <td>5.8740</td>
      <td>AGM00060353</td>
      <td>JIJEL-PORT</td>
      <td>36.817</td>
      <td>5.883</td>
      <td>2.574176</td>
    </tr>
    <tr>
      <th>18</th>
      <td>AGM00060351</td>
      <td>JIJEL</td>
      <td>36.795</td>
      <td>5.8740</td>
      <td>AGM00060419</td>
      <td>MOHAMED BOUDIAF INTL</td>
      <td>36.276</td>
      <td>6.620</td>
      <td>88.162740</td>
    </tr>
    <tr>
      <th>19</th>
      <td>AGM00060351</td>
      <td>JIJEL</td>
      <td>36.795</td>
      <td>5.8740</td>
      <td>AGM00060550</td>
      <td>EL-BAYADH</td>
      <td>33.667</td>
      <td>1.000</td>
      <td>562.845643</td>
    </tr>
  </tbody>
</table>
</div>



```python
# select New Zealand stations
nz_stations = stations_enriched.filter(
    F.col("COUNTRY_NAME").contains("New Zealand")
).select("ID", "NAME", "LATITUDE", "LONGITUDE")

# SELF CROSS JOIN within newzealand
left = nz_stations.select(
    F.col("ID").alias("ID_A"),
    F.col("NAME").alias("NAME_A"),
    F.col("LATITUDE").alias("LAT_A"),
    F.col("LONGITUDE").alias("LON_A"),
)
right = nz_stations.select(
    F.col("ID").alias("ID_B"),
    F.col("NAME").alias("NAME_B"),
    F.col("LATITUDE").alias("LAT_B"),
    F.col("LONGITUDE").alias("LON_B"),
)

pairs = left.crossJoin(right).filter(F.col("ID_A") < F.col("ID_B"))

pairs = pairs.withColumn("DIST_KM", haversine_udf("LAT_A", "LON_A", "LAT_B", "LON_B"))
```


```python
show_as_html(pairs)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID_A</th>
      <th>NAME_A</th>
      <th>LAT_A</th>
      <th>LON_A</th>
      <th>ID_B</th>
      <th>NAME_B</th>
      <th>LAT_B</th>
      <th>LON_B</th>
      <th>DIST_KM</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NZ000936150</td>
      <td>HOKITIKA AERODROME</td>
      <td>-42.717</td>
      <td>170.983</td>
      <td>NZ000937470</td>
      <td>TARA HILLS</td>
      <td>-44.517</td>
      <td>169.900</td>
      <td>218.309321</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NZ000936150</td>
      <td>HOKITIKA AERODROME</td>
      <td>-42.717</td>
      <td>170.983</td>
      <td>NZ000939870</td>
      <td>CHATHAM ISLANDS AWS</td>
      <td>-43.950</td>
      <td>-176.567</td>
      <td>1015.251566</td>
    </tr>
    <tr>
      <th>2</th>
      <td>NZ000936150</td>
      <td>HOKITIKA AERODROME</td>
      <td>-42.717</td>
      <td>170.983</td>
      <td>NZM00093781</td>
      <td>CHRISTCHURCH INTL</td>
      <td>-43.489</td>
      <td>172.532</td>
      <td>152.258567</td>
    </tr>
    <tr>
      <th>3</th>
      <td>NZ000936150</td>
      <td>HOKITIKA AERODROME</td>
      <td>-42.717</td>
      <td>170.983</td>
      <td>NZM00093110</td>
      <td>AUCKLAND AERO AWS</td>
      <td>-37.000</td>
      <td>174.800</td>
      <td>714.127832</td>
    </tr>
    <tr>
      <th>4</th>
      <td>NZ000936150</td>
      <td>HOKITIKA AERODROME</td>
      <td>-42.717</td>
      <td>170.983</td>
      <td>TL000091724</td>
      <td>NUKUNONO</td>
      <td>-9.200</td>
      <td>-171.917</td>
      <td>4082.088199</td>
    </tr>
    <tr>
      <th>5</th>
      <td>NZ000936150</td>
      <td>HOKITIKA AERODROME</td>
      <td>-42.717</td>
      <td>170.983</td>
      <td>NZ000939450</td>
      <td>CAMPBELL ISLAND AWS</td>
      <td>-52.550</td>
      <td>169.167</td>
      <td>1101.720586</td>
    </tr>
    <tr>
      <th>6</th>
      <td>NZ000936150</td>
      <td>HOKITIKA AERODROME</td>
      <td>-42.717</td>
      <td>170.983</td>
      <td>NZM00093439</td>
      <td>WELLINGTON AERO AWS</td>
      <td>-41.333</td>
      <td>174.800</td>
      <td>350.796507</td>
    </tr>
    <tr>
      <th>7</th>
      <td>NZ000936150</td>
      <td>HOKITIKA AERODROME</td>
      <td>-42.717</td>
      <td>170.983</td>
      <td>NZM00093929</td>
      <td>ENDERBY ISLAND AWS</td>
      <td>-50.483</td>
      <td>166.300</td>
      <td>934.248833</td>
    </tr>
    <tr>
      <th>8</th>
      <td>NZ000936150</td>
      <td>HOKITIKA AERODROME</td>
      <td>-42.717</td>
      <td>170.983</td>
      <td>NZM00093678</td>
      <td>KAIKOURA</td>
      <td>-42.417</td>
      <td>173.700</td>
      <td>224.981589</td>
    </tr>
    <tr>
      <th>9</th>
      <td>NZ000937470</td>
      <td>TARA HILLS</td>
      <td>-44.517</td>
      <td>169.900</td>
      <td>NZ000939870</td>
      <td>CHATHAM ISLANDS AWS</td>
      <td>-43.950</td>
      <td>-176.567</td>
      <td>1078.799953</td>
    </tr>
    <tr>
      <th>10</th>
      <td>NZ000937470</td>
      <td>TARA HILLS</td>
      <td>-44.517</td>
      <td>169.900</td>
      <td>NZM00093781</td>
      <td>CHRISTCHURCH INTL</td>
      <td>-43.489</td>
      <td>172.532</td>
      <td>239.530461</td>
    </tr>
    <tr>
      <th>11</th>
      <td>NZ000939870</td>
      <td>CHATHAM ISLANDS AWS</td>
      <td>-43.950</td>
      <td>-176.567</td>
      <td>NZM00093781</td>
      <td>CHRISTCHURCH INTL</td>
      <td>-43.489</td>
      <td>172.532</td>
      <td>876.909075</td>
    </tr>
    <tr>
      <th>12</th>
      <td>NZ000937470</td>
      <td>TARA HILLS</td>
      <td>-44.517</td>
      <td>169.900</td>
      <td>NZM00093110</td>
      <td>AUCKLAND AERO AWS</td>
      <td>-37.000</td>
      <td>174.800</td>
      <td>931.744249</td>
    </tr>
    <tr>
      <th>13</th>
      <td>NZ000939870</td>
      <td>CHATHAM ISLANDS AWS</td>
      <td>-43.950</td>
      <td>-176.567</td>
      <td>NZM00093110</td>
      <td>AUCKLAND AERO AWS</td>
      <td>-37.000</td>
      <td>174.800</td>
      <td>1062.046480</td>
    </tr>
    <tr>
      <th>14</th>
      <td>NZ000937470</td>
      <td>TARA HILLS</td>
      <td>-44.517</td>
      <td>169.900</td>
      <td>TL000091724</td>
      <td>NUKUNONO</td>
      <td>-9.200</td>
      <td>-171.917</td>
      <td>4299.297371</td>
    </tr>
    <tr>
      <th>15</th>
      <td>NZ000937470</td>
      <td>TARA HILLS</td>
      <td>-44.517</td>
      <td>169.900</td>
      <td>NZ000939450</td>
      <td>CAMPBELL ISLAND AWS</td>
      <td>-52.550</td>
      <td>169.167</td>
      <td>894.846229</td>
    </tr>
    <tr>
      <th>16</th>
      <td>NZ000937470</td>
      <td>TARA HILLS</td>
      <td>-44.517</td>
      <td>169.900</td>
      <td>NZM00093439</td>
      <td>WELLINGTON AERO AWS</td>
      <td>-41.333</td>
      <td>174.800</td>
      <td>533.227413</td>
    </tr>
    <tr>
      <th>17</th>
      <td>NZ000937470</td>
      <td>TARA HILLS</td>
      <td>-44.517</td>
      <td>169.900</td>
      <td>NZM00093929</td>
      <td>ENDERBY ISLAND AWS</td>
      <td>-50.483</td>
      <td>166.300</td>
      <td>716.176359</td>
    </tr>
    <tr>
      <th>18</th>
      <td>NZ000939870</td>
      <td>CHATHAM ISLANDS AWS</td>
      <td>-43.950</td>
      <td>-176.567</td>
      <td>TL000091724</td>
      <td>NUKUNONO</td>
      <td>-9.200</td>
      <td>-171.917</td>
      <td>3890.098209</td>
    </tr>
    <tr>
      <th>19</th>
      <td>NZ000939870</td>
      <td>CHATHAM ISLANDS AWS</td>
      <td>-43.950</td>
      <td>-176.567</td>
      <td>NZM00093439</td>
      <td>WELLINGTON AERO AWS</td>
      <td>-41.333</td>
      <td>174.800</td>
      <td>763.267727</td>
    </tr>
  </tbody>
</table>
</div>


### (b) closest station pair in NZ


```python
closest_pair = pairs.orderBy(F.col("DIST_KM").asc()).limit(1)
closest_pair.show(truncate=False)

stationA = closest_pair.first()["ID_A"]
stationB = closest_pair.first()["ID_B"]
print(f"The closest station pair in New Zealand is station {stationA} and {stationB}")
```

    +-----------+------------------------------+-----+-------+-----------+------------------------------+-------+-----+-----------------+
    |ID_A       |NAME_A                        |LAT_A|LON_A  |ID_B       |NAME_B                        |LAT_B  |LON_B|DIST_KM          |
    +-----------+------------------------------+-----+-------+-----------+------------------------------+-------+-----+-----------------+
    |NZ000093417|PARAPARAUMU AWS               |-40.9|174.983|NZM00093439|WELLINGTON AERO AWS           |-41.333|174.8|50.52909627580285|
    +-----------+------------------------------+-----+-------+-----------+------------------------------+-------+-----+-----------------+
    
    The closest station pair in New Zealand is station NZ000093417 and NZM00093439


## Q3 Daily Climate Summaries Study

### (a) Core Element stat


```python
# load daily
daily_schema = StructType(
    [
        StructField("ID", StringType()),  # Character Station code
        StructField(
            "DATE", StringType()
        ),  # Date Observation date formatted as YYYYMMDD
        StructField("ELEMENT", StringType()),  # Character Element type indicator
        StructField("VALUE", DoubleType()),  # Real Data value for ELEMENT
        StructField("MEASUREMENT", StringType()),  # Character Measurement Flag
        StructField("QUALITY", StringType()),  # Character Quality Flag
        StructField("SOURCE", StringType()),  # Character Source Flag
        StructField("TIME", StringType()),  # Time Observation time formatted as HHMM
    ]
)

daily = spark.read.csv(paths["daily"], schema=daily_schema)

show_as_html(daily)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>DATE</th>
      <th>ELEMENT</th>
      <th>VALUE</th>
      <th>MEASUREMENT</th>
      <th>QUALITY</th>
      <th>SOURCE</th>
      <th>TIME</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ASN00030019</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>24.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ASN00030021</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>200.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ASN00030022</td>
      <td>20100101</td>
      <td>TMAX</td>
      <td>294.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ASN00030022</td>
      <td>20100101</td>
      <td>TMIN</td>
      <td>215.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ASN00030022</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>408.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>ASN00029121</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>820.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>6</th>
      <td>ASN00029126</td>
      <td>20100101</td>
      <td>TMAX</td>
      <td>371.0</td>
      <td>None</td>
      <td>None</td>
      <td>S</td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>ASN00029126</td>
      <td>20100101</td>
      <td>TMIN</td>
      <td>225.0</td>
      <td>None</td>
      <td>None</td>
      <td>S</td>
      <td>None</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ASN00029126</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>0.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>9</th>
      <td>ASN00029126</td>
      <td>20100101</td>
      <td>TAVG</td>
      <td>298.0</td>
      <td>H</td>
      <td>None</td>
      <td>S</td>
      <td>None</td>
    </tr>
    <tr>
      <th>10</th>
      <td>ASN00029127</td>
      <td>20100101</td>
      <td>TMAX</td>
      <td>371.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>11</th>
      <td>ASN00029127</td>
      <td>20100101</td>
      <td>TMIN</td>
      <td>225.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>12</th>
      <td>ASN00029127</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>8.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>13</th>
      <td>ASN00029129</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>174.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>14</th>
      <td>ASN00029130</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>86.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>15</th>
      <td>ASN00029131</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>56.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>16</th>
      <td>ASN00029132</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>800.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>17</th>
      <td>ASN00029136</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>22.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>18</th>
      <td>ASN00029137</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>0.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>19</th>
      <td>ASN00029139</td>
      <td>20100101</td>
      <td>TMAX</td>
      <td>298.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



```python
# filter daily records containing only the five core elements
core_elems = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"]
daily_core = daily.filter(F.col("ELEMENT").isin(core_elems))
show_as_html(daily_core)
```

                                                                                    


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>DATE</th>
      <th>ELEMENT</th>
      <th>VALUE</th>
      <th>MEASUREMENT</th>
      <th>QUALITY</th>
      <th>SOURCE</th>
      <th>TIME</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ASN00030019</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>24.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ASN00030021</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>200.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ASN00030022</td>
      <td>20100101</td>
      <td>TMAX</td>
      <td>294.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ASN00030022</td>
      <td>20100101</td>
      <td>TMIN</td>
      <td>215.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ASN00030022</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>408.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>ASN00029121</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>820.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>6</th>
      <td>ASN00029126</td>
      <td>20100101</td>
      <td>TMAX</td>
      <td>371.0</td>
      <td>None</td>
      <td>None</td>
      <td>S</td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>ASN00029126</td>
      <td>20100101</td>
      <td>TMIN</td>
      <td>225.0</td>
      <td>None</td>
      <td>None</td>
      <td>S</td>
      <td>None</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ASN00029126</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>0.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>9</th>
      <td>ASN00029127</td>
      <td>20100101</td>
      <td>TMAX</td>
      <td>371.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>10</th>
      <td>ASN00029127</td>
      <td>20100101</td>
      <td>TMIN</td>
      <td>225.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>11</th>
      <td>ASN00029127</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>8.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>12</th>
      <td>ASN00029129</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>174.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>13</th>
      <td>ASN00029130</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>86.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>14</th>
      <td>ASN00029131</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>56.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>15</th>
      <td>ASN00029132</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>800.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>16</th>
      <td>ASN00029136</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>22.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>17</th>
      <td>ASN00029137</td>
      <td>20100101</td>
      <td>PRCP</td>
      <td>0.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>18</th>
      <td>ASN00029139</td>
      <td>20100101</td>
      <td>TMAX</td>
      <td>298.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
    <tr>
      <th>19</th>
      <td>ASN00029139</td>
      <td>20100101</td>
      <td>TMIN</td>
      <td>270.0</td>
      <td>None</td>
      <td>None</td>
      <td>a</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



```python
# groupby ELEMENT count
elem_counts = (
    daily_core.groupBy("ELEMENT")
    .agg(F.count("*").alias("OBSERVATION_COUNT"))
    .orderBy(F.desc("OBSERVATION_COUNT"))
)

show_as_html(elem_counts)
```

                                                                                    


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ELEMENT</th>
      <th>OBSERVATION_COUNT</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>PRCP</td>
      <td>1084610240</td>
    </tr>
    <tr>
      <th>1</th>
      <td>TMAX</td>
      <td>461915395</td>
    </tr>
    <tr>
      <th>2</th>
      <td>TMIN</td>
      <td>460752965</td>
    </tr>
    <tr>
      <th>3</th>
      <td>SNOW</td>
      <td>361688529</td>
    </tr>
    <tr>
      <th>4</th>
      <td>SNWD</td>
      <td>302055219</td>
    </tr>
  </tbody>
</table>
</div>



```python
print(f"The most observations is PRCP.")
```

    The most observations is PRCP.


### (b) TMIN TMAX


```python
# filter records which reports TMAX but TMIN：base on （ID, DATE）
tmax = daily.filter(F.col("ELEMENT") == "TMAX").select(
    F.col("ID").alias("ID_T"), F.col("DATE").alias("DATE_T")
)

tmin = daily.filter(F.col("ELEMENT") == "TMIN").select(
    F.col("ID").alias("ID_N"), F.col("DATE").alias("DATE_N")
)
```


```python
# left_anti join to find the part which left(tmax) have but right(tmin) haven't
tmax_no_tmin = tmax.join(
    tmin, (tmax.ID_T == tmin.ID_N) & (tmax.DATE_T == tmin.DATE_N), how="left_anti"
)
tmax_no_tmin_count = tmax_no_tmin.count()

show_as_html(tmax_no_tmin)
```

                                                                                    


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID_T</th>
      <th>DATE_T</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AE000041196</td>
      <td>19560704</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AE000041196</td>
      <td>19570607</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AE000041196</td>
      <td>19580617</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AE000041196</td>
      <td>19590525</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AE000041196</td>
      <td>19650714</td>
    </tr>
    <tr>
      <th>5</th>
      <td>AE000041196</td>
      <td>19650813</td>
    </tr>
    <tr>
      <th>6</th>
      <td>AE000041196</td>
      <td>19770612</td>
    </tr>
    <tr>
      <th>7</th>
      <td>AE000041196</td>
      <td>19770904</td>
    </tr>
    <tr>
      <th>8</th>
      <td>AE000041196</td>
      <td>19780117</td>
    </tr>
    <tr>
      <th>9</th>
      <td>AE000041196</td>
      <td>19780619</td>
    </tr>
    <tr>
      <th>10</th>
      <td>AE000041196</td>
      <td>19780910</td>
    </tr>
    <tr>
      <th>11</th>
      <td>AE000041196</td>
      <td>19780918</td>
    </tr>
    <tr>
      <th>12</th>
      <td>AE000041196</td>
      <td>19781018</td>
    </tr>
    <tr>
      <th>13</th>
      <td>AE000041196</td>
      <td>19781024</td>
    </tr>
    <tr>
      <th>14</th>
      <td>AE000041196</td>
      <td>19781113</td>
    </tr>
    <tr>
      <th>15</th>
      <td>AE000041196</td>
      <td>19790215</td>
    </tr>
    <tr>
      <th>16</th>
      <td>AE000041196</td>
      <td>19790420</td>
    </tr>
    <tr>
      <th>17</th>
      <td>AE000041196</td>
      <td>19790508</td>
    </tr>
    <tr>
      <th>18</th>
      <td>AE000041196</td>
      <td>19790517</td>
    </tr>
    <tr>
      <th>19</th>
      <td>AE000041196</td>
      <td>19790609</td>
    </tr>
  </tbody>
</table>
</div>



```python
print(f"Number of TMAX observations without corresponding TMIN: {tmax_no_tmin_count}")
```

    Number of TMAX observations without corresponding TMIN: 10735252



```python
# unique stations which contribute to missing_pairs count
unique_stations_missing = tmax_no_tmin.select("ID_T").distinct().count()
show_as_html(unique_stations_missing)

print(f"tmax_no_tmin_count = {tmax_no_tmin_count}")
print(f"unique_stations_missing = {unique_stations_missing}")
```

    [Stage 233:==>           (16 + 8) / 107][Stage 234:>              (0 + 0) / 107]


```python
print(f"Number of unique stations contributing to TMAX observations without corresponding TMIN: {unique_stations_missing}")
```

    Number of unique stations contributing to TMAX observations without corresponding TMIN: 28751



```python
stop_spark()
```

    25/09/12 01:19:46 WARN ExecutorPodsWatchSnapshotSource: Kubernetes client has been closed.



<p><b>Spark</b></p><p>The spark session is <b><span style="color:red">stopped</span></b>, confirm that <code>yxi75 (notebook)</code> is under the completed applications section in the Spark UI.</p><ul><li><a href="http://mathmadslinux2p.canterbury.ac.nz:8080/" target="_blank">Spark UI</a></li></ul>



```python

```
