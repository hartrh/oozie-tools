# oozie-tools

Collection of tools to assit with [Oozie](http://oozie.apache.org/) workflow creation, submission, and reporting.

## Belt

Intelligent Oozie belt-fed job submission based on HDFS directory path input. Recursively searches for workflow XMLs and automagically generates companion job.properties to accompany workflow submission. Uses HDFS and Yarn HA values when detected.

##### Prerequisites

- User must have Oozie ProxyUser permissions in order to submit job
- Must be using [kitesdk](http://kitesdk.org/docs/current/) >= `1.1.0` libraries to utilize Hadoop HA
  - kite-data-core-1.1.0.jar
  - kite-data-hive-1.1.0.jar
  - kite-data-mapreduce-1.1.0.jar
  - kite-hadoop-compatibility-1.1.0.jar

##### Setup

```bash
# clone the repo
$ git clone https://github.com/hartrh/oozie-tools.git
```

Using the `templates/config_example.txt` config as an example, create a text file within the `config/` directory using the naming convention `provider_database_classification_catalog.txt`, with the appropriate values. All slashes (`/`) must be escaped (`\`).

```
# credentials and connection string for example.com
sql_user: imauser
sql_pass: imapassword
conn_string: jdbc:jtds:sqlserver:\/\/example.service._color.consul:1453\/catalog;domain=EXAMPLE.COM;useCursors=true
```

##### Usage

```bash
# execute belt
$ oozie-tools/belt.sh -w 'hdfs/path/to/recursively/search/for/workflows'
```
