# mqpusher
The mqpusher tool is designed to transfer data from various sources to a single RabbitMQ queue. 
There is also option to execute js script on every data row before sending them to the queue. At the moment the following sources are supported:
- csv file - plain or compressed with gzip/zip
- json file (one json object per line) - plain or compressed with gzip/zip
- sql queries for the postgresql database

The configuration is defined through a configuration file, an example is `conf/config.yml`.
Only one of the source types must be specified at a time in the configuration.
Some settings can be overridden via flags:
```
-config string
    config file path (default "/etc/mqpusher/config.yml")
-csv_file string
    csv source file path
-json_file string
    json source file path
-script string
    script file path
```

