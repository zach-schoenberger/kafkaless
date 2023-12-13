# Kafkaless
## Install
* checkout the project: `git clone https://github.com/zach-schoenberger/kafkaless.git`
* run the install script: `./install.sh`
* add `$HOME/bin` to PATH: `export PATH=$PATH:$HOME/bin`
* Optional:
    - add aliases to .profile: `alias kl="kafkaless -b kafka03-prod02.messagehub.services.us-south.bluemix.net:9093 -s -u xxxx -p xxxx"`
## Options
```
usage: kafkaless -b <arg> [-c <arg>] [-F] [-g <arg>] [-h] [-i] [-l <arg>]
       [-o <arg>] [-P] [-p <arg>] [--pause] [--properties <properties>]
       [-r <arg>] [-s] -t <arg> [-u <arg>] [-X <arg>]
kafkaless
 -b,--broker <arg>              broker list [address:port]
 -c,--count <arg>               number of records to consume
 -F,--full                      print full consumer record
 -g,--group <arg>               consumer group name to use
 -h,--help                      help
 -i                             read stdin to publish to kafka topic
 -l,--file <arg>                file of lines to publish to kafka topic
 -o,--offset <arg>              offset to start at
                                [beginning|end|stored|<absolute
                                offset>|-<relative offset from
                                end>|@<timestamp in ms to start at>]
 -P,--publish                   publish to topic. requires either -l or -i
 -p,--password <arg>            ssl password
    --pause                     puases stream after displaying record.
                                must press enter for new record
    --properties <properties>   kafka properties
 -r,--regex <arg>               regex to filter by
 -s,--ssl                       enable ssl
 -t,--topic <arg>               topic
 -u,--user <arg>                ssl username
 -X <arg>                       additional kafka properties
```

## Example Usage
### Consume with regex
```
klprod -t page-views -r '.*"AID":10730.*'
```
### Produce from file
```
klqa -P -t page-views -l ./path/file.json
```
### Consume and Pipe to Another Cluster
```
klprod -t page-views -r '.*"AID":10730.*' | klqa -P -t page-views -i
```
### Reset Offsets of a queue's partitions to a point in time
```
klprod -s -t membership-updates -g membership-etl-consumer-prod-03 -o @1702429200000 -c 0
```
