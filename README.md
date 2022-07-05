## Simple prometheus kafka group lag exporter

This tool exposes the lag kafka consumer group topics for prometheus. 
Notice the concept of lag only makes sense if you have consumers consuming
a topic within a group.

### Some cmds useful for testing:

Assuming you have a consumer running on topic X and consumer group my-group, you
can use the kafka cli tools to get the current lag:

```
$ kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group drio-group-1

 GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                                   HOST            CLIENT-ID
 drio-group-1    drio_test_go    0          15              15              0               main@air (github.com/segmentio/kafka-go)-094823fd-bb45-4fc8-a2de-1ca10ddee9e8 /172.23.0.1     main@air (github.com/segmentio/kafka-go)
```
