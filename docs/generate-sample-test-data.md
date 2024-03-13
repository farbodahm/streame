For development environment (and maybe later on for end-to-end tests),
I was thinking to use a tool for producing sample schema and
data that I want to Kafka instead of using a language
specific client (like [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)).

The main option that I had in mind, was to use Kafka Connect
([FileStreamSourceConnector](https://docs.confluent.io/platform/current/connect/filestream_connector.html) class)
to achieve the mentioned requirement.

After doing a qiuck POC, I decided to not continue with it and
use the Go client that I'm going to use also in the core
library and test suits. Reasons are:

1. That was not flexible enough to be run for testing purposes.
   * So I already have to use a library for test suits. So decided
   to not make it complicated to have 2 different ways for producing
   sample data in Kafka topics and stick to the library.
2. There wasn't neat ways to run it multiple times during tests
or local developments. (That's not what it's designed for.)

Long story shot, it was not a good way to integrate Kafka
Connect to workflow only for development and test setup.

Here I will provide the configs that I used during the POC for
future reference:

* Kafka Connect properties (`connect-standalone.properties`):
```properties
bootstrap.servers=localhost:9092

key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
plugin.path=/usr/local/share/kafka/plugins,/usr/share/filestream-connectors

offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
```

* File Source connector properties (`connect-file-source.properties`):
```properties
name=local-file-source
connector.class=org.apache.kafka.connect.file.FileStreamSourceConnector
tasks.max=1
file=/tmp/data.json
topic=my-topic
```

* Docker command to execute Kafka Connect:
```sh
docker run \
  --name=kafka-connect-file-source \
  --net=host \
  -v $(pwd)/connect-file-source.properties:/etc/kafka-connect/connect-file-source.properties \
  -v $(pwd)/data.json:/tmp/data.json \
  -v $(pwd)/connect-standalone.properties:/etc/kafka-connect/connect-standalone.properties confluentinc/cp-kafka-connect:latest \
  bash -c 'connect-standalone /etc/kafka-connect/connect-standalone.properties /etc/kafka-connect/connect-file-source.properties'
```