# timeseries-mock-consumer

## setup

To install on OpenShift, run:

```bash
oc new-app centos/python-36-centos7~https://github.com/ruivieira/timeseries-mock-consumer \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_TOPIC=example \
  --name=listener
```