# prometheus-timestream-adapter

:warning: **This is a very early version**: Be very careful here!

Prometheus-timestream-adapter is a service which receives [Prometheus](https://github.com/prometheus) metrics through [`remote_write`](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write), and sends them into [AWS Timestream](https://aws.amazon.com/timestream).

## Building

```
go build
```

## Testing

```
go test
```

## Configuring Prometheus

To configure Prometheus to send samples to this binary, add the following to your `prometheus.yml`:

```yaml
remote_write:
  - url: "http://prometheus-timestream-adapter:9201/write"
```

### Why is there no remote_reader?

[AWS Timestream](https://aws.amazon.com/timestream) has a very powerful [query language](https://docs.aws.amazon.com/timestream/latest/developerguide/reference.html) and there is a [Grafana Plugin](https://grafana.com/grafana/plugins/grafana-timestream-datasource) supporting Timestream as a datasource. However, this is the reason why I don't think a reader implementation is needed.

## FAQ

### What does the warning `Measure name exceeds the maximum supported length` mean?

The maximum number of characters for an AWS Timestream Dimension name is 256 bytes. If a metric name is bigger than that it can't be written to AWS Timestream.

[Timestream Quotas](https://docs.aws.amazon.com/timestream/latest/developerguide/ts-limits.html)
  

