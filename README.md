# prometheus-timestream-adapter

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

:warning: **There is a very early remote_reader version!** [AWS Timestream](https://aws.amazon.com/timestream) has a very powerful [query language](https://docs.aws.amazon.com/timestream/latest/developerguide/reference.html) and there is a [Grafana Plugin](https://grafana.com/grafana/plugins/grafana-timestream-datasource) supporting Timestream as a datasource. However, there is a very basic reader implementation.

```yaml
remote_read:
  - url: "http://prometheus-timestream-adapter:9201/read"
```

## Access Prometheus Timestream Database

The [Session](https://pkg.go.dev/github.com/aws/aws-sdk-go/aws/session) will attempt to load configuration and credentials from the environment, configuration files, and other credential sources.

### AWS Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowReadWriteToTable",
            "Effect": "Allow",
            "Action": [
                "timestream:WriteRecords",
                "timestream:Select"
            ],
            "Resource": "arn:aws:timestream:region:AccoundId:database/DatabaseName/table/TableName"
        },
        {
            "Sid": "AllowValueRead",
            "Effect": "Allow",
            "Action": "timestream:SelectValues",
            "Resource": "*"
        }
    ]
}
```

## FAQ

### What does the warning `Measure name exceeds the maximum supported length` mean?

The maximum number of characters for an AWS Timestream Dimension name is 256 bytes. Is a metric name is bigger than that it can't be written to AWS Timestream.

[Timestream Quotas](https://docs.aws.amazon.com/timestream/latest/developerguide/ts-limits.html)
  

