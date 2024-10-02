### Tenzir exporter
---
Simple Prometheus exporter for Tenzir node metrics written in Python with Flask.
Tenzir links:
- [Tenzir GitHub](https://github.com/tenzir/tenzir)
- [Tenzir docs](https://docs.tenzir.com/get-started)
- [Exported metrics](https://docs.tenzir.com/operators/metrics).

### Files
---
|File|Description|
|---|---|
|`tenzir.json`|Grafana dashboard JSON|
|`tenzir_exporter.py`|Tenzir exporter|
---

### Requirements
To start work with exporter, you should install following Linux distro packages:
- gcc
- python3-devel
- uwsgi
- uwsgi-plugin-python3
- pushgateway

And then install Python PyPi dependencies from file:
```
pip3 install -r requirements.txt
```

### Usage
To test export for Tenzir node metrics you can use the following command:
```
./tenzir 'metrics --live | to http HOST_WITH_EXPORTER:8000 write json --compact-output'
```
If you want to create tenzir pipeline in Tenzir config to get metrics constantly, please use command specified above, like this:
```
  pipelines:
    metrics-to-prometheus:
      name: Export Tenzir own metrics
      definition: |
        metrics --live | to http HOST_WITH_EXPORTER:8000 write json --compact-output
      restart-on-error: true
```

If dependencies installation was successful, you can try to run exporter with the folllowing command:
```
uwsgi --http-socket 0.0.0.0:8000 --plugin python3 --wsgi-file tenzir_exporter.py --callable app --stats 0.0.0.0:9191
```
