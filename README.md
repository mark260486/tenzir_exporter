### Tenzir exporter
---
Simple Prometheus exporter for Tenzir node metrics written in Python with Flask.

---

### Requirements
To start work with exporter, you should install following Linux distro packages:
- gcc
- python3-devel
- uwsgi
- uwsgi-plugin-python3

And then install Python PyPi dependencies from file:
```
pip3 install -r requirements.txt
```

### Usage
If dependencies installation was successful, yiu can try to run exporter with the folllowing command:
```
uwsgi --http-socket 0.0.0.0:8000 --plugin python3 --wsgi-file tenzir_exporter.py --callable app --stats 0.0.0.0:9191
```
