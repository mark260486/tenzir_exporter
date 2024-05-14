from prometheus_client import Info, Gauge, CollectorRegistry, push_to_gateway
from flask import Flask, request
import json
import re


class AppMetrics:
    """
    Representation of Prometheus metrics and loop to fetch and transform
    application metrics into Prometheus metrics.
    """

    def __init__(self):
        self.registry = CollectorRegistry()

        # Memory metrics
        self.tenzir_memory_total_bytes = Gauge("tenzir_memory_total_bytes", "Memory total bytes", registry = self.registry)
        self.tenzir_memory_used_bytes = Gauge("tenzir_memory_used_bytes", "Memory used bytes", registry = self.registry)
        self.tenzir_memory_free_bytes = Gauge("tenzir_memory_free_bytes", "Memory free bytes", registry = self.registry)
        # CPU metrics
        self.tenzir_loadavg_1m = Gauge("tenzir_loadavg_1m", "Load average 1m", registry = self.registry)
        self.tenzir_loadavg_5m = Gauge("tenzir_loadavg_5m", "Load average 5m", registry = self.registry)
        self.tenzir_loadavg_15m = Gauge("tenzir_loadavg_15m", "Load average 15m", registry = self.registry)
        # Process metrics
        self.tenzir_swap_space_usage = Gauge("tenzir_swap_space_usage", "Swap space usage", registry = self.registry)
        self.tenzir_open_fds = Gauge("tenzir_open_fds", "Open FDS", registry = self.registry)
        self.tenzir_current_memory_usage = Gauge("tenzir_current_memory_usage", "Current memory usage", registry = self.registry)
        self.tenzir_peak_memory_usage = Gauge("tenzir_peak_memory_usage", "Peak memory usage", registry = self.registry)
        # Disk metrics
        self.tenzir_disk_total_bytes = Gauge("tenzir_disk_total_bytes", "Disk total bytes", registry = self.registry)
        self.tenzir_disk_used_bytes = Gauge("tenzir_disk_used_bytes", "Disk used bytes", registry = self.registry)
        self.tenzir_disk_free_bytes = Gauge("tenzir_disk_free_bytes", "Disk free bytes", registry = self.registry)
        # Operator metrics
        self.tenzir_operator_run = Gauge("tenzir_operator_run",
                                        "The number of the run, starting at 1 for the first run.",
                                        ['pipeline_id'], registry = self.registry)
        self.tenzir_operator_duration = Gauge("tenzir_operator_duration",
                                                "Operator duration",
                                                ['pipeline_id'], registry = self.registry)
        self.tenzir_operator_starting_duration = Gauge("tenzir_operator_starting_duration",
                                                        "Operator starting duration",
                                                        ['pipeline_id'], registry = self.registry)
        self.tenzir_operator_processing_duration = Gauge("tenzir_operator_processing_duration",
                                                        "Operator processing duration",
                                                        ['pipeline_id'], registry = self.registry)
        self.tenzir_operator_scheduled_duration = Gauge("tenzir_operator_scheduled_duration",
                                                        "Operator scheduled duration",
                                                        ['pipeline_id'], registry = self.registry)
        self.tenzir_operator_running_duration = Gauge("tenzir_operator_running_duration",
                                                        "Operator running duration",
                                                        ['pipeline_id'], registry = self.registry)
        self.tenzir_operator_paused_duration = Gauge("tenzir_operator_paused_duration",
                                                        "Operator paused duration",
                                                        ['pipeline_id'], registry = self.registry)
        self.tenzir_operator_input_elements = Gauge("tenzir_operator_input_elements",
                                                        "Operator input elements",
                                                        ['pipeline_id', 'unit'], registry = self.registry)
        self.tenzir_operator_output_elements = Gauge("tenzir_operator_output_elements",
                                                        "Operator output elements",
                                                        ['pipeline_id', 'unit'], registry = self.registry)
        self.tenzir_operator_input_bytes = Gauge("tenzir_operator_input_bytes",
                                                        "Operator input approximate bytes",
                                                        ['pipeline_id', 'unit'], registry = self.registry)
        self.tenzir_operator_output_bytes = Gauge("tenzir_operator_output_bytes",
                                                        "Operator output approximate bytes",
                                                        ['pipeline_id', 'unit'], registry = self.registry)
        self.tenzir_operator_input_unit = Info("tenzir_operator_input_unit",
                                                        "Pipeline input unit",
                                                        ['pipeline_id'], registry = self.registry)
        self.tenzir_operator_output_unit = Info("tenzir_operator_output_unit",
                                                        "Pipeline output unit",
                                                        ['pipeline_id'], registry = self.registry)
        self.tenzir_operator_pipeline_id = Info("tenzir_operator_pipeline_id",
                                                        "Pipeline ID", registry = self.registry)


    def fetch(self):
        print(f"Request: {request}")
        try:
            self.status_data = (request.data).decode('utf-8').splitlines()
        except:
            return json.dumps({"error": 1})

        for item in self.status_data:
            data = json.loads(item)
            if not(data.get("pipeline_id") is None):
                reg = r'[a-z]'
                duration = re.sub(reg, '', data["duration"])
                starting_duration = re.sub(reg, '', data["starting_duration"])
                processing_duration = re.sub(reg, '', data["processing_duration"])
                scheduled_duration = re.sub(reg, '', data["scheduled_duration"])
                running_duration = re.sub(reg, '', data["running_duration"])
                paused_duration = re.sub(reg, '', data["paused_duration"])
                self.tenzir_operator_run.labels(data["pipeline_id"]).set(duration)
                self.tenzir_operator_duration.labels(data["pipeline_id"]).set(starting_duration)
                self.tenzir_operator_starting_duration.labels(data["pipeline_id"]).set(starting_duration)
                self.tenzir_operator_processing_duration.labels(data["pipeline_id"]).set(processing_duration)
                self.tenzir_operator_scheduled_duration.labels(data["pipeline_id"]).set(scheduled_duration)
                self.tenzir_operator_running_duration.labels(data["pipeline_id"]).set(running_duration)
                self.tenzir_operator_paused_duration.labels(data["pipeline_id"]).set(paused_duration)
                self.tenzir_operator_input_elements.labels(data["pipeline_id"], data["input"]["unit"]).set(data["input"]["elements"])
                self.tenzir_operator_output_elements.labels(data["pipeline_id"], data["output"]["unit"]).set(data["output"]["elements"])
                self.tenzir_operator_input_bytes.labels(data["pipeline_id"], data["input"]["unit"]).set(data["input"]["approx_bytes"])
                self.tenzir_operator_output_bytes.labels(data["pipeline_id"], data["output"]["unit"]).set(data["output"]["approx_bytes"])
                self.tenzir_operator_input_unit.labels(data["pipeline_id"]).info({"tenzir_operator_input_unit": data["input"]["unit"]})
                self.tenzir_operator_output_unit.labels(data["pipeline_id"]).info({"tenzir_operator_input_unit": data["output"]["unit"]})
                self.tenzir_operator_pipeline_id.info({"pipeline_id": data["pipeline_id"]})


            elif not(data.get("loadavg_1m") is None):
                self.tenzir_loadavg_1m.set(data["loadavg_1m"])
                self.tenzir_loadavg_5m.set(data["loadavg_5m"])
                self.tenzir_loadavg_15m.set(data["loadavg_15m"])

            elif not(data.get("swap_space_usage") is None):
                self.tenzir_swap_space_usage.set(data["swap_space_usage"])
                self.tenzir_open_fds.set(data["open_fds"])
                self.tenzir_current_memory_usage.set(data["current_memory_usage"])
                self.tenzir_peak_memory_usage.set(data["peak_memory_usage"])

            elif not(data.get("path") is None):
                self.tenzir_disk_total_bytes.set(data["total_bytes"])
                self.tenzir_disk_used_bytes.set(data["used_bytes"])
                self.tenzir_disk_free_bytes.set(data["free_bytes"])

            else:
                self.tenzir_memory_total_bytes.set(data["total_bytes"])
                self.tenzir_memory_used_bytes.set(data["used_bytes"])
                self.tenzir_memory_free_bytes.set(data["free_bytes"])

            push_to_gateway('{{ inventory_hostname }}:9091', job = 'tenzir', registry = self.registry)
        return json.dumps({"error": 0})


app_metrics = AppMetrics()

app = Flask(__name__)
app.debug = False
app.add_url_rule("/", methods = ["POST"], view_func = app_metrics.fetch)
