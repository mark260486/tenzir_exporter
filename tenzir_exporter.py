# Reviewed October 02, 2024

from prometheus_client import Info, Gauge, CollectorRegistry, push_to_gateway
from flask import Flask, request
import json
import re
import sys
from loguru import logger

# Logger initialization
logger.remove()
logger.add(sys.stdout, level="DEBUG", format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}")


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
        # Rebuild metrics
        self.tenzir_rebuild_partitions = Gauge("tenzir_rebuild_partitions",
                                               "The number of partitions currently being rebuilt.",
                                               registry = self.registry)
        self.tenzir_rebuild_queued_partitions = Gauge("tenzir_rebuild_queued_partitions",
                                                      "The number of partitions currently queued for rebuilding.",
                                                      registry = self.registry)
        

    def fetch(self):
        logger.debug(f"# Request: {request}")
        try:
            self.status_data = (request.data).decode('utf-8').splitlines()
        except:
            logger.error(f"# Cannot complete fetch() request")
            return json.dumps({"error": 1})

        logger.debug(f"# Data: {self.status_data}")
        parsed_data = json.loads('[' + ''.join(self.status_data).replace('}{', '},{') + ']')
        logger.debug(f"# Parsed data: {parsed_data}")
        for item in parsed_data:
            logger.debug(f"# Item in data: {item}")
            if "schema" in item:
                continue
            if not(item.get("pipeline_id") is None):
                reg = r'[a-z]'
                duration = re.sub(reg, '', item["duration"])
                starting_duration = re.sub(reg, '', item["starting_duration"])
                processing_duration = re.sub(reg, '', item["processing_duration"])
                scheduled_duration = re.sub(reg, '', item["scheduled_duration"])
                running_duration = re.sub(reg, '', item["running_duration"])
                paused_duration = re.sub(reg, '', item["paused_duration"])
                self.tenzir_operator_run.labels(item["pipeline_id"]).set(duration)
                self.tenzir_operator_duration.labels(item["pipeline_id"]).set(starting_duration)
                self.tenzir_operator_starting_duration.labels(item["pipeline_id"]).set(starting_duration)
                self.tenzir_operator_processing_duration.labels(item["pipeline_id"]).set(processing_duration)
                self.tenzir_operator_scheduled_duration.labels(item["pipeline_id"]).set(scheduled_duration)
                self.tenzir_operator_running_duration.labels(item["pipeline_id"]).set(running_duration)
                self.tenzir_operator_paused_duration.labels(item["pipeline_id"]).set(paused_duration)
                self.tenzir_operator_input_elements.labels(item["pipeline_id"], item["input"]["unit"]).set(item["input"]["elements"])
                self.tenzir_operator_output_elements.labels(item["pipeline_id"], item["output"]["unit"]).set(item["output"]["elements"])
                self.tenzir_operator_input_bytes.labels(item["pipeline_id"], item["input"]["unit"]).set(item["input"]["approx_bytes"])
                self.tenzir_operator_output_bytes.labels(item["pipeline_id"], item["output"]["unit"]).set(item["output"]["approx_bytes"])
                self.tenzir_operator_input_unit.labels(item["pipeline_id"]).info({"tenzir_operator_input_unit": item["input"]["unit"]})
                self.tenzir_operator_output_unit.labels(item["pipeline_id"]).info({"tenzir_operator_input_unit": item["output"]["unit"]})
                self.tenzir_operator_pipeline_id.info({"pipeline_id": item["pipeline_id"]})


            elif not(item.get("loadavg_1m") is None):
                self.tenzir_loadavg_1m.set(item["loadavg_1m"])
                self.tenzir_loadavg_5m.set(item["loadavg_5m"])
                self.tenzir_loadavg_15m.set(item["loadavg_15m"])

            elif not(item.get("swap_space_usage") is None):
                self.tenzir_swap_space_usage.set(item["swap_space_usage"])
                self.tenzir_open_fds.set(item["open_fds"])
                self.tenzir_current_memory_usage.set(item["current_memory_usage"])
                self.tenzir_peak_memory_usage.set(item["peak_memory_usage"])

            elif not(item.get("path") is None):
                self.tenzir_disk_total_bytes.set(item["total_bytes"])
                self.tenzir_disk_used_bytes.set(item["used_bytes"])
                self.tenzir_disk_free_bytes.set(item["free_bytes"])

            elif not(item.get("partitions") is None):
                self.tenzir_rebuild_partitions.set(item["partitions"])
                self.tenzir_rebuild_queued_partitions.set(item["queued_partitions"])

            else:
                self.tenzir_memory_total_bytes.set(item["total_bytes"])
                self.tenzir_memory_used_bytes.set(item["used_bytes"])
                self.tenzir_memory_free_bytes.set(item["free_bytes"])

            push_to_gateway('s-msk-p-sem-tenzir01:9091', job = 'tenzir', registry = self.registry)
        return json.dumps({"error": 0})

logger.debug(f"# Starting...")

app_metrics = AppMetrics()

app = Flask(__name__)
app.debug = True
app.add_url_rule("/", methods = ["POST"], view_func = app_metrics.fetch)
