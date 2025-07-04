# Reviewed July 04, 2025

from prometheus_client import Info, Gauge, CollectorRegistry, push_to_gateway
from flask import Flask, request
import json
import re
import sys
from loguru import logger

# Logger initialization
logger.remove()
logger.add(
    sys.stdout, level="DEBUG", format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}"
)


class AppMetrics:
    """
    Representation of Prometheus metrics and loop to fetch and transform
    application metrics into Prometheus metrics.
    """

    def __init__(self):
        self.registry = CollectorRegistry()
        # Docs: https://docs.tenzir.com/reference/operators/metrics

        # Memory metrics
        # https://docs.tenzir.com/reference/operators/metrics/#tenzirmetricsmemory
        # Contains a measurement of the available memory on the host.
        self.tenzir_memory_total_bytes = Gauge(
            "tenzir_memory_total_bytes", "Memory total bytes", registry=self.registry
        )
        self.tenzir_memory_used_bytes = Gauge(
            "tenzir_memory_used_bytes", "Memory used bytes", registry=self.registry
        )
        self.tenzir_memory_free_bytes = Gauge(
            "tenzir_memory_free_bytes", "Memory free bytes", registry=self.registry
        )

        # CPU metrics
        # https://docs.tenzir.com/reference/operators/metrics/#tenzirmetricscpu
        # Contains a measurement of CPU utilization.
        self.tenzir_loadavg_1m = Gauge(
            "tenzir_loadavg_1m", "Load average 1m", registry=self.registry
        )
        self.tenzir_loadavg_5m = Gauge(
            "tenzir_loadavg_5m", "Load average 5m", registry=self.registry
        )
        self.tenzir_loadavg_15m = Gauge(
            "tenzir_loadavg_15m", "Load average 15m", registry=self.registry
        )

        # Process metrics
        # https://docs.tenzir.com/reference/operators/metrics/#tenzirmetricsprocess
        # Contains a measurement of the amount of memory used by the tenzir-node process.
        self.tenzir_swap_space_usage = Gauge(
            "tenzir_swap_space_usage", "Swap space usage", registry=self.registry
        )
        self.tenzir_open_fds = Gauge(
            "tenzir_open_fds", "Open FDS", registry=self.registry
        )
        self.tenzir_current_memory_usage = Gauge(
            "tenzir_current_memory_usage",
            "Current memory usage",
            registry=self.registry,
        )
        self.tenzir_peak_memory_usage = Gauge(
            "tenzir_peak_memory_usage", "Peak memory usage", registry=self.registry
        )

        # Disk metrics
        # https://docs.tenzir.com/reference/operators/metrics/#tenzirmetricsdisk
        # Contains a measurement of disk space usage.
        self.tenzir_disk_total_bytes = Gauge(
            "tenzir_disk_total_bytes",
            "Disk total bytes",
            ["path"],
            registry=self.registry,
        )
        self.tenzir_disk_used_bytes = Gauge(
            "tenzir_disk_used_bytes",
            "Disk used bytes",
            ["path"],
            registry=self.registry,
        )
        self.tenzir_disk_free_bytes = Gauge(
            "tenzir_disk_free_bytes",
            "Disk free bytes",
            ["path"],
            registry=self.registry,
        )

        # Ingest metrics
        # https://docs.tenzir.com/reference/operators/metrics/#tenzirmetricsingest
        # Contains a measurement of all data ingested into the database.
        self.tenzir_ingest_schema = Info(
            "tenzir_ingest_schema", "Ingest scheme name", registry=self.registry
        )
        self.tenzir_ingest_schema_id = Info(
            "tenzir_ingest_schema_id", "Ingest scheme ID", registry=self.registry
        )
        self.tenzir_ingest_events = Gauge(
            "tenzir_ingest_events", "Ingested events", registry=self.registry
        )

        # Operator metrics
        # https://docs.tenzir.com/reference/operators/metrics/#tenzirmetricsoperator
        # Contains input and output measurements over some amount of time for a single operator instantiation.
        self.tenzir_operator_run = Gauge(
            "tenzir_operator_run",
            "The number of the run, starting at 1 for the first run.",
            ["pipeline_id"],
            registry=self.registry,
        )
        self.tenzir_operator_duration = Gauge(
            "tenzir_operator_duration",
            "Operator duration",
            ["pipeline_id"],
            registry=self.registry,
        )
        self.tenzir_operator_starting_duration = Gauge(
            "tenzir_operator_starting_duration",
            "Operator starting duration",
            ["pipeline_id"],
            registry=self.registry,
        )
        self.tenzir_operator_processing_duration = Gauge(
            "tenzir_operator_processing_duration",
            "Operator processing duration",
            ["pipeline_id"],
            registry=self.registry,
        )
        self.tenzir_operator_scheduled_duration = Gauge(
            "tenzir_operator_scheduled_duration",
            "Operator scheduled duration",
            ["pipeline_id"],
            registry=self.registry,
        )
        self.tenzir_operator_running_duration = Gauge(
            "tenzir_operator_running_duration",
            "Operator running duration",
            ["pipeline_id"],
            registry=self.registry,
        )
        self.tenzir_operator_paused_duration = Gauge(
            "tenzir_operator_paused_duration",
            "Operator paused duration",
            ["pipeline_id"],
            registry=self.registry,
        )
        self.tenzir_operator_input_elements = Gauge(
            "tenzir_operator_input_elements",
            "Operator input elements",
            ["pipeline_id", "unit"],
            registry=self.registry,
        )
        self.tenzir_operator_output_elements = Gauge(
            "tenzir_operator_output_elements",
            "Operator output elements",
            ["pipeline_id", "unit"],
            registry=self.registry,
        )
        self.tenzir_operator_input_bytes = Gauge(
            "tenzir_operator_input_bytes",
            "Operator input approximate bytes",
            ["pipeline_id", "unit"],
            registry=self.registry,
        )
        self.tenzir_operator_output_bytes = Gauge(
            "tenzir_operator_output_bytes",
            "Operator output approximate bytes",
            ["pipeline_id", "unit"],
            registry=self.registry,
        )
        self.tenzir_operator_input_unit = Info(
            "tenzir_operator_input_unit",
            "Pipeline input unit",
            ["pipeline_id"],
            registry=self.registry,
        )
        self.tenzir_operator_output_unit = Info(
            "tenzir_operator_output_unit",
            "Pipeline output unit",
            ["pipeline_id"],
            registry=self.registry,
        )
        self.tenzir_operator_pipeline_id = Info(
            "tenzir_operator_pipeline_id", "Pipeline ID", registry=self.registry
        )

        # Rebuild metrics
        # https://docs.tenzir.com/reference/operators/metrics/#tenzirmetricsrebuild
        # Contains a measurement of the partition rebuild process.
        self.tenzir_rebuild_partitions = Gauge(
            "tenzir_rebuild_partitions",
            "The number of partitions currently being rebuilt.",
            registry=self.registry,
        )
        self.tenzir_rebuild_queued_partitions = Gauge(
            "tenzir_rebuild_queued_partitions",
            "The number of partitions currently queued for rebuilding.",
            registry=self.registry,
        )

    def fetch(self):
        logger.debug(f"# Request: {request}")
        try:
            self.status_data = (request.data).decode("utf-8").splitlines()
        except:
            logger.error("# Cannot complete fetch() request")
            return json.dumps({"error": 1})

        logger.debug(f"# Data: {self.status_data}")
        parsed_data = json.loads(
            "[" + "".join(self.status_data).replace("}{", "},{") + "]"
        )
        logger.debug(f"# Parsed data: {parsed_data}")
        for item in parsed_data:
            logger.debug(f"# Item in data: {item}")

            # Rebuild metrics
            if "queued_partitions" in item.keys():
                self.tenzir_rebuild_partitions.set(item["partitions"])
                self.tenzir_rebuild_queued_partitions.set(item["queued_partitions"])

            # CPU metrics
            if "loadavg_1m" in item.keys():
                self.tenzir_loadavg_1m.set(item["loadavg_1m"])
                self.tenzir_loadavg_5m.set(item["loadavg_5m"])
                self.tenzir_loadavg_15m.set(item["loadavg_15m"])

            # Memory metrics
            if "total_bytes" in item.keys() and "path" not in item.keys():
                self.tenzir_memory_total_bytes.set(item["total_bytes"])
                self.tenzir_memory_used_bytes.set(item["used_bytes"])
                self.tenzir_memory_free_bytes.set(item["free_bytes"])

            # Process metrics
            if "swap_space_usage" in item.keys():
                self.tenzir_swap_space_usage.set(item["swap_space_usage"])
                self.tenzir_open_fds.set(item["open_fds"])
                self.tenzir_current_memory_usage.set(item["current_memory_usage"])
                self.tenzir_peak_memory_usage.set(item["peak_memory_usage"])

            # Disk metrics
            if "path" in item.keys():
                self.tenzir_disk_total_bytes.set(item["total_bytes"])
                self.tenzir_disk_used_bytes.set(item["used_bytes"])
                self.tenzir_disk_free_bytes.set(item["free_bytes"])

            # Ingest metrics
            if "schema_id" in item.keys() and "run" not in item.keys():
                self.tenzir_ingest_schema.set(item["schema"])
                self.tenzir_ingest_schema_id.set(item["schema_id"])
                self.tenzir_ingest_events.set(item["events"])

            # Operator metrics
            if "pipeline_id" in item.keys() and "transformation" in item.keys():
                reg = r"[a-z]"
                duration = re.sub(reg, "", item["duration"])
                starting_duration = re.sub(reg, "", item["starting_duration"])
                processing_duration = re.sub(reg, "", item["processing_duration"])
                scheduled_duration = re.sub(reg, "", item["scheduled_duration"])
                running_duration = re.sub(reg, "", item["running_duration"])
                paused_duration = re.sub(reg, "", item["paused_duration"])
                self.tenzir_operator_run.labels(item["pipeline_id"]).set(duration)
                self.tenzir_operator_duration.labels(item["pipeline_id"]).set(
                    starting_duration
                )
                self.tenzir_operator_starting_duration.labels(item["pipeline_id"]).set(
                    starting_duration
                )
                self.tenzir_operator_processing_duration.labels(
                    item["pipeline_id"]
                ).set(processing_duration)
                self.tenzir_operator_scheduled_duration.labels(item["pipeline_id"]).set(
                    scheduled_duration
                )
                self.tenzir_operator_running_duration.labels(item["pipeline_id"]).set(
                    running_duration
                )
                self.tenzir_operator_paused_duration.labels(item["pipeline_id"]).set(
                    paused_duration
                )
                self.tenzir_operator_input_elements.labels(
                    item["pipeline_id"], item["input"]["unit"]
                ).set(item["input"]["elements"])
                self.tenzir_operator_output_elements.labels(
                    item["pipeline_id"], item["output"]["unit"]
                ).set(item["output"]["elements"])
                self.tenzir_operator_input_bytes.labels(
                    item["pipeline_id"], item["input"]["unit"]
                ).set(item["input"]["approx_bytes"])
                self.tenzir_operator_output_bytes.labels(
                    item["pipeline_id"], item["output"]["unit"]
                ).set(item["output"]["approx_bytes"])
                self.tenzir_operator_input_unit.labels(item["pipeline_id"]).info(
                    {"tenzir_operator_input_unit": item["input"]["unit"]}
                )
                self.tenzir_operator_output_unit.labels(item["pipeline_id"]).info(
                    {"tenzir_operator_input_unit": item["output"]["unit"]}
                )
                self.tenzir_operator_pipeline_id.info(
                    {"pipeline_id": item["pipeline_id"]}
                )

            push_to_gateway(
                "{{ inventory_hostname }}:9091", job="tenzir", registry=self.registry
            )
        return {}


logger.debug("# Starting...")

app_metrics = AppMetrics()

app = Flask(__name__)
app.debug = True
app.add_url_rule("/", methods=["POST"], view_func=app_metrics.fetch)
