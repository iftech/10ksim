from typing import Iterator

from src.analysis.metrics.config import MetricToScrape


def _pods(namespace: str, stateful_set: str) -> str:
    """Selector for one role's pods, so each metric file holds a single node type."""
    return f"namespace='{namespace}', pod=~'{stateful_set}-[0-9]+'"


def container_recv_bytes(namespace: str, stateful_set: str) -> MetricToScrape:
    return MetricToScrape(
        name="container_recv_bytes",
        query=f"rate(container_network_receive_bytes_total{{{_pods(namespace, stateful_set)}}}[$__rate_interval])",
        extract_field="pod-node",
        folder_name="container-recv/",
    )


def container_sent_bytes(namespace: str, stateful_set: str) -> MetricToScrape:
    return MetricToScrape(
        name="container_sent_bytes",
        query=f"rate(container_network_transmit_bytes_total{{{_pods(namespace, stateful_set)}}}[$__rate_interval])",
        extract_field="pod-node",
        folder_name="container-sent/",
    )


def container_memory_bytes(namespace: str, stateful_set: str) -> MetricToScrape:
    # `max by (pod)` picks the pod cgroup total, which for a store node includes postgres.
    return MetricToScrape(
        name="container_memory_bytes",
        query=f"max by (pod) (container_memory_usage_bytes{{{_pods(namespace, stateful_set)}}})",
        extract_field="pod",
        folder_name="container-memory/",
    )


def container_cpu_millicores(namespace: str, stateful_set: str) -> MetricToScrape:
    # Millicores, since the plotter labels whole numbers and an edge node uses about 2.
    return MetricToScrape(
        name="container_cpu_millicores",
        query=f"1000 * max by (pod) (rate(container_cpu_usage_seconds_total{{{_pods(namespace, stateful_set)}}}[$__rate_interval]))",
        extract_field="pod",
        folder_name="container-cpu/",
    )


def archive_insert_micros(namespace: str, stateful_set: str) -> MetricToScrape:
    """Mean microseconds a store node takes to write one message to its archive."""
    selector = _pods(namespace, stateful_set)
    return MetricToScrape(
        name="archive_insert_micros",
        query=(
            f"1e6 * sum by (pod) (rate(logos_delivery_archive_insert_duration_seconds_sum{{{selector}}}[$__rate_interval]))"
            f" / sum by (pod) (rate(logos_delivery_archive_insert_duration_seconds_count{{{selector}}}[$__rate_interval]))"
        ),
        extract_field="pod",
        folder_name="archive-insert/",
    )


def role_metrics(namespace: str, stateful_set: str) -> Iterator[MetricToScrape]:
    yield container_recv_bytes(namespace, stateful_set)
    yield container_sent_bytes(namespace, stateful_set)
    yield container_memory_bytes(namespace, stateful_set)
    yield container_cpu_millicores(namespace, stateful_set)
