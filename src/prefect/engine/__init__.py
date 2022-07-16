from warnings import warn
from prefect import config
import prefect.executors
import prefect.engine.state
import prefect.engine.signals
import prefect.engine.result
from prefect.engine.flow_runner import FlowRunner
from prefect.engine.task_runner import TaskRunner
import prefect.engine.cloud


def get_default_executor_class() -> type:
    """
    Returns the `Executor` class specified in
    `prefect.config.engine.executor.default_class`. If the value is a string, it will
    attempt to load the already-imported object. Otherwise, the value is returned.

    Defaults to `LocalExecutor` if the string config value can not be loaded
    """
    config_value = config.engine.executor.default_class

    if not isinstance(config_value, str):
        return config_value
    try:
        return prefect.utilities.serialization.from_qualified_name(config_value)
    except ValueError:
        warn(
            f"Could not import {config_value}; using prefect.executors.LocalExecutor instead."
        )

        return prefect.executors.LocalExecutor


def get_default_flow_runner_class() -> type:
    """
    Returns the `FlowRunner` class specified in
    `prefect.config.engine.flow_runner.default_class` If the value is a string, it will
    attempt to load the already-imported object. Otherwise, the value is returned.

    Defaults to `FlowRunner` if the string config value can not be loaded
    """
    config_value = config.engine.flow_runner.default_class

    if not isinstance(config_value, str):
        return config_value
    try:
        return prefect.utilities.serialization.from_qualified_name(config_value)
    except ValueError:
        warn(
            f"Could not import {config_value}; using prefect.engine.flow_runner.FlowRunner instead."
        )

        return prefect.engine.flow_runner.FlowRunner


def get_default_task_runner_class() -> type:
    """
    Returns the `TaskRunner` class specified in `prefect.config.engine.task_runner.default_class` If the
    value is a string, it will attempt to load the already-imported object. Otherwise, the
    value is returned.

    Defaults to `TaskRunner` if the string config value can not be loaded
    """
    config_value = config.engine.task_runner.default_class

    if not isinstance(config_value, str):
        return config_value
    try:
        return prefect.utilities.serialization.from_qualified_name(config_value)
    except ValueError:
        warn(
            f"Could not import {config_value}; using prefect.engine.task_runner.TaskRunner instead."
        )

        return prefect.engine.task_runner.TaskRunner


__all__ = ["FlowRunner", "TaskRunner"]
