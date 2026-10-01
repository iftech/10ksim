# Python Imports
import asyncio
import logging
import re
from typing import List, Optional, Tuple

from kubernetes import client
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from kubernetes.stream.ws_client import WSClient
from pydantic import BaseModel, ConfigDict, Field

# Project Imports


logger = logging.getLogger(__name__)

EXIT_CODE_MARKER = "__EXIT_CODE:{}__"
EXIT_CODE_REGEX = re.compile(r"__EXIT_CODE:(\d+)__\s*$")


def wrap_command_with_exit_code(command: List[str]) -> List[str]:
    index = 0
    try:
        if command[0] == "/bin/sh" and command[1] == "-c":
            index = 2
    except IndexError:
        pass
    shell_cmd = " ".join(command[index:])
    wrapped = ["/bin/sh", "-c", f"{shell_cmd}; echo {EXIT_CODE_MARKER.format('$?')}"]
    return wrapped


def unwrap_exit_code(output: str) -> Tuple[int, str]:
    """
    Extract and remove the exit code marker from the output.

    :param output: Command output potentially containing the exit code marker.
    :return: Tuple of (exit_code, cleaned_output).
    :raises ValueError: if exit code marker not found in output.
    """
    match = EXIT_CODE_REGEX.search(output)
    if not match:
        raise ValueError("Exit code marker not found in output")
    code = int(match.group(1))
    cleaned_output = output[: match.start()]
    return code, cleaned_output


def _exec_ws_client(
    namespace: str,
    pod_name: str,
    command: List[str],
    *,
    stdin: bool = False,
    tty: bool = False,
    timeout: Optional[int] = None,
    container: Optional[str] = None,
) -> WSClient:
    api = client.CoreV1Api()
    container_kwargs = {"container": container} if container else {}
    try:
        ws_client = stream(
            api.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            command=command,
            stderr=True,
            stdin=stdin,
            stdout=True,
            tty=tty,
            _preload_content=False,
            **container_kwargs,
        )
        if timeout:
            ws_client.update(timeout=timeout)
        return ws_client
    except ApiException as e:
        raise ApiException(f"Failed to execute command in pod {pod_name}: {e}") from e


class PodCommand(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ws_client: WSClient

    output: str = Field(default_factory=str)
    """Standard out"""

    std_error: str = Field(default_factory=str)
    wrapped_for_exit_code: bool = Field(default=False)

    return_code: Optional[int] = None
    _completed: bool = False  # TODO: use None return code to indicate this(?)

    class Incomplete(ValueError):
        pass

    class ParseError(ValueError):
        pass

    def _poll(self, *, timeout: Optional[int] = 0) -> bool:
        """

        :rvalue: True if the command is still running.
        False if command finished."""
        if self.ws_client.is_open():
            return False
        self._update_output()
        self._extract_finished_output()
        return True

    def _update_output(self, *, timeout: int = 0) -> bool:
        """
        Add data for stdout and stderr.

        Note: only call if you've checked that self.ws_client.is_open() == True

        :param timeout: If `timeout == 0`, then polls for new updates and returns.
        If `timeout > 0`, waits for output for `timeout` and raises an error if no output exists.

        :raises WebSocketTimeoutException: On timeout if `timeout > 0`.
        """
        self.ws_client.update(timeout=timeout)
        while self.ws_client.peek_stdout():
            self.output += self.ws_client.read_stdout()
        while self.ws_client.peek_stderr():
            self.std_error += self.ws_client.read_stderr()

    def _read_all_output(self, timeout: int):
        """Update and read stdout/stderr until ws closes."""
        while self.ws_client.is_open():
            self._update_output(timeout=timeout)

    async def _read_all_output_async(self, *, polling_interval: float = 0.1, timeout: int = 0):
        """Update and read stdout/stderr until ws closes."""
        while self.ws_client.is_open():
            self._update_output(timeout=timeout)
            await asyncio.sleep(polling_interval)

    def _extract_finished_output(self) -> None:
        """
        Parse and clean the output after the command finishes.

        Sets self._returncode based on presence of exit code marker if wrapped,
        and sets self._completed to True.

        :raises ValueError: if expected exit code marker is missing when wrapping is enabled.
        """
        self._completed = True
        if not self.wrapped_for_exit_code:
            self.return_code = 0
        else:
            try:
                code, cleaned_output = unwrap_exit_code(self.output)
                self.return_code = code
                self.output = cleaned_output
            except ValueError as e:
                raise PodCommand.ParseError("Failed to parse output") from e

    def collect_output(self, timeout: Optional[int] = None) -> str:
        self._read_all_output(timeout)
        self._extract_finished_output()
        return self.output

    async def collect_output_async(self, timeout: Optional[int] = None) -> str:
        await self._read_all_output_async(timeout=timeout)
        self._extract_finished_output()
        return self.output

    @property
    def exit_code(self):
        if not self._completed:
            if self.ws_client.is_open():
                # Use collect_output() to wait for command completion and get stdout.
                raise PodCommand.Incomplete("Command has not completed.")
            self._update_output()
            self._extract_finished_output()
        return self.return_code

    @property
    def ok(self):
        return self.exit_code == 0

    def close(self):
        self.ws_client.close()


def exec_command_in_pod(
    namespace: str,
    pod_name: str,
    command: List[str],
    *,
    capture_exit_code: bool = True,
    stdin: bool = False,
    tty: bool = False,
    timeout: Optional[int] = None,
    container: Optional[str] = None,
) -> PodCommand:
    """
    Execute a command in a Kubernetes pod and return a helper for interaction and output collection.

    :param command: Example: ["/bin/sh", "-c", f"tc qdisc add dev eth0 root netem delay 50ms"]
    :param capture_exit_code: Wrap the command to append exit code to output for capturing it.
                              This is needed because Kubernetes exec API does not provide exit codes.
    :param stdin: Enable stdin stream (for interactive commands).
    :param tty: Allocate a TTY (required for interactive shells).
    :param timeout: Timeout in seconds for websocket reads and updates.
    :param container: Container to exec in. Required when the pod has more than one.
    :return: A PodCommand object wrapping the WSClient with methods to collect output and check status.
    :raises kubernetes.client.rest.ApiException: If the command failed to start in the pod.
    """
    if capture_exit_code:
        command = wrap_command_with_exit_code(command)

    ws_client = _exec_ws_client(
        namespace,
        pod_name,
        command=command,
        stdin=stdin,
        tty=tty,
        timeout=timeout,
        container=container,
    )
    return PodCommand(ws_client=ws_client, wrapped_for_exit_code=True)
