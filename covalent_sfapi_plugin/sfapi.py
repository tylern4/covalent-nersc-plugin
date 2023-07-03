# Copyright 2021 Agnostiq Inc.
#
# This file is part of Covalent.
#
# Licensed under the GNU Affero General Public License 3.0 (the "License").
# A copy of the License may be obtained with this software package or at
#
#      https://www.gnu.org/licenses/agpl-3.0.en.html
#
# Use of this file is prohibited except in compliance with the License. Any
# modifications or derivative works of this file must retain this copyright
# notice, and modified files must contain a notice indicating that they have
# been altered from the originals.
#
# Covalent is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the License for more details.
#
# Relief from the License may be granted by purchasing a commercial license.

"""SuperFacility API executor plugin, based on the Covalent-Slurm plugin."""

import asyncio
import io
import os
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Union

import cloudpickle as pickle
from covalent._results_manager.result import Result
from covalent._shared_files import logger
from covalent._shared_files.config import get_config
from covalent.executor.base import AsyncBaseExecutor

from sfapi_client import AsyncClient as AsyncSFapiClient
from sfapi_client import SfApiError, StatusValue
from sfapi_client.paths import AsyncRemotePath
from sfapi_client.compute import Machine, AsyncCompute

app_log = logger.app_log
log_stack_info = logger.log_stack_info

_EXECUTOR_PLUGIN_DEFAULTS = {
    "machine": "",
    "cert_file": None,
    "remote_workdir": "covalent-workdir",
    "create_unique_workdir": False,
    "conda_env": "",
    "options": {},
    "prerun_commands": None,
    "postrun_commands": None,
    "use_srun": True,
    "srun_options": {},
    "srun_append": None,
    "bashrc_path": "$HOME/.bashrc",
    "cache_dir": str(Path(get_config("dispatcher.cache_dir")).expanduser().resolve()),
    "cleanup": True,
}

executor_plugin_name = "SuperFacilityExecutor"


class SuperFacilityExecutor(AsyncBaseExecutor):
    """SuperFacility executor plugin class.

    Args:
        machine: Remote NERSC machine.
        cert_file: Certificate file used to authenticate with the SF Api.
        remote_workdir: Working directory on the remote cluster.
        create_unique_workdir: Whether to create a unique working (sub)directory for each task.
        conda_env: Name of conda environment on which to run the function. Use "base" for the base environment or "" for no conda.
        options: Dictionary of parameters used to build a Slurm submit script.
        prerun_commands: List of shell commands to run before running the pickled function.
        postrun_commands: List of shell commands to run after running the pickled function.
        use_srun: Whether or not to run the pickled Python function with srun. If your function itself makes srun or mpirun calls, set this to False.
        srun_options: Dictionary of parameters passed to srun inside submit script.
        srun_append: Command nested into srun call.
        bashrc_path: Path to the bashrc file to source before running the function.
        cache_dir: Local cache directory used by this executor for temporary files.
        cleanup: Whether to perform cleanup or not on remote machine.
    """

    def __init__(
        self,
        machine: Union[str, Machine] = None,
        cert_file: Union[str, Path] = None,
        remote_workdir: str = None,
        create_unique_workdir: bool = None,
        conda_env: str = None,
        options: Dict = None,
        prerun_commands: List[str] = None,
        postrun_commands: List[str] = None,
        use_srun: bool = None,
        srun_options: Dict = None,
        srun_append: str = None,
        bashrc_path: str = None,
        cache_dir: str = None,
        cleanup: bool = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.machine = machine or get_config("executors.sfapi.machine")

        try:
            self.cert_file = cert_file or get_config("executors.sfapi.cert_file")
        except KeyError:
            self.cert_file = None

        self.remote_workdir = remote_workdir or get_config("executors.sfapi.remote_workdir")

        self.create_unique_workdir = (
            get_config("executors.sfapi.create_unique_workdir")
            if create_unique_workdir is None
            else create_unique_workdir
        )

        try:
            self.conda_env = (
                get_config("executors.sfapi.conda_env") if conda_env is None else conda_env
            )
        except KeyError:
            self.conda_env = None

        try:
            self.bashrc_path = (
                get_config("executors.sfapi.bashrc_path") if bashrc_path is None else bashrc_path
            )
        except KeyError:
            self.bashrc_path = None

        self.cache_dir = cache_dir or get_config("executors.sfapi.cache_dir")
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

        # To allow passing empty dictionary
        if options is None:
            options = get_config("executors.sfapi.options")
        self.options = deepcopy(options)

        self.use_srun = get_config("executors.sfapi.use_srun") if use_srun is None else use_srun

        if srun_options is None:
            srun_options = get_config("executors.sfapi.srun_options")
        self.srun_options = deepcopy(srun_options)

        try:
            self.srun_append = srun_append or get_config("executors.sfapi.srun_append")
        except KeyError:
            self.srun_append = None

        self.prerun_commands = list(prerun_commands) if prerun_commands else []
        self.postrun_commands = list(postrun_commands) if postrun_commands else []

        self.cleanup = get_config("executors.sfapi.cleanup") if cleanup is None else cleanup

    async def _client_connect(self) -> AsyncCompute:
        """
        Helper function for connecting to the remote machine through sfapi_client module.

        Args:
            None

        Returns:
            The connection object
        """

        if not self.cert_file:
            raise ValueError("sfapi certificate is a required parameter in the sfapi plugin.")

        self.client = AsyncSFapiClient(key=self.cert_file)

        try:
            user = await self.client.user()
            self.username = user.name
        except SfApiError as e:
            raise RuntimeError(
                f"Could not authenticate client from keys: {self.cert_file}", e
            )

        try:
            conn = await self.client.compute(self.machine)
        except SfApiError as e:
            raise RuntimeError(
                f"Could not connect to: {self.machine}", e
            )

        if conn.status != StatusValue.active:
            raise RuntimeError(f"Cannot run on compute resource {self.machine}, {conn.status}")

        return conn

    def _format_submit_script(
        self, python_version: str, py_filename: str, current_remote_workdir: str
    ) -> str:
        """Create the SLURM that defines the job, uses srun to run the python script.

        Args:
            python_version: Python version required by the pickled function.
            py_filename: Name of the python script.
            current_remote_workdir: Current working directory on the remote machine.

        Returns:
            script: String object containing a script parsable by sbatch.
        """

        # Add chdir to current working directory
        self.options["chdir"] = current_remote_workdir

        # preamble
        slurm_preamble = "#!/bin/bash\n"
        for key, value in self.options.items():
            slurm_preamble += "#SBATCH "
            if len(key) == 1:
                slurm_preamble += f"-{key}" + (f" {value}" if value else "")
            else:
                slurm_preamble += f"--{key}" + (f"={value}" if value else "")
            slurm_preamble += "\n"
        slurm_preamble += "\n"

        conda_env_clean = "" if self.conda_env == "base" else self.conda_env

        # Source commands
        if self.bashrc_path:
            source_text = f"source {self.bashrc_path}\n"
        else:
            source_text = ""

        # sets up conda environment
        if self.conda_env:
            slurm_conda = f"""
            conda activate {conda_env_clean}
            retval=$?
            if [ $retval -ne 0 ] ; then
                >&2 echo "Conda environment {self.conda_env} is not present on the compute node. "\
                "Please create the environment and try again."
                exit 99
            fi
            """
        else:
            slurm_conda = ""

        # checks remote python version
        slurm_python_version = f"""
remote_py_version=$(python -c "print('.'.join(map(str, __import__('sys').version_info[:2])))")
if [[ "{python_version}" != $remote_py_version ]] ; then
  >&2 echo "Python version mismatch. Please install Python {python_version} in the compute environment."
  exit 199
fi
"""
        # runs pre-run commands
        if self.prerun_commands:
            slurm_prerun_commands = "\n".join([""] + self.prerun_commands + [""])
        else:
            slurm_prerun_commands = ""

        if self.use_srun:
            # uses srun to run script calling pickled function
            srun_options_str = ""
            for key, value in self.srun_options.items():
                srun_options_str += " "
                if len(key) == 1:
                    srun_options_str += f"-{key}" + (f" {value}" if value else "")
                else:
                    srun_options_str += f"--{key}" + (f"={value}" if value else "")

            slurm_srun = f"srun{srun_options_str} \\"

            if self.srun_append:
                # insert any appended commands
                slurm_srun += f"""
    {self.srun_append} \\
    """
            else:
                slurm_srun += """
    """

        else:
            slurm_srun = ""

        remote_py_filename = os.path.join(self.remote_workdir, py_filename)
        python_cmd = slurm_srun + f"python {remote_py_filename}"

        # runs post-run commands
        if self.postrun_commands:
            slurm_postrun_commands = "\n".join([""] + self.postrun_commands + [""])
        else:
            slurm_postrun_commands = ""

        # assemble commands into slurm body
        slurm_body = "\n".join([slurm_prerun_commands, python_cmd, slurm_postrun_commands, "wait"])

        # assemble script
        return "".join(
            [slurm_preamble, source_text, slurm_conda, slurm_python_version, slurm_body]
        )

    def _format_py_script(
        self,
        func_filename: str,
        result_filename: str,
    ) -> str:
        """Create the Python script that executes the pickled python function.

        Args:
            func_filename: Name of the pickled function.
            result_filename: Name of the pickled result.

        Returns:
            script: String object containing a script parsable by sbatch.
        """
        func_filename = os.path.join(self.remote_workdir, func_filename)
        result_filename = os.path.join(self.remote_workdir, result_filename)
        return f"""
import cloudpickle as pickle

with open("{func_filename}", "rb") as f:
    function, args, kwargs = pickle.load(f)

result = None
exception = None

try:
    result = function(*args, **kwargs)
except Exception as e:
    exception = e

with open("{result_filename}", "wb") as f:
    pickle.dump((result, exception), f)
"""

    async def _poll_slurm(self, job_id: int, conn: AsyncCompute) -> None:
        """Poll a Slurm job until completion.

        Args:
            job_id: Slurm job ID.
            conn: SSH connection object.

        Returns:
            None
        """

        job = await conn.job(jobid=job_id, command="sacct")
        try:
            await job.complete()  # timeout=...
        except TimeoutError:
            raise RuntimeError("Job failed with status:\n", await job.status)

    async def _query_result(
        self, result_filename: str, task_results_dir: str, conn: AsyncCompute
    ) -> Any:
        """Query and retrieve the task result including stdout and stderr logs.

        Args:
            result_filename: Name of the pickled result file.
            task_results_dir: Directory on the Covalent server where the result will be copied.
            conn: SSH connection object.
        Returns:
            result: Task result.
        """

        # Stream files from remote machine to Covalent server
        remote_result_filename = os.path.join(self.remote_workdir, result_filename)
        try:
            [remote_result] = await conn.ls(remote_result_filename)
            result_stream = await remote_result.download(binary=True)
        except SfApiError:
            raise FileNotFoundError(remote_result_filename)

        remote_stdout_filename = os.path.join(self.remote_workdir, os.path.basename(self.options["output"]))
        try:
            [remote_stdout] = await conn.ls(remote_stdout_filename)
            stdout_stream = await remote_stdout.download(binary=True)
        except SfApiError:
            raise FileNotFoundError(remote_stdout_filename)

        remote_stderr_filename = os.path.join(self.remote_workdir, os.path.basename(self.options["error"]))
        try:
            [remote_stderr] = await conn.ls(remote_stderr_filename)
            stderr_stream = await remote_stderr.download(binary=True)
        except SfApiError:
            raise FileNotFoundError(remote_stderr_filename)

        result, exception = pickle.loads(result_stream.getbuffer())
        stdout = stdout_stream.getbuffer()
        stderr = stderr_stream.getbuffer()

        return result, stdout, stderr, exception

    async def run(self, function: Callable, args: List, kwargs: Dict, task_metadata: Dict):
        """Run a function on a remote machine using Slurm.

        Args:
            function: Function to be executed.
            args: List of positional arguments to be passed to the function.
            kwargs: Dictionary of keyword arguments to be passed to the function.
            task_metadata: Dictionary of metadata associated with the task.

        Returns:
            result: Result object containing the result of the function execution.
        """
        dispatch_id = task_metadata["dispatch_id"]
        node_id = task_metadata["node_id"]
        results_dir = task_metadata["results_dir"]
        task_results_dir = os.path.join(results_dir, dispatch_id)

        if self.create_unique_workdir:
            current_remote_workdir = os.path.join(
                self.remote_workdir, dispatch_id, "node_" + str(node_id)
            )
        else:
            current_remote_workdir = self.remote_workdir

        result_filename = f"result-{dispatch_id}-{node_id}.pkl"
        py_script_filename = f"script-{dispatch_id}-{node_id}.py"
        func_filename = f"func-{dispatch_id}-{node_id}.pkl"

        if "output" not in self.options:
            self.options["output"] = os.path.join(
                current_remote_workdir, f"stdout-{dispatch_id}-{node_id}.log"
            )
        if "error" not in self.options:
            self.options["error"] = os.path.join(
                current_remote_workdir, f"stderr-{dispatch_id}-{node_id}.log"
            )

        result = None

        conn = await self._client_connect()

        py_version_func = ".".join(function.args[0].python_version.split(".")[:2])
        app_log.debug(f"Python version: {py_version_func}")

        # Create the remote directory
        app_log.debug(f"Creating remote work directory {current_remote_workdir} ...")
        cmd_mkdir_remote = f"mkdir -p {current_remote_workdir}"
        await conn.run(cmd_mkdir_remote)

        try:
            [remote_workdir] = await conn.ls(path=self.remote_workdir, directory=True)
            func_bytes = io.BytesIO()
            pickle.dump((function, args, kwargs), func_bytes)
            func_bytes.filename = func_filename
            await remote_workdir.upload(func_bytes)

            python_exec_script = self._format_py_script(func_filename, result_filename)
            py_bytes = io.BytesIO(bytes(python_exec_script, "ascii"))
            py_bytes.filename = py_script_filename
            await remote_workdir.upload(py_bytes)
            
        except SfApiError as e:
            raise RuntimeError(e)

        slurm_submit_script = self._format_submit_script(
            py_version_func, py_script_filename, current_remote_workdir
        )

        try:
            job_info = await conn.submit_job(slurm_submit_script)
            slurm_job_id = job_info.jobid
        except SfApiError as e:
            raise RuntimeError(e)

        app_log.debug(f"Polling slurm with job_id: {slurm_job_id} ...")
        await self._poll_slurm(slurm_job_id, conn)

        app_log.debug(f"Querying result with job_id: {slurm_job_id} ...")
        result, stdout, stderr, exception = await self._query_result(
            result_filename, task_results_dir, conn
        )

        print(stdout)
        print(stderr, file=sys.stderr)

        if exception:
            raise RuntimeError(exception)

        app_log.debug("Preparing for teardown...")
        self._remote_func_filename = os.path.join(self.remote_workdir, func_filename)
        self._remote_py_script_filename = os.path.join(self.remote_workdir, py_script_filename)
        self._remote_result_filename = os.path.join(self.remote_workdir, result_filename)

        return result

    async def teardown(self, task_metadata: Dict):
        """Perform cleanup on remote machine.

        Args:
            task_metadata: Dictionary of metadata associated with the task.

        Returns:
            None
        """
        if self.cleanup:
            try:
                app_log.debug("Performing cleanup on remote...")
                conn = await self._client_connect()
                await conn.run(f"rm -f {self._remote_func_filename} {self._remote_py_script_filename} {self._remote_result_filename} {self.options['output']} {self.options['error']}")
            except Exception:
                app_log.warning("Slurm cleanup could not successfully complete. Nonfatal error.")
