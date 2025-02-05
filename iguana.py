from pathlib import Path
import subprocess
import os
import logging
from string import Template
from dataclasses import dataclass

from triplestore import Triplestore, DatabaseVersion
from dataset import Dataset
import util


@dataclass
class IguanaConfiguration:
    name: str
    path: Path
    values: dict


class Iguana:
    def __init__(self, base_dir: Path) -> None:
        self.installation_dir = base_dir.joinpath("iguana")
        self.installation_dir.mkdir(parents=True, exist_ok=True)
        self.executable_path = self.installation_dir.joinpath("iguana")
        

    def install(self, prefer_compilation: bool = True) -> bool:
        raise NotImplemented()

        if self.is_installed():
            logging.info("Iguana is already installed. To reinstall delete the installation directory.")
            return True

        if not prefer_compilation:
            return self.download_binaries()
        
        # check if maven is installed
        result = subprocess.run("mvn --version", shell=True)
        if result.returncode != 0:
            logging.info("Maven is not installed. Downloading precompiled binaries.")
            return self.download_binaries() # maven not found
    
        result = os.getenv("GRAALVM_HOME")
        if result is None:
            logging.warning("The GRAALVM_HOME environment variable hasn't been set. Install GraalVM and set the variable for compiling IGUANA. Downloading precompiled binaries.")
            return self.download_binaries() # graalvm is not installed

        result = subprocess.run("git --version", shell=True)
        if result.returncode != 0:
            return self.download_binaries() # git is not installed

        result = subprocess.run("git clone https://github.com/dice-group/IGUANA.git", shell=True, cwd=self.installation_dir)
        if result.returncode != 0:
            return self.download_binaries() # git clone failed

        result = subprocess.run("mvn -Pnative -Dagent=true package", shell=True, cwd=self.installation_dir.joinpath("IGUANA"))
        if result.returncode != 0:
            return self.download_binaries() # maven build failed
        
        self.executable_path = self.installation_dir.joinpath("IGUANA", "target", "iguana")
        return self.is_installed()


    def download_binaries(self) -> bool:
        util.download_file("https://github.com/dice-group/IGUANA/releases/latest/download/iguana", self.executable_path)
        self.executable_path.chmod(0o755)
        return self.is_installed()


    def load_template(self, template_path: Path) -> None:
        with open(template_path, "r") as f:
            self.template = Template(f.read())


    def instantiate_template(self, name: str, suites_location_dir: Path, **kwargs) -> IguanaConfiguration:
        if suites_location_dir.exists() and not suites_location_dir.is_dir():
            ex = RuntimeError("Invalid directory location for the instantiated iguana configurations. The given path is not a directory.")
            logging.exception(ex)
            raise ex
        suites_location_dir.mkdir(parents=True, exist_ok=True)

        with open(suites_location_dir.joinpath(f"{name}.yaml"), "w") as f:
            f.write(self.template.substitute(kwargs))
        return IguanaConfiguration(name, suites_location_dir.joinpath(f"{name}.yaml"), kwargs)


    def is_installed(self) -> bool:
        logging.info(f"Checking for path {self.executable_path}")
        if not self.executable_path.exists():
            return False
        else:
            return True
        
        process = subprocess.Popen([f"{self.executable_path}"], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        output, error = process.communicate()
        logging.debug(output.decode())
        exit_code = process.wait()
        if exit_code != 0:
            return False

        return True

    def run_benchmark(self, triplestore: Triplestore, benchmark: Dataset, configuration: IguanaConfiguration) -> None:
        # loading dataset into triplestore
        if not triplestore.is_database_loaded(benchmark):
            logging.info(f"The {benchmark.name} dataset hasn't been loaded into {triplestore.name} yet. Loading now.")
            db = triplestore.load(benchmark)
            logging.info(f"Loaded {benchmark.name} dataset into {triplestore.name}.")
        else:
            import datetime
            db = DatabaseVersion(datetime.datetime.now(), benchmark)

        # starting triplestore
        logging.info(f"Starting {triplestore.name}.")
        handle = triplestore.start(db)
        triplestore_running = lambda: handle.poll() is None
        assert triplestore_running()
        logging.info(f"Waiting for {triplestore.name} to initialize")
        util.wait_until_available(triplestore.sparql_endpoint, timeout=20 * 60)  # up to 20 minutes
        logging.info(f"Started {triplestore.name}.")

        # running benchmark
        logging.info(f"Running benchmark {configuration.name}.")
        subprocess.run([f"{self.executable_path}", configuration.path], check=True)
        assert triplestore_running()
        logging.info(f"Finished benchmark {configuration.name}.")

        # stopping triplestore
        logging.info(f"Stopping {triplestore.name}.")
        triplestore.stop(handle)
        assert not triplestore_running()
