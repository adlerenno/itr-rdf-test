import datetime
import shutil
import subprocess
import os
import time
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from subprocess import Popen
import logging

import util
from dataset import Dataset
from global_params import ram_limit_g
from util import bash


@dataclass
class DatabaseVersion:
    timestamp: datetime
    dataset: Dataset

    @staticmethod
    def for_dataset(dataset: Dataset):
        return DatabaseVersion(datetime.now(), dataset)


class Triplestore:
    def __init__(self, name, base_dir: Path) -> None:
        self.name: str = name
        self.sparql_endpoint: str = None
        self.installation_dir: Path = base_dir.joinpath(f"triplestores/{self.name}")
        self.database_dir: Path = base_dir.joinpath(f"databases/{self.name}")
        self.database_dir.mkdir(parents=True, exist_ok=True)

        self.logs_dir: Path = base_dir.joinpath("logs")

    def _load_impl(self, dataset: Dataset) -> tuple[DatabaseVersion, int]:
        raise NotImplemented()

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        raise NotImplemented()

    def load(self, dataset: Dataset) -> DatabaseVersion:
        import time
        elapsed = time.perf_counter_ns()
        db_version, mem_usage = self._load_impl(dataset)
        elapsed = time.perf_counter_ns() - elapsed

        try:
            # write elapsed time and memory footprint to file
            import json
            (self.database_logs_dir(db_version)
            .joinpath("loading_stats.json")
            .write_text(json.dumps({
                "ns": elapsed,
                "bytes": bash(f"du -bs '{self.dataset_db_dir(dataset).absolute()}'").split("\t")[0],
                "rss": mem_usage
            })))
        finally:
            return db_version

    def stop(self, handle: Popen[bytes]):
        handle.terminate()  # TODO: SIGINT maybe, because of tentris?
        for i in range(30):  # wait up to 30 seconds
            time.sleep(1)
            if handle.poll() is not None:
                break
        handle.kill()

    def database_logs_dir(self, db_version: DatabaseVersion) -> Path:
        return self.logs_dir.joinpath(
            f"{db_version.dataset.name}/{self.name}/{db_version.timestamp.isoformat()}")
    
    def download(self) -> None:
        raise NotImplemented()
    
    def is_installed(self) -> bool:
        return self.installation_dir.exists()

    def delete_database(self, dataset: Dataset) -> None:
        if self.dataset_db_dir(dataset).exists():
            shutil.rmtree(self.dataset_db_dir(dataset))

    def dataset_db_dir(self, dataset: Dataset) -> Path:
        return self.database_dir.joinpath(dataset.name)
    
    def is_database_loaded(self, dataset: Dataset) -> bool:
        return self.dataset_db_dir(dataset).exists()
    


class Tentris(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__("tentris", *args, **kwargs)
        self.sparql_endpoint: str = "http://localhost:9080/sparql"

    def download(self) -> None:
        # download tentris
        util.download_and_extract("https://github.com/dice-group/tentris/releases/download/v1.4.0/tentris.zip", self.installation_dir, compression_algorithm=util.CompressionAlgorithm.ZIP)
        self.installation_dir.joinpath("tentris_loader").chmod(0o755)
        self.installation_dir.joinpath("tentris_server").chmod(0o755)
        assert self.is_installed()

    def _load_impl(self, dataset: Dataset) -> tuple[DatabaseVersion, int]:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)  # intentionally throw if exists

        db_version = DatabaseVersion.for_dataset(dataset)
        log_dir = self.database_logs_dir(db_version)

        proc = subprocess.Popen([f"{self.installation_dir.absolute()}/tentris_loader",
                        "--file", f"{dataset.dataset_path}",
                        "--storage", db_dir,
                        "--logfiledir",
                        f"{log_dir.absolute()}",
                        "--loglevel", "trace"])
        mem = util.monitor_memory_usage(proc)
        return db_version, mem

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        """
        Start the server in the background and return the handle
        :param db_version:  The database version to start
        :return:          The handle to the process
        """
        return subprocess.Popen([f"{self.installation_dir.absolute()}/tentris_server",
                                 "-j", f"{1}",
                                 "--storage", self.dataset_db_dir(db_version.dataset),
                                 "--logfiledir",
                                 f"{self.database_logs_dir(db_version)}/",
                                 "--loglevel", "info"])

class Oxigraph(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__("oxigraph", *args, **kwargs)
        self.executable_path: Path = self.installation_dir.joinpath("oxigraph_server_v0.3.22_x86_64_linux_gnu")
        self.sparql_endpoint: str = "http://localhost:7878/"
        self.update_endpoint: str = "http://localhost:7878/update"

    def download(self) -> None:
        util.download_file("https://github.com/oxigraph/oxigraph/releases/download/v0.3.22/oxigraph_server_v0.3.22_x86_64_linux_gnu", self.executable_path)
        self.executable_path.chmod(0o755)
        assert self.is_installed()

    def _load_impl(self, dataset: Dataset) -> tuple[DatabaseVersion, int]:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)  # intentionally throw if exists

        db_version = DatabaseVersion.for_dataset(dataset)
        log_dir = self.database_logs_dir(db_version)
        log_dir.mkdir(parents=True, exist_ok=True)

        with open(log_dir.joinpath("loading.log"), "w") as f:
            proc = subprocess.Popen([f"{self.executable_path}",
                            "load",
                            "--file", f"{dataset.dataset_path}",
                            "--location", db_dir,
                            "--lenient"], stdout=f, stderr=subprocess.STDOUT)
            mem = util.monitor_memory_usage(proc)
        return db_version, mem

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        return subprocess.Popen([f"{self.executable_path}",
                                 "serve",
                                 "--location", str(self.dataset_db_dir(db_version.dataset))])  # TODO: log


class Fuseki(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__("fuseki", *args, **kwargs)
        self.version = "5.3.0"
        self.jena_dir = self.installation_dir.joinpath(f"apache-jena-{self.version}")
        self.fuseki_dir = self.installation_dir.joinpath(f"apache-jena-fuseki-{self.version}")
        self.sparql_endpoint = "http://localhost:3030/ds/sparql"
        self.update_endpoint = "http://localhost:3030/ds/update"

    def download(self) -> None:
        util.download_file(url=f"https://dlcdn.apache.org/jena/binaries/apache-jena-fuseki-{self.version}.tar.gz",
                        checksum=0x650d4576927cb4e330c372c46f76dde3fbd683b59cc692bac501b545097402c58c2254b25502e4225ba6f7d4c79ee12c5e43b78124115dbbe6371c487aa8604f,
                        checksum_type="sha512",
                        dest=self.installation_dir.joinpath(f"apache-jena-fuseki-{self.version}.tar.gz"))
        bash(f"tar -xf {self.installation_dir.joinpath(f"apache-jena-fuseki-{self.version}.tar.gz")} -C {self.installation_dir}")
        self.installation_dir.joinpath(f"apache-jena-fuseki-{self.version}.tar.gz").unlink(missing_ok=True)
        util.download_file(url=f"https://dlcdn.apache.org/jena/binaries/apache-jena-{self.version}.tar.gz",
                        checksum=0x9a66044573ca269c0a9a1191fe7a8e1925f2d77e2e0bdadddd68616a1bafed1d9819c0b2988dd03614ec778a3882ccb091d825976fc0837406916f9f001b701c,
                        checksum_type="sha512",
                        dest=self.installation_dir.joinpath(f"apache-jena-{self.version}.tar.gz"))
        bash(f"tar -xf {self.installation_dir}/apache-jena-{self.version}.tar.gz -C {self.installation_dir}")
        self.installation_dir.joinpath(f"apache-jena-{self.version}.tar.gz").unlink(missing_ok=True)


    def _load_impl(self, dataset: Dataset) -> DatabaseVersion:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)  # intentionally throw if exists
        # for fuseki the database path must not exist
        db_dir.rmdir()

        db_version = DatabaseVersion.for_dataset(dataset)
        log_dir = self.database_logs_dir(db_version)
        log_dir.mkdir(parents=True, exist_ok=True)

        env_opts = os.environ.copy()
        env_opts['JAVA_OPTS'] = f'-Xms1g -Xmx{ram_limit_g}g'

        with open(log_dir.joinpath("loading.log"), "w") as f:
            r = subprocess.Popen([f"{self.jena_dir}/bin/tdb2.tdbloader",
                                "--loc", f"{db_dir}",
                                f"{dataset.dataset_path}", ],
                               stdout=f, stderr=subprocess.STDOUT,
                               env=env_opts)
            mem = util.monitor_memory_usage(r)
            assert r.returncode == 0

        return db_version, mem

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        env_opts = os.environ.copy()
        env_opts['JAVA_OPTS'] = f'-Xms1g -Xmx{ram_limit_g}g'

        return subprocess.Popen(["java", "-jar", "fuseki-server.jar",
                                 f"--loc={self.dataset_db_dir(db_version.dataset).absolute()}",
                                 "--update",
                                 "/ds"],
                                cwd=self.fuseki_dir,
                                env=env_opts)  # TODO: log


class Virtuoso(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__("virtuoso", *args, **kwargs)
        self.sparql_endpoint = "http://localhost:8890/sparql"
        self.update_endpoint = "http://localhost:8890/sparql"

    def download(self) -> None:
        bash(
            f"curl -L https://github.com/openlink/virtuoso-opensource/releases/download/v7.2.12/virtuoso-opensource.x86_64-generic_glibc25-linux-gnu.tar.gz"
            f" | tar -xz -C {self.installation_dir.parent}")
        self.installation_dir.parent.joinpath("virtuoso-opensource").rename(self.installation_dir.parent.joinpath("virtuoso"))
        assert self.is_installed()

    def _load_impl(self, dataset: Dataset) -> DatabaseVersion:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)
        db_version = DatabaseVersion.for_dataset(dataset)
        log_dir = self.database_logs_dir(db_version)
        log_dir.mkdir(parents=True, exist_ok=True)
        db_dir.joinpath("database").mkdir(parents=True, exist_ok=True)

        config_path = dataset.path.joinpath("virtuoso.ini")
        from string import Template
        template_path = self.installation_dir.parent.parent.parent.joinpath("virtuoso_template.ini")
        config_template = Template(template_path.read_text("utf-8"))
        substitutions = {
            "installation_dir": str(self.installation_dir.absolute()),
            "database_dir": str(db_dir.absolute()),
            "benchmarks_dir": str(dataset.path.absolute()),
            "thread_count": os.cpu_count(),
            "max_dirty_buffers": ram_limit_g * 62500,
            "number_of_buffers": ram_limit_g * 85000,
            "serve_log": str(log_dir.joinpath("serve.log")),  # should be fine
        }
        config_path.write_text(config_template.substitute(substitutions), "utf-8")

        p = subprocess.Popen(
            [f"{self.installation_dir.joinpath('bin').joinpath('virtuoso-t')}", "-c", f"{config_path}", "-w",
             "+foreground"])
        util.wait_until_available(self.sparql_endpoint)

        command = \
            f"""ld_dir ('{dataset.path.absolute()}', '*.nt', 'http://example.com');
rdf_loader_run();
GRANT SPARQL_UPDATE TO "SPARQL";
DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);
INSERT INTO DB.DBA.SYS_SPARQL_HOST (SH_HOST, SH_GRAPH_URI) VALUES ('localhost:8890', 'http://example.com');
checkpoint;
shutdown;"""

        p2 = subprocess.Popen([f"{self.installation_dir.joinpath('bin').joinpath('isql')}"], text=True, stdin=subprocess.PIPE)
        p2.communicate(input=command)
        mem = util.monitor_memory_usage(p)
        p2.wait()

        #wait = p.wait(20 * 60)  # max 20 min
        #if wait != 0:
        #    p.kill()
        #    raise RuntimeError("Virtuoso checkpoint and shutdown failed")

        return db_version, mem

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        config_path = db_version.dataset.path.joinpath("virtuoso.ini")
        assert config_path.is_file()
        return subprocess.Popen([f"{self.installation_dir.joinpath('bin').joinpath('virtuoso-t')}",
                                 "-c", f"{config_path}",
                                 "-f",
                                 "+foreground"])


class ITR(Triplestore):
    def __init__(self, *args, **kwargs):
        super().__init__("itr", *args, **kwargs)
        self.sparql_endpoint: str = "http://localhost:8080/"

    def download(self) -> None:
        # download ITR
        bash(f"""
        cd {self.installation_dir}
        git clone https://github.com/adlerenno/IncidenceTypeRePair.git
        cd IncidenceTypeRePair
        mkdir -p build
        cd build
        cmake -DCMAKE_BUILD_TYPE=Release -DOPTIMIZE_FOR_NATIVE=on -DWITH_RRR=on -DCLI=on -DWEB_SERVICE=on ..
        make
        """)
        assert self.is_installed()

    def _load_impl(self, dataset: Dataset) -> tuple[DatabaseVersion, int]:
        db_dir = self.dataset_db_dir(dataset)
        db_dir.mkdir(parents=True, exist_ok=False)  # intentionally throw if exists

        db_version = DatabaseVersion.for_dataset(dataset)

        proc = subprocess.Popen([f"{self.installation_dir.absolute()}/cgraph-cli",
                                 "--max-rank", "128",
                                 "--factor", "64",
                                 "--sampling", "0",
                                 "--rrr"
                                 f"{dataset.dataset_path}", db_dir,
                                 ])
        mem = util.monitor_memory_usage(proc)
        return db_version, mem

    def start(self, db_version: DatabaseVersion) -> Popen[bytes]:
        """
        Start the server in the background and return the handle
        :param db_version:  The database version to start
        :return:          The handle to the process
        """
        return subprocess.Popen([f"{self.installation_dir.absolute()}/cgraph-cli",
                                 self.dataset_db_dir(db_version.dataset),
                                 "--port", "8080"])
