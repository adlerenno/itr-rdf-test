from pathlib import Path
import zipfile

from query_translate import process_sparql_file
from util import bash, hash_file, download_file

class Dataset:
    def __init__(self, name, datasets_dir: Path):
        self.name = name
        self.path = datasets_dir.joinpath(name)
        self.path.mkdir(parents=True, exist_ok=True)
        self.dataset_path: Path = self.path.joinpath("dataset.nt")
        self.queries_path: Path = self.path.joinpath("queries.txt")

    def download(self) -> None:
        pass

    def is_downloaded(self) -> bool:
        return self.dataset_path.exists() and self.queries_path.exists()


class SWDF(Dataset):
    def __init__(self, directory: Path):
        super().__init__("swdf", directory)

    def download(self):
        # warmup queries
        queriesurl = "https://raw.githubusercontent.com/dice-group/iswc2020_tentris/master/queries/SWDF-Queries.txt"
        bash(f"curl -L '{queriesurl}' > '{self.queries_path.absolute()}'")
        assert self.queries_path.exists()
        queriesurl_sha1 = 'e8c4d295d29f36f11b0b77a1ea83e13ff7333488'
        assert queriesurl_sha1 == hash_file(self.queries_path, "sha1")

        old = self.queries_path.rename(f'{self.queries_path.absolute()}2')
        process_sparql_file(old, self.queries_path)

        # use bash to download and decompress the dataset
        dataset_url = "https://files.dice-research.org/datasets/ISWC2020_Tentris/swdf.zip"
        from urllib.request import urlopen
        from io import BytesIO
        with urlopen(dataset_url) as zipresp:
            with zipfile.ZipFile(BytesIO(zipresp.read())) as zfile:
                zfile.extractall()
        bash(f"mv swdf.nt {self.dataset_path.absolute()}")
        assert self.dataset_path.exists()


class DBpedia2015(Dataset):
    def __init__(self, directory: Path):
        super().__init__("dbpedia", directory)

    def download(self):
        # warmup queries
        queriesurl = "https://files.dice-research.org/projects/tentris_compression/feasible-DBpedia-bgp-v2.txt"
        bash(f"curl -L '{queriesurl}' > '{self.queries_path.absolute()}'")
        assert self.queries_path.exists()
        queries_sha1 = '10c397a57f4a7d3844194c214cfb2c26ab132d01'
        assert queries_sha1 == hash_file(self.queries_path, "sha1")

        old = self.queries_path.rename(f'{self.queries_path.absolute()}2')
        process_sparql_file(old, self.queries_path)

        # use bash to download and decompress the dataset
        dataset_url = "https://files.dice-research.org/datasets/ISWC2020_Tentris/dbpedia_2015-10_en_wo-comments_c.nt.zst"
        bash(f"curl -L '{dataset_url}' | zstd -d > '{self.dataset_path.absolute()}'")
        assert self.dataset_path.exists()


class Wikidata(Dataset):
    def __init__(self, directory: Path):
        super().__init__("wikidata", directory)

    def download(self: Dataset):
        # warmup queries
        queries_url = "https://files.dice-research.org/projects/tentris_compression/feasible-exmp-wikidata500-bgp-v4.txt"
        bash(f"curl -L '{queries_url}' > '{self.queries_path.absolute()}'")
        assert self.queries_path.exists()
        queriesurl_sha1 = 'd881ea12c315669ff3ef1f8073ca553e3f9b2715'
        assert queriesurl_sha1 == hash_file(self.queries_path, "sha1")

        old = self.queries_path.rename(f'{self.queries_path.absolute()}2')
        process_sparql_file(old, self.queries_path)

        # use bash to download and decompress the dataset
        dataset_url = "https://files.dice-research.org/datasets/hypertrie_update/wikidata/wikidata-2020-11-11-truthy-BETA-without-preparation.nt.zst"
        bash(f"curl -L '{dataset_url}' | zstd -d > '{self.dataset_path.absolute()}'")
        assert self.dataset_path.exists()


class Watdiv(Dataset):
    def __init__(self, datasets_dir: Path):
        super().__init__("watdiv", datasets_dir)

    def download(self):
        import shutil
        # generated queries hasn't been uploaded yet
        shutil.copy(Path("watdiv_queries.txt"), self.queries_path)

        old = self.queries_path.rename(f'{self.queries_path.absolute()}2')
        process_sparql_file(old, self.queries_path)

        # download dataset
        dataset_url = "https://dsg.uwaterloo.ca/watdiv/watdiv.1000M.tar.bz2"
        bash(f"curl -L '{dataset_url}' | tar -xOjf - > '{self.dataset_path.absolute()}'")
        assert self.dataset_path.exists()

