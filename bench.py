# TODO: monitor loading

import logging
from datetime import datetime
from pathlib import Path
from rich.logging import RichHandler

import util
from iguana import Iguana
from dataset import SWDF, Wikidata, Dataset, Watdiv, DBpedia2015
from triplestore import Tentris, Fuseki, ITR, Triplestore, Oxigraph, Virtuoso

if __name__ == "__main__":
    dry_run = False
    debug_logging = False

    # setup logging
    Path("logs").mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=f'logs/benchmark_{datetime.now().isoformat().replace(":", "-")}.log', encoding='utf-8', level=(logging.DEBUG if debug_logging else logging.INFO))
    console = RichHandler(log_time_format="[%d/%m/%Y %X:%f]", omit_repeated_times=False)
    logging.getLogger('').addHandler(console) 

    # from getpass import getpass
    # pw = getpass(f"Please enter password for user '{os.getlogin()}': ")

    # variables setup
    base_dir = Path("benchmarks")
    datasets_dir = base_dir.joinpath("datasets")
    # might be best to start only with swdf first, as it is the smallest dataset
    datasets: list[Dataset] = [SWDF(datasets_dir),
                               DBpedia2015(datasets_dir),
                               Wikidata(datasets_dir),
                               Watdiv(datasets_dir)]  # select datasets here
    triplestores: list[Triplestore] = [#Tentris(base_dir),
                                       #Fuseki(base_dir),
                                       #Oxigraph(base_dir),
                                       #Virtuoso(base_dir),
                                       ITR(base_dir)]  # select triplestores here

    # install iguana
    iguana = Iguana(base_dir)
    if not iguana.is_installed():
        logging.info("Iguana is not installed. Installing it now.")
        if not dry_run: iguana.download_binaries()
    else:
        logging.info(f"Found Iguana.")
    iguana.load_template(Path("template.yml"))

    # setup triplestores
    for triplestore in triplestores:
        if not triplestore.is_installed():
            logging.info(f"Missing {triplestore.name}. Installing it now.")
            if not dry_run: triplestore.download()
        else:
            logging.info(f"Found {triplestore.name}.")


    # setup datasets
    for dataset in datasets:
        if not dataset.is_downloaded():
            logging.info(f"Missing {dataset.name} dataset. Downloading it now.")
            if not dry_run: dataset.download()
        else:
            logging.info(f"Found {dataset.name}.")


    # run benchmarks
    for dataset in datasets:
        for triplestore in triplestores:
            logging.info(f"Running benchmark for {dataset.name} on {triplestore.name}.")

            #print(util.bash(
            #    f'echo "{pw}" | sudo -S sh -c "/usr/bin/sync; /usr/bin/echo 3 > /proc/sys/vm/drop_caches && /usr/bin/echo \\"caches dropped\\""'))


            # setup iguana configuration for selected dataset and triplestore
            # also maybe adjust timeout and number of runs for specific datasets, as they might require more time
            # especially for wikidata, as a reference tentris takes about 2 hours for a single run (more with other triplestores)
            substitution_map = {
                    "dataset": dataset.name,
                    "triplestore": triplestore.name,
                    "triplestore_endpoint": triplestore.sparql_endpoint,
                    "dataset_queries": dataset.queries_path.absolute(),
                    "timeout_seconds": 180,
                    "warmup_query_runs": 10, # should be at least 1, otherwise benchmark results might be a bit skewed
                    "query_runs": 30,
                    "result_directory": base_dir.joinpath("results").joinpath(f"{triplestore.name}-{dataset.name}"),
            }

            iguana_configuration = iguana.instantiate_template(f"{triplestore.name}-{dataset.name}", base_dir.joinpath("suites"), **substitution_map)
            iguana.run_benchmark(triplestore, dataset, iguana_configuration)
