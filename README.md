# Benchmark Script

## Requirements
Following executables need to be installed:

- `bash`
- `curl`
- `wget`
- `zstd`
- (`python 3.13`)

For tentris, set `ulimit -n 64000` to your .bashrc. Log out and in again to apply the changes.

## Initializing the Environment

We recommend to install python 3.13 through pyenv. To do so, run the following commands:
```bash
curl https://pyenv.run | bash
export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
```

Then, install python 3.13:
```bash
pyenv install 3.13 # make sure this is successful. If not, install the required dependencies from your package manager, run again and confirm [Y] if asked to continue because it already exists. 
```

Afterwards set up the environment:
```bash
pyenv local 3.13
pip install -r requirements.txt
```

## Execution

Before benchmarking tentris and fuseki, run the `tentris-os-optimizations.sh` file. It will require
admin privileges for execution.

To run the benchmarks execute the `bench.py` file:
```bash
python3 bench.py
```

## Relevant files

- The file `bench.py` controls the general flow of the benchmarks and executes them
- The file `triplestore.py` contains the code for controlling the triplestores. You can add a class for your own system in that file. The download method can be replaced with a simple copy function, if your system
is not available publicly.

## Some notes

- Oxigraph probably doesn't work for some reasons (it doesn't load any data into the database)
- Virtuoso also might not work
- Tentris might return a 501 error during benchmarks, can be ignored probably
- For best results, consider dropping memory caches in between different benchmarks 
(`bench.py` contains commented code for that, though it will need admin privileges and store the user's password)
- If the loading of a database fails, you will need to delete the corresponding database directory to be
able to reload it again

## Configurations

- There is a `template.yml` file which works as a template for benchmark configurations Iguana uses.
There are some notes regarding configuration.
- There are also some comments left inside the `bench.py` file regarding benchmark configurations
- Memory polling rate during loading can be adjusted in the `util.py` in the method `monitor_memory_usage`. The default value for the interval is always used.

## Results

- Loading results are stored in `benchmarks/logs/.../loading_stats.json`
- Some triplestores create snapshots of their databases (tentris for example), so their reported database sizes might actually be lower
- Iguana results are stored under the `benchmarks/results/` directory
- There is some explanation for the results in the documentation of iguana: https://dice-group.github.io/IGUANA//docs/latest/configuration/, relevant chapters are result storage, metrics and rdf results if interested
