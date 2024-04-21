# DBMaster

Personal Quant Research Database Manager

## Setup 

Choose one of the below approaches to set up.

### A. Install from Pypi

For most of users, simply `pip install` in a **python3.12** environment: (`py3.10`, `py3.11` might work for now but don't guarantee in the future)

```bash
pip install dbmaster[binance]
```

Note: you can remove vendor-specific extra requirements if you don't need.


### B. Run source code

for developers, contributors, or users who want to have finer control over the code.

1. Clone this repo:

```bash
git clone https://github.com/xyshell/dbmaster
```

2. Create a `py3.12` virtual environment. if using conda:

```bash
conda create -n <name-your-py-env> python=3.12
```

```bash
conda activate <name-your-py-env>
```

3. install dependencies

```bash
pip install -r requirements.txt
```

4. development install

```bash
pip install -e .[dev]
```

### C. Install from github release

Find and download the last released package in https://github.com/xyshell/dbmaster/releases.

```bash
pip install dbmaster-x.y.z-py3-none-any.whl[binance]
```

## Hello World

Find location of site-packages by `pip show dbmaster`

Under `site-packages/dbmaster/`, rename `config_example.toml` to `config.toml` and modify the setting for your case. (or override environment variable `DBMASTER_CONFIG_PATH` to point to your modified `config.toml`)

See available commands by:

```bash
python -m dbmaster -h
```

Download data by:

```bash
python -m dbmaster update kline --vendor=binance --freq=1d  --datefrom=2024-04-01 --symbol="['BTCUSDT', 'ETHUSDT']"
```

Check `/exmaple` for more usages.
