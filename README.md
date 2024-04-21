# dbmaster

Personal Quant Research Database Manager

## Setup 

Depends on your circumstance, you might choose one of the below approaches to set up.

### A. Run source code

for developers, contributors, or users who want to have finer control over the code.

1. Clone this repo:

```bash
git clone https://github.com/xyshell/dbmaster
```

2. Create a `py3.12` virtual environment. (`py3.10`, `py3.11` might work for now but don't guarantee in the future)m if using conda:

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

### B. Install python packages

For most of users, find and download the last released package in https://github.com/xyshell/dbmaster/releases.

```bash
pip install dbmaster-x.y.z-py3-none-any.whl[binance]
```

Note: you can remove vendor-specific extra requirements if you don't need.


## Hello World

under `dbmaster/`, rename `config_example.toml` to `config.toml` and modify the setting for your case.

See available commands by:

```bash
python -m dbmaster -h
```

download data by:

```bash
python -m dbmaster update kline --vendor=binance --freq=1d  --datefrom=2024-04-01 --symbol="['BTCUSDT', 'ETHUSDT']"
```


check `/exmaple` for more usages.
