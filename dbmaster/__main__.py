import fire


from dbmaster.command import Update


class Main:
    """DBMaster command line interface.

    Environment Variables:
        - DBMASTER_CONFIG_PATH: Path to config file
        - DBMASTER_MAX_WORKERS: Max number of workers, default is number of cpu cores
            set lower if you experience too frequent bans from vendor API or database locking.
    """

    def __init__(self):
        self.update = Update()


if __name__ == "__main__":
    fire.Fire(Main())
