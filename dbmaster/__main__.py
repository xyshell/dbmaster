from collections import defaultdict
import json
import fire

from dbmaster.command import Update, Compute


class Main:
    """DBMaster command line interface.

    Environment Variables:
        - DBMASTER_CONFIG_PATH: Path to config file
        - DBMASTER_MAX_WORKERS: Max number of workers, default is number of cpu cores
            set lower if you experience too frequent bans from vendor API or database locking.
    """

    def __init__(self):
        self.update = Update()
        self.compute = Compute()

    def list(self) -> None:
        """List all available catalogs and vendors."""
        from dbmaster.catalog import CatalogBase
        from dbmaster.vendor import VendorBase
        from dbmaster.util import get_subclasses

        catelog_clses = CatalogBase.__subclasses__()
        vendor_clses = get_subclasses(VendorBase)
        holder = defaultdict(list)
        for catelog_cls in catelog_clses:
            for vendor_cls in vendor_clses:
                if catelog_cls.name in vendor_cls.has:
                    holder[catelog_cls.name].append(vendor_cls.name)
        print("Available catalogs and vendors:")
        print(json.dumps(holder, sort_keys=True, indent=4))


if __name__ == "__main__":
    fire.Fire(Main())
