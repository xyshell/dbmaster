from typing import Callable


class VendorBase:
    pass


class KlineVendorBase(VendorBase):
    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)

        if "universe" not in cls.__dict__:  # abstract class property
            raise NotImplementedError("universe is not implemented")


class VendorFactory:
    @classmethod
    def get(cls, name: str) -> VendorBase:
        import dbmaster.vendor as vendor

        vendor_name = name.capitalize()
        vendor_cls = getattr(vendor, vendor_name)
        return vendor_cls

    @classmethod
    def get_handler(cls, name: str, catalog: str) -> Callable:
        func_name = f"get_{catalog}"
        return getattr(cls.get(name), func_name)


__all__ = ["VendorFactory"]
