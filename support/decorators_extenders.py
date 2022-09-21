def init_only_existing(cls):
    real_init = cls.__init__

    def pre_init(self, *args, **kwargs):
        try:
            self._pre_init__
        except AttributeError:
            pass
        else:
            args, kwargs = self._pre_init__(*args, **kwargs)

        kwargs = {
            k: v for k, v in kwargs.items()
            if k in self.__dataclass_fields__
        }
        real_init(self, *args, **kwargs)

    cls.__init__ = pre_init
    return cls
