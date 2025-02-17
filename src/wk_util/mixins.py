class EnumReflectMixin:
    @classmethod
    def str2enum(cls, name: str):
        try:
            return getattr(cls, name.upper())
        except AttributeError as e:
            available_options = [name.lower() for name in dir(cls) if not name.startswith('__')]
            raise AttributeError(f'Unsupported option `{e.args[0]}`, available options are {available_options}')