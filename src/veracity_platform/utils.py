def fix_aiohttp():
    """ Fixes "event loop is closed" bug in aiohttp.

    Reference:
        https://github.com/aio-libs/aiohttp/issues/4324#issuecomment-733884349
    """
    from functools import wraps
    from asyncio.proactor_events import _ProactorBasePipeTransport

    def silence_event_loop_closed(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except RuntimeError as e:
                if str(e) != "Event loop is closed":
                    raise

        return wrapper

    _ProactorBasePipeTransport.__del__ = silence_event_loop_closed(
        _ProactorBasePipeTransport.__del__
    )
