import logging

def error_handler(*exceptions):
    def handler(func):
        def new_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                logging.error(f"{e.__class__.__name__}: {e}")
            except Exception as e:
                logging.error(f"{e.__class__.__name__}: {e}")
        return new_func
    return handler