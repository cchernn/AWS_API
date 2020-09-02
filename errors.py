import logging

logging.basicConfig(format="%(asctime)s, %(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d:%H:%M:%S"
)

def error_handler(*exceptions):
    def handler(func):
        def new_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                logging.error(f"{e.__class__.__name__}: {e}")
                return {
                    "error": str(e)
                }
            except Exception as e:
                logging.error(f"{e.__class__.__name__}: {e}")
                return {
                    "error": str(e)
                }
        return new_func
    return handler

if __name__ == "__main__":
    pass