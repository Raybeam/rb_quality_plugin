def map_opt_arg(arg):
    def decor(func):
        def wrapped(*args):
            if args[arg] is None:
                return None
            return func(*args)

        return wrapped

    return decor
