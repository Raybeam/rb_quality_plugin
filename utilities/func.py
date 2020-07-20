def map_opt_arg(arg):
    """
    Decorator that takes an argument number `n`. If the provided function has the value
    None for the argument at that index, we short circuit and return None instead of
    calling the decorated function
    :param arg: What arg to check for None-ness
    :type arg: Int
    :return: A decorated function
    """
      def decor(func):
          def wrapped(*args):
              if args[arg] is None:
                  return None
            return func(*args)

        return wrapped

    return decor

def option_between(self, value, low, high):
    if low is not None and value < low:
        return False
    if high is not None and value > high:
        return False
    return True
