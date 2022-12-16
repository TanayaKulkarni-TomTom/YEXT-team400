def get_or_default(d, *keys, default=None):
    """
    Returns a value from dict by it's keys.
    If key is missing returns default.
    :param d: object
    :param keys: keys in object
    :param default: the default value if value is missing for that key
    :param returnBool: if set to true - the True will be returned if value for key is found
    Example:
        d = {"a": {"b": {"c": [41, 42]}}}
        print(get_or_default(d, "a", "b", "c", 1)) # => 42
        print(get_or_default(d, "a", "b", "d", default=43)) # => 43

    """
    try:
        for k in keys:
            d = d[k]
    except (KeyError, IndexError):
        return default
    return d
