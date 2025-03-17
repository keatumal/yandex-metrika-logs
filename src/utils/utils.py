def populate_with_attribution(src_map: dict, attr_map: dict) -> dict:
    result = dict()

    for map_key, map_value in src_map.items():
        if "<attr>" not in map_key:
            result[map_key] = map_value
            continue
        for attr_key, attr_value in attr_map.items():
            new_key = map_key.replace("<attr>", attr_key)
            new_value = map_value.replace("<attr>", attr_value)
            result[new_key] = new_value

    return result
