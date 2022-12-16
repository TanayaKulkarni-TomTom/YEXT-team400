def multi_column_category_mapper(raw_categories, lookup_table):
    """
    Compares list of raw_categories with lookup dict - recoding_schema
    to find best possible match. Returns dict with gdf category/subcategory or none
    """
    mapped_row = None
    for code in raw_categories:
        mapped_row = lookup_table.get(code)
        if mapped_row != None:
            return mapped_row

    return mapped_row
