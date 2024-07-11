"""
Helper functions
"""

def dict_factory(cursor, row):
    """
    Utility factor to allow results to be used like a dictionary
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def to_json_list(cursor):
    """
    Helper function that converts query result to json list, after cursor has 
    executed a query.
    """
    results = cursor.fetchall()
    headers = [d[0] for d in cursor.description]
    return [dict(zip(headers, row)) for row in results]
