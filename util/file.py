import os
import json
def get_schema(path: str,table: str) -> dict:
    """Get the table model.

    :return: Table model. Return a dictionary containing a definition of a table
    :rtype: dict
    """
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(os.path.abspath(os.path.join(current_dir, os.pardir)), path, f'{table}.json')
    with open(
        file_path
    ) as f:
        d = json.load(f)
    return d