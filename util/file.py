import os
import json
def read_content_json(path: str,table: str) -> dict:
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

def get_schema_datalake(domain,table):
    return read_content_json(f"datalake/schema/{domain}",table)