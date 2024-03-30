import typing
def convert_nametuple(schema,table_name):
    class_attributes = []
    columns = schema['fields']
    for col in columns:
        if col["type"] == "STRING":
            attribute_type=str
        elif col["type"] == "INTEGER":
            attribute_type=int
        elif col["type"] == "TIMESTAMP":
            attribute_type=str #Timestamp
        elif col["type"] == "BOOLEAN":
            attribute_type=bool
        # class_attributes[col['name')] = attribute_type
        class_attributes.append(col["name"], attribute_type)
    generated_class= typing.NamedTuple(table_name, class_attributes)
    # LogicalType.register_logical_type(MillisInstant)
    return generated_class