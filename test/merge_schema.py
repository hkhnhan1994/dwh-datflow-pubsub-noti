lst1 = [{"id": 1, "x": "one"}, {"id": 2, "x": "two"}]
lst2 = [{"id": 1, "x": "one"}, {"id": 3, "x": "three"}, {"id": 5, "x": "five"}]

# Convert lists to dictionaries with "id" as the key
dict1 = {item['id']: item for item in lst1}
dict2 = {item['id']: item for item in lst2}

# Merge dictionaries
merged_dict = {**dict1, **dict2}

# Convert merged dictionary back to a list
merged_list = list(merged_dict.values())

# Identify items in lst1 that are not in lst2
any(for item in lst2 if item['id'] not in dict1)
diff_list = [item for item in lst2 if item['id'] not in dict1]
diff_list = [item for item in lst2 if item['id'] not in dict1]

print("Merged List:")
print(merged_list)
print("Different items in lst1 compared to lst2:")
print(diff_list)
