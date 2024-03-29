def check_membership_and_update(source_list, current_list):
  missing_members = set(source_list) - set(current_list)
  extra_members = set(current_list) - set(source_list)
  return missing_members, extra_members

  # Example usage
source_list = ["apple", "banana", "orange", "grape"]
current_list = ["apple", "banana", "cherry"]

missing_members, extra_members = check_membership_and_update(source_list, current_list)
print(missing_members)  # Output: [{"type": "missing", "member": "orange"}, {"type": "missing", "member": "grape"}, {"type": "extra", "member": "cherry"}]
print(extra_members)