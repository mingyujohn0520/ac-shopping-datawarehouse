import sys

column_list = ["customer_id", "first_name"]

column_filter = ""
if column_list:
    col_string = "'{}'".format("', '".join(column_list))
    column_filter = f"and column_name in ({col_string.strip()}) "

print("', '".join(column_list))
