
def add (a,b):
    return a+b

def subtract (a,b):
    return a-b

def func_apply_func(func, a,b):
    return func(a,b)



ls = [1,2,3,4,5]
new_ls = [x*2 for x in ls if x%2 == 0]
print("List comprehension result:", new_ls)

coordinates = [ (x,y) for x in range(3) for y in range(3) if x == y]
print("Coordinates:", coordinates)

mat = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

falt_ls = [val for row in mat for val in row]
print("Flattened+list", falt_ls)

def args_func(*args):
    for val in args:
        print(val)

args_func(1, 2, 3, 4, 5)

def k_args(partition_key,**kwargs):
    print(partition_key)
    for key, value in kwargs.items():
        print(f"{key}: {value}")

k_args("key1", name="Alice", age=30, city="New York")



myDict = {x: x**2 for x in [1,2,3,4,5] if x % 2 == 0}

column_set = ['id','name','age', 'creted_time', 'updated_time']
composite_keys = ['id', 'creted_time']

insert_set = {col: f"source.{col}" for col in column_set}

update_set = {col:f"source.{col}" for col in column_set if col != "updated_time"}
print(insert_set)
print(update_set)
merge_condition = " AND ".join(f"target.{key} = source.{key}" for key in composite_keys)
print(merge_condition)

# detla_table.alias("target").merge(
#     df.alias("source"),  merge_condition)\
#     .whenMatchedUpdate(set = update_set)\
#     .whenNotMatchedInsert(values = insert_set)\
#     .execute()

# while True:
#     print("Enter the first number:")
#     a = int(input())
#     print("Enter the second number:")
#     b = int(input())
#     print("Enter the opertation: 1 for add, 2 for subract, 0 to exit")
#     c = int(input())
#     if c == 1:
#         print(func_apply_func(add, a,b))
#     elif c == 2:
#         print(func_apply_func(subtract, a,b))
#     elif c == 0:
#         print("Exiting the program")
#         break