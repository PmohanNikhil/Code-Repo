
a = [1,2,3,4]

b = [5,6,7,8]

a.extend(b)

def add(x, y):
    return x + y

def subtract(x, y):
    return x - y

def apply_func(func, a, b):
    return func(a, b)

print(apply_func(add, 3, 5))
print(apply_func(subtract, 10, 4))

ls = ['a', 'b', 'c']

ts = list(map(lambda x: x.upper(), ls))

key_ls = ['id']
merge_condition = " AND ". join(f"target.{key} = source.{key}" for key in key_ls)
print(merge_condition)

print(ts)

a = [2,3,4,5]
res = [val ** 3 for val in a]
print(res)


keys = ['a', 'b', 'c']
values = [1, 2, 3]

key_length = len(keys)
final_dict = {}

for i in range(key_length):
    dict = {keys[i]:values[i]}
    final_dict.update(dict)

print(final_dict)
