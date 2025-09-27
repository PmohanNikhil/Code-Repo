"""
r for read mode
w for write mode
a for append mode
x for create mode
b for binary mode
t for text mode
"""
file1 = open('C:\\Users\\pmoha\\OneDrive\\Desktop\\Project-Python\\Pandas and Numpy Project\\example1.txt', 'r')

#To get the mode
print(file1.mode)
print(file1.name)
print(file1.closed)
type(file1)

#Using a with statement to open a file is the best practice

with open('C:\\Users\\pmoha\\OneDrive\\Desktop\\Project-Python\\Pandas and Numpy Project\\example1.txt', 'r') as file2:
    # text = file2.read()
    text = file2.readlines()
    print(text)
print(file2.closed)