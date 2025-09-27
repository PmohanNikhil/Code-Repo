File1 = open('C:\\Users\\pmoha\\OneDrive\\Desktop\\Project-Python\\Pandas and Numpy Project\\example2.txt','w')
File1.write("This is line A \n")
File1.write("This is line B \n")
File1.write("This is line C \n")


lines = ["This is line 1 \n", "This is line 2 \n", "This is line 3 \n"]
with open('C:\\Users\\pmoha\\OneDrive\\Desktop\\Project-Python\\Pandas and Numpy Project\\example3.txt','w') as File2:
    for line in lines:
        File2.write(line)


#append
with open('C:\\Users\\pmoha\\OneDrive\\Desktop\\Project-Python\\Pandas and Numpy Project\\example3.txt','a') as File2:
    File2.write("This is line 4 \n")
    File2.write("This is line 5 \n")
    
#Additional modes
"""
r+ for read and write mode
w+ for write and read mode
a+ for append and read mode
x+ for create and read mode
"""

#Examples
with open('C:\\Users\\pmoha\\OneDrive\\Desktop\\Project-Python\\Pandas and Numpy Project\\example4.txt','x') as File3:
    File3.write("This is line 1 \n")
    File3.write("This is line 2 \n")
    File3.write("This is line 3 \n")

with open('C:\\Users\\pmoha\\OneDrive\\Desktop\\Project-Python\\Pandas and Numpy Project\\example4.txt','r+') as File4:
    text = File4.readlines()
    print(text)
    File4.write("This is line 4 \n")
    File4.write("This is line 5 \n")

with open('C:\\Users\\pmoha\\OneDrive\\Desktop\\Project-Python\\Pandas and Numpy Project\\example4.txt','a+') as File5:
    File5.write("This is line 6 \n")
    File5.write("This is line 7 \n")
    File5.seek(0)
    text = File5.readlines()
    print(text)

with open('C:\\Users\\pmoha\\OneDrive\\Desktop\\Project-Python\\Pandas and Numpy Project\\example4.txt','w+') as File6:
    File6.write("This is line 8 \n")
    File6.write("This is line 9 \n")
    File6.seek(0)
    text = File6.readlines()
    print(text)