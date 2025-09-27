import time


def bubble_sort(arr):
    length = len(arr)
    for i in range(length):
        for j in range(0, length-i-1):
            if arr[j] > arr[j+1]:
                arr[j], arr[j+1] = arr[j+1] , arr[j]
    return arr


def fibonacci(limit):
    a, b = 0,1
    ls = []
    for i in range(1,limit):
        ls.append(a)
        a,b = b , a+b        
    return ls

# print(fibonacci(1000))

print(bubble_sort([64, 34, 25, 12, 22, 11, 90]))

def countdown(n):
    while n>0:
        yield n
        n -= 1
for i in countdown(10):
    print(i)
    time.sleep(1)
print("Countdown finished!")

def avg(*nums):
    val = 0
    total_length = len(nums)
    for item in nums:
        val+=item
    avg_value = val/total_length
    return avg_value

res = avg(2,5)
fptr.write('%.2f' % res + '\n')