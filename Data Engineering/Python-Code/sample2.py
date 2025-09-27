import time
def fibonacci(limit):
    a,b = 0,1
    while a<limit:
        yield a
        a,b = b,a+b

limit = 5
for i in fibonacci(limit):
    print(i)


def countdown(n):
    while n > 0:
        yield n
        n-=1
        time.sleep(1)
for i in countdown(100):
    print(i)
