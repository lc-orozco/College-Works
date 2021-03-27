
def fibonacci(x, y):
    #i = x
    #j = y

    print(f'{x}, {y}, {(x + y) / y}')

    x = x + y
    y = x + y

    if (x > 1000000000000000000):
        return

    return fibonacci(x, y)

def main():
    
    fibonacci(0, 1)

main()

