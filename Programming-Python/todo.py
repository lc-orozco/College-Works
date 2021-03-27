class Todo():

    def __init__(self): 
        self.data = []
          
    def add(self, chore): 
        self.data.append(chore)
    
    def length_dec(self):
        num = len(self.data) 
        pos_nums = []

        while (num != 0):
            pos_nums.append(num % 10)
            num = num // 10
        
        return pos_nums

    def length(self):
        return len(self.data)

def main():
    it = 0
    todo = Todo()
    print("Got anything to do today? List it here (Type Stop now to exit or at any time to conclude the list): ")
    print("")

    while(True):
        it += 1
        chores = input()
        chores_ns = chores.replace(" ", "") 
        if (chores_ns == ""):
            continue
        if (str.upper(chores) == 'STOP'):
            break
        todo.add(chores)

    print("")

    list_length = todo.length_dec()

    if ((str.upper(chores) == 'STOP') and it == 0):
        print("There are no chores to complete today, be the rest of your day okay!")
        exit()

    else:
        if (todo.length() >= 10):
            print("There are " + str(list_length[1] * 10) + "+ chores to display, want me to continue? (Reply Yes or No)")
            cont = input()
            print("")

            if (str.upper(cont) == "NO"):
                exit()

            elif (str.upper(cont) != "YES"):
                while (True):
                    print("Please type either Yes or No")
                    print("")
                    contt = input()
                    print(""
                    )
                    if (str.upper(contt) == "YES"):
                        break
                    elif (str.upper(contt) == "NO"):
                        exit()

        print("Here is the given list of chores to complete today:")

    for i in range(todo.length()):
        print(f'{i + 1}-{todo.data[i]}')

main()