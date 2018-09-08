def hello_world():
    return "Hello world"

def increase_age(age):
    return age+2

def findvim(line):
    ind = str(line).find('TwitVim')
    if(ind != -1):
        return True
    else:
        return False
