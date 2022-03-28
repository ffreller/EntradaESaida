from program import ExecuteProgram
from datetime import datetime
from time import sleep

def ExecuteProgramDaily(hour, minute):
    while True:
        agora = datetime.now()
        if (agora.hour == hour) & (agora.minute <= minute):
            ExecuteProgram()
        sleep(560)
        
if __name__ == '__main__':
    ExecuteProgramDaily(6, 10)

