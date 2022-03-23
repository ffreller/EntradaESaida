import os
import sys

if __name__ == '__main__': 
    # Set library path for SQLAlchemy
    new_lib = '/home/ffreller/EntradaESaida/V3_UI-UTI/instantclient_21_5'
    os.environ['LD_LIBRARY_PATH'] = new_lib
    # Run program.py
    os.execv(sys.executable, [sys.executable] + ['program.py']) 