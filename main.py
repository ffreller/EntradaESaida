import os
import sys


if __name__ == '__main__': 
    # Set library path for SQLAlchemy
    dir_path = os.path.dirname(os.path.realpath(__file__))
    instant_client_path = os.path.join(dir_path, 'instantclient_21_5')
    os.environ['LD_LIBRARY_PATH'] = instant_client_path
    os.environ['TZ'] = 'America/Sao_Paulo'
    # Run program.py
    os.execv(sys.executable, [sys.executable] + ['program.py'])