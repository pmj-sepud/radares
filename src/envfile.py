import dotenv
import os
import sys

def load():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    project_dir = os.path.join(dir_path, os.pardir)
    sys.path.append(project_dir)
    dotenv_path = os.path.join(project_dir,'.env')
    dotenv.load_dotenv(dotenv_path)
