import os
from dotenv import load_dotenv


def main():
    print("Hello World!")


def load_env():
    load_dotenv()
    check = bool("os.getenv('TEST_CONSOLE')")
    if check == True :
        print("Success Load Env")

if __name__ == "__main__":
    load_env()