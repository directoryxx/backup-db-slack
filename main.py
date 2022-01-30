import os
from dotenv import load_dotenv
from slack_sdk import WebClient
import json
from slack_sdk.errors import SlackApiError
import shlex
import subprocess
from datetime import date
import math


def load_env():
    load_dotenv()
    check = bool("os.getenv('TEST_CONSOLE')")
    if check == True :
        print("Success Load Env")

def send_message(client,message,stat):
  if stat == 1:
    emoji = ':thunder_cloud_and_rain:'
  else :
    emoji = ':sunny:'
  
  
  intro_msg  = json.dumps([
    {
      "text": emoji+" "+message,
      "color": "good",
      "fields": [
        {
          "title": "master",
          "value": "Postgres Master Revamp"
        }
      ]
    }
  ])

  response = client.chat_postMessage(
      channel=os.getenv('CHANNEL_NAME'),
      username="os.getenv('CHANNEL_NAME')",
      text=emoji+" "+message,
      color="good",
      attachments=intro_msg, 
      as_user=True
  )

def load_data(client):
  send_message(client,'Starting Load Data', 1)
  DB_HOST = os.getenv('DB_HOST')
  DB_NAME = os.getenv('DB_NAME')
  DB_USER = os.getenv('DB_USERNAME')
  DB_PORT = os.getenv('DB_PORT')
  DB_PASSWORD = os.getenv('DB_PASSWORD')
  today = date.today()
  fileloc = os.getcwd()+'/'+os.getenv('SAVED')+'/'+today.strftime("%b-%d-%Y")+'.dmp' 
  command = f'pg_dump -Fc --host={DB_HOST} ' \
          f'--dbname={DB_NAME} ' \
          f'--username={DB_USER} ' \
          f'--port={DB_PORT} ' \
          f'--no-password ' \
          f'--file={fileloc}'
  proc = subprocess.Popen(command, shell=True, env={
                   'PGPASSWORD': DB_PASSWORD
                   })
  proc.wait()
  send_message(client,'Completed Load Data', 2)
  return [fileloc,today.strftime("%b-%d-%Y")+'.dmp']

def upload_data(client,fileloc):
  send_message(client,'Trying Upload Data', 1)
  size = os.stat(fileloc[0]).st_size
  convert = convert_size(size)
  if (convert[1] >= 500 and convert[2] == 'MB'):
    print("split")
  else:
    response = client.files_upload(
        channels=os.getenv('CHANNEL_NAME'),
        file=fileloc[0],
        title=fileloc[1]
    )
  send_message(client,'Successful Upload Data', 2)

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return ["%s %s" % (s, size_name[i]),s,size_name[i]]

if __name__ == "__main__":
  load_env()
  slack_token = os.getenv("TOKEN_BOT")
  client = WebClient(token=slack_token)
  send_message(client,'Starting Backup', 1)
  fileloc = load_data(client)
  if fileloc != None:
    upload_data(client,fileloc)

  