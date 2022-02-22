import os
from dotenv import load_dotenv
from slack_sdk import WebClient
import time
import json
from slack_sdk.errors import SlackApiError
import shlex
import subprocess
from datetime import date
import math
import docker
import psycopg2

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
          "value": "Postgres Master "+os.getenv('PURPOSE')
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
  fileloc = os.getcwd()+'/'+os.getenv('SAVED')+'/'+today.strftime("%b-%d-%Y")+'-'+os.getenv('PURPOSE')+'.dmp'
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
  return [fileloc,today.strftime("%b-%d-%Y")+'-'+os.getenv('PURPOSE')+'.dmp']

def upload_data(client,fileloc):
  send_message(client,'Trying Upload Data', 1)
  size = os.stat(fileloc[0]).st_size
  convert = convert_size(size)
  
  if ((convert[1] >= 100 and convert[2] == 'MB') or (convert[2] == "GB" and convert[1] >= 1)):
    splitfile(fileloc,client)
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

def verify_backup(client,fileloc):
  container_name = os.getenv('CONTAINER_TEMP')
  # COUNTUSER=$(psql -P format=wrapped  -T -X -A -U ${USERNAME} -h ${HOSTNAME} -d ${DATABASE} -c 'SELECT COUNT(*) FROM users')
  os.system('docker stop '+container_name)
  os.system('docker rm '+container_name)
  send_message(client,'Verifying Backup', 1)
  clientDocker = docker.from_env()
  container_name = os.getenv('CONTAINER_TEMP')
  DB_HOST = os.getenv('DB_HOST')
  DB_NAME = os.getenv('DB_NAME')
  DB_USER = os.getenv('DB_USERNAME')
  DB_PORT = os.getenv('DB_PORT')
  DB_PASSWORD = os.getenv('DB_PASSWORD')
  # temp_pg = clientDocker.containers.run("postgres:12",detach=True,name=container_name,environment=["POSTGRES_DB={DB_NAME}","POSTGRES_USER={DB_USER}","POSTGRES_PASSWORD={DB_PASSWORD}"],ports={'5432/tcp': 2222})
  os.system('docker run --name '+container_name+' -e POSTGRES_DB='+DB_NAME+' -e POSTGRES_USER='+DB_USER+' -e POSTGRES_PASSWORD='+DB_PASSWORD+' -p 127.0.0.1:5490:5432 -d postgres:12')
  # Copy File
  os.system('docker cp backup/'+fileloc[1]+' '+container_name+':/tmp/'+fileloc[1])
  time.sleep(10)
  # Unzip File
  os.system('docker exec '+container_name+' gunzip /tmp/'+fileloc[1])
  time.sleep(10)
  # Restore Backup
  os.system('docker exec '+container_name+' pg_restore -U '+DB_USER+' -d '+DB_NAME+' -1  /tmp/'+fileloc[1])
  time.sleep(10)
  if check_data():
    send_message(client,'Verified Backup (Restore Docker Method)', 2)
  else:
    send_message(client,'Unverified Backup (Restore Docker Method)', 2)
  os.system('docker stop '+container_name)
  os.system('docker rm '+container_name)
  send_message(client,'Cleanup Container', 2)

def check_data():
  DB_HOST = os.getenv('DB_HOST')
  DB_NAME = os.getenv('DB_NAME')
  DB_USER = os.getenv('DB_USERNAME')
  DB_PORT = os.getenv('DB_PORT')
  DB_PASSWORD = os.getenv('DB_PASSWORD')
  connectionReal = psycopg2.connect(user=DB_USER,
                                  password=DB_PASSWORD,
                                  host=DB_HOST,
                                  port=DB_PORT,
                                  database=DB_NAME)

  connectionTemp = psycopg2.connect(user=DB_USER,
                                  password=DB_PASSWORD,
                                  host="127.0.0.1",
                                  port="5490",
                                  database=DB_NAME)

  cursor = connectionReal.cursor()
  cursor.execute("SELECT COUNT(*) from users")
  cursor2 = connectionTemp.cursor()
  cursor2.execute("SELECT COUNT(*) from users")
  # Fetch result
  record = cursor.fetchone()
  record2 = cursor2.fetchone()
  return record == record2

def splitfile(fileloc,client):
  os.system('split '+fileloc[0]+' -b 124288000 '+fileloc[1])
  # time.sleep(30)
  currentpwd = os.getcwd()
  for path, currentDirectory, files in os.walk(currentpwd):
    for file in files:
        if file.startswith(fileloc[1]+'a'):
            response = client.files_upload(
                channels=os.getenv('CHANNEL_NAME'),
                file=currentpwd+'/'+file,
                title=file
            )
            os.remove(file)  

if __name__ == "__main__":
  load_env()
  slack_token = os.getenv("TOKEN_BOT")
  client = WebClient(token=slack_token,timeout=120000)
  send_message(client,'Starting Backup', 1)
  fileloc = load_data(client)
  # fileloc = ["/home/directoryx/workspace/backup/backup/Jan-30-2022-survey.dmp","Jan-30-2022-survey.dmp"]
  if fileloc != None:
    upload_data(client,fileloc)
    verify_backup(client,fileloc)

  os.remove(fileloc[0])
  send_message(client,'Finished Backup', 2)


  
