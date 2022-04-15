#! /usr/bin/python3

import requests
from datetime import date
import sys
import os

def main(args):

    username = os.getlogin()
    response = requests.get("http://www.psudataeng.com:8000/getBreadCrumbData")
    path = "/home/" + username + "/InforMetro/ctran_data/" 
    exists = os.path.exists(path)
    if not exists:
      os.makedirs(path)
    filename = "/home/" + username + "/InforMetro/ctran_data/" + date.today().strftime('%m-%d-%Y') + ".txt"
    f = open(filename, "w")
    f.write(response.text)

if __name__ == '__main__':
  main(sys.argv)