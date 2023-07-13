from configparser import ConfigParser
import os

working_dir = r'C:\Users\Kevin.Morrison\Desktop\SHORTCUTS\NAIP_Download'

user_info = "NAIP_USER_INFO.txt"

config = ConfigParser()

config["USER_DATA"] = {
    "file": os.path.join(working_dir, user_info)
        } 

with open("user_input.ini", 'w') as file:
    config.write(file)