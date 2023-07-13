import requests
import json
import pandas as pd
import datetime
import pytz
import zipfile
import os
import logging
import sys
from boxsdk import JWTAuth, Client

working_dir = r'Y:\NAIP_download'

jsonfile = '1736111_nmqx1km3_config_NASS.json'

config_JSON = os.path.join(working_dir, 'Json', jsonfile)

download_output = os.path.join(working_dir, 'Downloads')

download_days = 7

states = ['AK','AL','AR','AZ','CA','CO','CT','DC','DE',
          'FL','GA', 'HI','IA','ID','IL','IN','KS','KY',
          'LA','MA','MD','ME', 'MI','MN','MO','MS','MT','NC',
          'ND','NE','NH','NJ','NM', 'NV','NY','OH','OK','OR',
          'PA','RI','SC','SD','TN','TX', 'UT','VA','VT','WA',
          'WI','WV','WY']

def get_page_num(url):
    response = requests.get(url)
    line_lst = []
    #n = 0
    for line in response.iter_lines():
        #n += 1
        if line: 
            line_lst.append(line)
            #print (f'%%{n}%%', line)
    content_line = line_lst[-1]
    variable_lst = content_line.rsplit(b";")
    metadata = variable_lst[6].rsplit(b'<script>')[-1]
    file_info = metadata.split(b'postStreamData = ')[1]
    data = json.loads(file_info.decode('utf8').replace("'", '"'))
    pageCount = data['/app-api/enduserapp/shared-folder']['pageCount'] 
    return pageCount


def get_fileInfo_df(url):
    response = requests.get(url)
    line_lst = []
    #n = 0
    for line in response.iter_lines():
        #n += 1
        if line: 
            line_lst.append(line)
            #print (f'%%{n}%%', line)
    content_line = line_lst[-1]
    variable_lst = content_line.rsplit(b";")
    metadata = variable_lst[6].rsplit(b'<script>')[-1]
    file_info = metadata.split(b'postStreamData = ')[1]
    data = json.loads(file_info.decode('utf8').replace("'", '"'))
    list_files = data['/app-api/enduserapp/shared-folder']['items']
    df = pd.DataFrame(list_files)
    df['contentUpdated'] = [datetime.datetime.fromtimestamp(i, tz=datetime.timezone.utc ) for i in list(df['contentUpdated'])]
    return df


def get_full_fileInfo_df(page, id):
    final_df = pd.DataFrame()
    for i in range(page):
        url = f'https://nrcs.app.box.com/v/naip/folder/{id}?page={i+1}'
        df = get_fileInfo_df(url)
        final_df = pd.concat([final_df,df],ignore_index=True)
    return final_df


def get_folder_file_dfs(df):
    folder_df = df[df['type']=='folder']
    folder_df = folder_df[folder_df['contentUpdated']>=datetime.datetime.now(tz=pytz.UTC)-datetime.timedelta(days=download_days)]
    file_df = df[df['type']=='file']
    file_df = file_df[file_df['contentUpdated']>=datetime.datetime.now(tz=pytz.UTC)-datetime.timedelta(days=download_days)]
    return folder_df, file_df


def get_download_speed(start_time, end_time):
    init_date = datetime.date.today()
    init_time = str(end_time - start_time)
    format_time = f"{init_date}: {init_time}"
    return format_time
        
    
def get_file_size(file):
    byte_size = os.path.getsize(file)
    # 1 mb = 1048576 Bytes 
    file_size = byte_size / 1048576
    file_size_format = format(file_size, 'g')
    return file_size_format


def get_logger(name, log_file, level=logging.WARNING):    
    log_format = "%(asctime)s - %(message)s"    
    formatter = logging.Formatter(log_format)    
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger
   
     
def main():
    # Define Folder Output
    current = datetime.date.today()
    day = current.strftime("%d")
    mon = current.strftime("%b").upper()
    fldName = mon+'_'+day+'_Download'
    fldPath = os.path.join(download_output, fldName)
    os.mkdir(fldPath)
        
    main_page_url = 'https://nrcs.app.box.com/v/naip'
    # main folder get number of pages
    main_page_num = get_page_num(main_page_url)
    main_df = pd.DataFrame()
    for i in range(main_page_num):
        df = get_fileInfo_df(main_page_url+f'?page={i+1}')
        main_df = pd.concat([main_df,df],ignore_index=True)
    main_folder_df, main_file_df = get_folder_file_dfs(main_df)
    
    #Year folder
    state_info_df = pd.DataFrame()
    for i in main_folder_df.index:
        year_id = main_folder_df['id'][i]
        year_url = f'https://nrcs.app.box.com/v/naip/folder/{year_id}'
        page = get_page_num(year_url)
        state_df = get_full_fileInfo_df(page, year_id)
        state_folder_df, state_file_df = get_folder_file_dfs(state_df)
        state_info_df = pd.concat([state_info_df,state_folder_df],ignore_index=True)
        
    ### Check if Data exists in 'state_info_df'
    if state_info_df.empty:
        # create 'no-data.log' output
        no_data_log = 'no-data.log'
        no_data_name = os.path.join(fldPath, no_data_log)
        no_data_logger = get_logger('no_data_logger', no_data_name)
        no_data_message = f"No Updates have been posted to the NAIP website over the previous {download_days} days"
        no_data_logger.warning(no_data_message)
        sys.exit()
    else:
        pass
   
    state_info_df['name'] = state_info_df['name'].str.upper()
    final_state_df = state_info_df[state_info_df['name'].isin(states)]
    
    #State folders
    in_state_info_df = pd.DataFrame()
    for i in final_state_df.index:
        st_folder_id = final_state_df['id'][i]
        st_url = f'https://nrcs.app.box.com/v/naip/folder/{st_folder_id}'
        page = get_page_num(st_url)
        state_folder_df = get_full_fileInfo_df(page, st_folder_id)
        state_folder_info_df, state_file_info_df = get_folder_file_dfs(state_folder_df)
        file_lst = [final_state_df['name'][i].lower()+r'_n',
                     final_state_df['name'][i].lower()+r'_m',
                     final_state_df['name'][i].lower()+r'_c']        
        
        # got the file end with either _n, _m or _c
        tmp_df = state_folder_info_df[state_folder_info_df['name'].isin(file_lst)]
        # Get the file in rank of _n>_m>_c
        for j in file_lst:
            if any(tmp_df['name']==j):
                in_state_info_df = pd.concat([in_state_info_df, tmp_df[tmp_df['name']==j]],ignore_index=True)
                break
            
    # Actual files
    final_files_df = pd.DataFrame()
    for i in in_state_info_df.index:
        in_folder_id = in_state_info_df['id'][i]
        st_url = f'https://nrcs.app.box.com/v/naip/folder/{st_folder_id}'
        page = get_page_num(st_url)
        instate_df = get_full_fileInfo_df(page, in_folder_id)
        instate_folder_df, instate_file_df = get_folder_file_dfs(instate_df)
        final_files_df = pd.concat([final_files_df,instate_file_df],ignore_index=True)
        
    ### Check if Data exists in 'final_files_df'        
    if final_files_df.empty:
        # create 'no-data.log' output
        no_data_log = 'no-data.log'
        no_data_name = os.path.join(fldPath, no_data_log)
        no_data_logger = get_logger('no_data_logger', no_data_name)
        no_data_message = f"Updates have been posted to the NAIP website over the previous {download_days} days.\nUnfortunately, these NAIP folders were not available for download."
        no_data_logger.warning(no_data_message)
        sys.exit()
    else:        
        # create 'event.log' output
        event_log = 'event.log'
        event_name = os.path.join(fldPath, event_log)
        event_logger = get_logger('event_logger', event_name)
    
        # create 'update.log' output
        update_log = 'update.log'
        update_name = os.path.join(fldPath, update_log)
        update_logger = get_logger('update_logger', update_name)
        update_init_message = f"Updates have been posted to the NAIP website over the previous {download_days} days.\nThe following NAIP folders were available for download:\n"
        update_logger.warning(update_init_message)
        
    # downloading file
    for i in final_files_df.index:
        
        # logging 'event.log' initial output
        event_init_message = f"Downloading NAIP file: {final_files_df['name'][i]} \n"
        event_logger.warning(event_init_message)
        
        # Start Time Lapse
        start_time = datetime.datetime.now()
        
        # Start Processes
        config = JWTAuth.from_settings_file(config_JSON)
        client = Client(config)
        file_id = final_files_df['id'][i]
        file_info = client.file(file_id=str(file_id)).get()
        
        # _c, _n, _m
        file_folder_nm = file_info.parent.name
        parent_folder_info = client.folder(folder_id=file_info.parent.id).get()
        
        # state
        state_folder_nm = parent_folder_info.parent.name
        
        # year
        state_folder_info = client.folder(folder_id=parent_folder_info.parent.id).get()
        year_folder_nm = state_folder_info.parent.name
        
        file_dir = fr'{fldPath}\{year_folder_nm}\{state_folder_nm}\{file_folder_nm}'
        
        if os.path.exists(file_dir) == False:
            try:
                os.makedirs(file_dir)
            except OSError:
                pass
        else:
            pass
        
        # writing the zip files
        save_file_nm = fr'{file_dir}\{final_files_df["name"][i]}'
        with open(save_file_nm, 'wb') as open_file:
            client.file(file_id=f'{file_id}').download_to(open_file)
            open_file.close()
        
        # Get End Time
        end_time = datetime.datetime.now()
        
        # File Size
        get_file_size(save_file_nm)
        
        # File Download Speed
        get_download_speed(start_time, end_time)
        
        # unzip the zip files
        with zipfile.ZipFile(save_file_nm, "r") as zip_extract:
            zip_extract.extractall(save_file_nm[:-4])
            zip_extract.close()
        
        # logging 'event.log' output
        event_message = f"File ID: {file_id}, File Name: {final_files_df['name'][i]} \n File Size: {get_file_size(save_file_nm)} Megabytes, Time Lapse: {get_download_speed(start_time, end_time)} \n Download Location: {file_dir} \n"
        event_logger.warning(event_message)
        
        # logging 'update.log' output        
        update_message = f"{year_folder_nm} - {state_folder_nm} - {file_folder_nm}"        
        # read in 'update.log' 
        with open(update_name, 'r') as file:
            content = file.read()
            # check 'update.log' for 'update_message' 
            if not update_message in content:
                # log 'update_message' into 'update.log'
                update_logger.warning(update_message)
            else:
                pass
        
        # delete the zip files
        os.remove(save_file_nm)
                
if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"Script failed, ERROR: {err}")
        