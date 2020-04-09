import dropbox
import pandas as pd
import fnmatch
import yaml
import os
import io

dropbox_config = 'auth.yaml'

with open(dropbox_config, 'r') as config_file:
    config = yaml.safe_load(config_file)

dbx = dropbox.Dropbox(config['dropbox']['dropbox_sdk'])
dbx.users_get_current_account()


def get_dbx_paths(folder):
    """
    dropbox specific functions for creating list of files in directory
    """
    file_list = []
    result = dbx.files_list_folder(folder, recursive=True)

    def process_entries(entries):
        for entry in entries:
            if isinstance(entry, dropbox.files.FileMetadata):
                file_list.append([entry.path_display])

    process_entries(result.entries)
    while result.has_more:
        result = dbx.files_list_folder_continue(result.cursor)
        process_entries(result.entries)

    return file_list


def match_filename(paths, pattern):
    """
    Input a list of paths, check for matches of filename or file extension, return the matched list.
    The rules for filename pattern matching can be found in the fnmatch docs.
    """
    new_list = []

    for file in paths:
        if fnmatch.filter(file, pattern):
            new_list.append(file)

    new_list = [i[0] for i in new_list]
    return new_list


def dbx_pathlist_to_df(list_of_files, xls_index_col=0, xls_skip_rows=0, xls_sheet_name=0, xlsx_sheet_num=1):
    """
    Download paths into Pandas DF.
    Utilizes download_master for QualityZone2, with rules for .csv and .xlsx
    :param xls_index_col: location of index in old xls files
    :param xls_skip_rows: skip rows at top of xls file
    :param xls_sheet_name: name OR num
    :return: pandas DF
    """
    list_of_df = []
    for f in list_of_files:
        if f.endswith('.csv'):
            _, res = dbx.files_download(f)
            with io.BytesIO(res.content) as stream:
                df = pd.read_csv(stream,
                             index_col=0,
                             na_values='NAN')
                df.index = pd.to_datetime(df.index)
            list_of_df.append(df)
            print(f)

        elif f.endswith('.xlsx'):
            _, res = dbx.files_download(f)
            with io.BytesIO(res.content) as stream:
                df = pd.read_excel(stream,
                               sheet_name=xlsx_sheet_num,
                               index_col=0,
                               na_values='NAN')
                df.index = pd.to_datetime(df.index)
            list_of_df.append(df)
            print(f)
            
        elif f.endswith('.xls'):
            _, res = dbx.files_download(f)
            with io.BytesIO(res.content) as stream:
                df = pd.read_excel(stream,
                                sheet_name=xls_sheet_name,
                                index_col=xls_index_col,
                                skiprows=xls_skip_rows,
                                na_values='NAN')
                df.index = pd.to_datetime(df.index)
            list_of_df.append(df)
            print(f)

    return list_of_df


def print_columns(list):
    """
    Returns the number of columns for each dataframe in a list of dataframes.
    """
    for i in range(len(list)):
            print('Dataframe ' + str(i) + ' in list has: ' + str(len(list[i].columns)) + ' columns')
        
        
def file_to_df(path):
    """
    Same as dbx_pathist_to_df, for singe files. I will likely add args as problems appear.
    :param path: the file in question
    :return: pandas DF
    """
    if path.endswith('.csv'):
        _, res = dbx.files_download(path)
        with io.BytesIO(res.content) as stream:
            df = pd.read_csv(stream,
                             index_col=0,
                             na_values='NAN')
            df.index = pd.to_datetime(df.index)
        return df

    elif path.endswith('.xlsx'):
        _, res = dbx.files_download(path)
        with io.BytesIO(res.content) as stream:
            df = pd.read_excel(stream,
                               sheet_name=1,
                               index_col=0,
                               na_values='NAN')
            df.index = pd.to_datetime(df.index)
        return df

def time_zero(df):
    """
    Determines if a datalogger clock (or any Pandas DF with datetime index) is increminting off of a 00:00 start time. 
    If odd clock entries are found, only the first instance  is printed. Ex: 2019-08-13 10:43:00. 
    :param df: dataframe
    """
    failures = 0
    first_run = True
    for i in df.index.astype(str):
        if (i.endswith('0:00')) == False:
            failures += 1
            if first_run:
                print('First timestamp misalignment found at ' + str(i))
                first_run=False
    print(str(failures) + ' total timestamps misaligned in DF')
    print()
       
def check_wy(df, wy):
    """
    check if dataframe contains dates outside of the water year. 
    Returns df with data outside range.
    Dataframe must have datetime index. 
    :param wy: Water Year in YY format, e.g. WY2011 is 11
    """
    start_remove = pd.to_datetime('20' + str(wy-1) + '-10-01 00:00:00')
    end_remove = pd.to_datetime('20' + str(wy) + '-10-01 00:00:00')
    
    df_outside = df.loc[(df.index < start_remove) | (df.index > end_remove)]
    if len(df_outside) > 0:
        print('Number of timestamps outside of WY' + '20' + str(wy) + ': ' + str(len(df_outside)))
        print('---------------------------------------------')
    else:
        print('No timestamps found outside of WY' + '20' + str(wy))
    print(df_outside)



