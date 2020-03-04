import dropbox
import pandas as pd
import fnmatch


dbx = dropbox.Dropbox(config.dropbox_api)
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


def dbx_pathlist_to_df(list_of_files):
    """
    Download paths into Pandas DF.
    Utilizes download_master for QualityZone2
    *Excel args specify 2nd sheet*
    """
    list_of_df = []
    for f in list_of_files:
        if f.endswith('.csv'):
            _, res = dbx.files_download(f)
            df = pd.read_csv(res.raw,
                             index_col=0,
                             na_values='NAN'
                             )
            df.index = pd.to_datetime(df.index)
            list_of_df.append(df)

        elif f.endswith('.xlsx'):
            _, res = dbx.files_download(f)
            df = pd.read_excel(res.raw,
                               sheet_name=1,
                               index_col=0,
                               na_values='NAN'
                               )
            df.index = pd.to_datetime(df.index)
            list_of_df.append(df)

    return list_of_df
