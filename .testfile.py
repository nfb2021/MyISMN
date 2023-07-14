import os
from MyISMN.flaghandler import Flags  

if __name__ == '__main__':
    root = os.getcwd()
    # try:
    #     os.remove('logger.log')
    # except FileNotFoundError:
    #     pass


    database_name = os.path.join('/home', 'nbader', 'Documents', 'ISMN_data_opener_trial', 'Data_separate_files_header_19500101_20230712_10307_yOnD_20230712')
    if os.path.isdir(database_name):
        flags = Flags(database_name)
        # flags.get_all_numbers()
   
        flags.make_sensor_ids()   
        flags.get_flag_df()
        flags.get_flag_normalization_df()
        flags.normalize_flag_df()

    else:
        raise(f'No database named {database_name} found')
