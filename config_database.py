from configparser import ConfigParser

config = ConfigParser()

config['db'] = {
        'db_name' : ' ',
        'db_address' : ' ',
        'db_port' : ' ',
        'db_password' : ' ',
        'db_username' : ' '
    }

with open('./creden.ini','w') as f:
    config.write(f)
