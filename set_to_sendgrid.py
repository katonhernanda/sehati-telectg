from configparser import ConfigParser

config = ConfigParser()

config['sendgrid'] = {
        'auth' : ' '
    }

with open('sendgrid.ini','w') as f:
    config.write(f)
