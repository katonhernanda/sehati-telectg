from configparser import ConfigParser

config = ConfigParser()

config['sendgrid'] = {
        'auth' : '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'
    }

with open('sendgrid.ini','w') as f:
    config.write(f)
