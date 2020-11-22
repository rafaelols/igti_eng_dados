import json
from tweepy import OAuthHandler

# Get access variables
with open('./vaccess_keys.txt') as access_file:
    vaccess = json.load(access_file)

# Cadastrar as chaves de acesso
consumer_key = vaccess['consumer_key']
consumer_secret = vaccess['consumer_secret']
access_token = vaccess['access_token']
access_token_secret = vaccess['access_token_secret']

# Arquivo de saída para armazenar os tweets coletados

