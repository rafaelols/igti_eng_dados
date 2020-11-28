import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime

# Get access variables
with open('./vaccess_keys.txt') as access_file:
    vaccess = json.load(access_file)

# Cadastrar as chaves de acesso
consumer_key = vaccess['consumer_key']
consumer_secret = vaccess['consumer_secret']
access_token = vaccess['access_token']
access_token_secret = vaccess['access_token_secret']

# Arquivo de saída para armazenar os tweets coletados
data_atual = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
out = open(f"collected_tweets{data_atual}.txt", "w")

# Implementar uma classe para conexão com o Twitter
class MyListener(StreamListener):
    def on_data(self, data):
        itemString = json.dumps(data)
        out.write(itemString + "\n")
        return True

    def on_error(self, status):
        print(status)

# Implementar main
if __name__ == "__main__":
    l = MyListener()
    auth= OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=["Trump"])