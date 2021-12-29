from flask import Flask 
import os 
import requests

from service import WordRecommendation

app = Flask(__name__) 

SERVICE_PORT = os.getenv("SERVICE_PORT")
GATHERING_HOST = os.getenv("GATHERING_HOST")
GATHERING_PORT = os.getenv("GATHERING_PORT")
API_ENDPOINT = os.getenv("API_ENDPOINT")

def _collect_phrase(phrase):
    URI = f'http://{GATHERING_HOST}:{GATHERING_PORT}/{API_ENDPOINT}'
    _ = requests.post(URI, json={"word": phrase, "lang": "en"})

@app.route('/top-phrases/<phrase>')
def phrase_recommend(phrase):
    #collect phrase from client
    _collect_phrase(phrase)
    
    #return result
    list_recommeded_phrases = word_recommender.top_phrases_for(phrase)
    return list_recommeded_phrases

@app.route('/test-api/<phrase>')
def test_api(phrase):
    return phrase

if __name__ == "__main__":
    
    word_recommender = WordRecommendation()
    word_recommender.start()
    
    app.run(host="0.0.0.0", port=SERVICE_PORT)