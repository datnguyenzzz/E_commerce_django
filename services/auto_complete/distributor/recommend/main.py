from flask import Flask 
import os 
import requests

from service import WordRecommendation

app = Flask(__name__) 

@app.route('/top-phrases/<phrase>')
def phrase_recommend(phrase):
    word_recommender = WordRecommendation()
    word_recommender.start()
    
    list_recommeded_phrases = word_recommender.top_phrases_for(phrase)
    return list_recommeded_phrases

@app.route('/test-api/<phrase>')
def test_api(phrase):
    print(f'request to : http://search-service-1:5001/search-services/{phrase}')
    r = requests.get(f'http://search-service-1:5001/search-services/{phrase}')
    print(r.text)
    return r.text

if __name__ == "__main__":
    SERVICE_PORT = os.getenv("SERVICE_PORT")
    app.run(host="0.0.0.0", port=SERVICE_PORT)