from flask import Flask 
import os 
import requests

from service import WordRecommendation

app = Flask(__name__) 

@app.route('/top-phrases/<phrase>')
def phrase_recommend(phrase):
    list_recommeded_phrases = word_recommender.top_phrases_for(phrase)
    return list_recommeded_phrases

@app.route('/test-api/<phrase>')
def test_api(phrase):
    return phrase

if __name__ == "__main__":
    SERVICE_PORT = os.getenv("SERVICE_PORT")
    
    word_recommender = WordRecommendation()
    word_recommender.start()
    
    app.run(host="0.0.0.0", port=SERVICE_PORT)