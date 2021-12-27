from flask import Flask 
import os 

from service import WordRecommendation

app = Flask(__name__) 

@app.route('/top-phrases/<phrase>')
def phrase_recommend(phrase):
    #word_recommender = WordRecommendation()
    #word_recommender.start()
    
    #list_recommeded_phrases = word_recommender.top_phrases_for(phrase)
    #return list_recommeded_phrases
    return phrase

if __name__ == "__main__":
    SERVICE_PORT = os.getenv("SERVICE_PORT")
    app.run(host="0.0.0.0", port=SERVICE_PORT)