from flask import Flask 
app = Flask(__name__) 

@app.route('/top-phrases')
def phrase_recommend():
    return "Mother ficker !!!"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7777)