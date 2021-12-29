from flask import Flask 
from flask import jsonify
import os 

app = Flask(__name__) 

@app.route('/search-services/<phrase>', methods=['GET'])
def service_handle(phrase):
    result = [f'{phrase}', os.getenv("SERVICE_PORT"), 'string 3']
    return jsonify(result)

if __name__ == "__main__":
    SERVICE_PORT = os.getenv("SERVICE_PORT") 
    app.run(host="0.0.0.0", port=SERVICE_PORT)