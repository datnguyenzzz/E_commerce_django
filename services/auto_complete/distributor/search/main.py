from flask import Flask 
import os 

app = Flask(__name__) 

@app.route('/search-services/<phrase>')
def service_handle(phrase):
    return f'FROM SEARCH SERVICE with requested: {phrase} in port: {os.getenv("SERVICE_PORT") }'

if __name__ == "__main__":
    SERVICE_PORT = os.getenv("SERVICE_PORT") 
    app.run(host="0.0.0.0", port=SERVICE_PORT)