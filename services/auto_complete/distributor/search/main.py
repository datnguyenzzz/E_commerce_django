from flask import Flask, jsonify
import os 

from Servers import Servers
app = Flask(__name__) 

PORT_OFFSET = int(os.getenv("PORT_OFFSET"))
SERVICE_PORT = int(os.getenv("SERVICE_PORT"))

@app.route('/search-services/<phrase>', methods=['GET'])
def service_handle(phrase):
    
    partition = SERVICE_PORT - PORT_OFFSET
    
    server = Servers(partition)
    server.start()
    
    result = [f'{phrase}', os.getenv("SERVICE_PORT"), 'string 3']
    return jsonify(result)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=SERVICE_PORT)