from flask import Flask, jsonify
import os 

from Servers import Servers
app = Flask(__name__) 

PORT_OFFSET = int(os.getenv("PORT_OFFSET"))
SERVICE_PORT = int(os.getenv("SERVICE_PORT"))

@app.route('/search-services/<phrase>', methods=['GET'])
def service_handle(phrase):
    
    top_phrases_result = server.top_phrases_for(phrase)
    if len(top_phrases_result) == 0:
        return jsonify([phrase])
    
    return jsonify(top_phrases_result)

if __name__ == "__main__":
    partition = SERVICE_PORT - PORT_OFFSET
    
    server = Servers(partition)
    server.start()
    
    app.run(host="0.0.0.0", port=SERVICE_PORT)