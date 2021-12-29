from flask import Flask,request, jsonify
from flask_cors import CORS 
from flask_sqlalchemy import SQLAlchemy
from dataclasses import dataclass
from sqlalchemy import UniqueConstraint

import json

from producer import fanout_publish,topic_publish

app = Flask(__name__) 
app.config["SQLALCHEMY_DATABASE_URI"] = 'mysql://root:root@db/user_update'
CORS(app)

db = SQLAlchemy(app)

@dataclass 
class ProductChangeHistroy(db.Model):
    id = db.Column(db.Integer, primary_key=True) 
    user_id = db.Column(db.Integer)
    product_id = db.Column(db.Integer)

@app.route('/user_api/products',methods=['POST']) 
def create():
    data = request.get_json()
    method = "product_create"
    fanout_publish(method, body=data)
    #routing_key = 'users.product.create'
    #topic_publish(method, body=data, routing_key=routing_key)
    return f'user try to create with body = {json.dumps(data)}' 

@app.route('/user_api/products/<int:id>',methods=['PUT'])
def update(id):
    #name, image, price are required
    return f'user try to update product with {id}'

@app.route('/user_api/products/<int:id>',methods=['DELETE'])
def update(id):
    #name, image, price are required
    return f'user try to delete product with {id}'

if __name__=="__main__":
    app.run(debug=True, host='0.0.0.0')