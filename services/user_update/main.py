from flask import Flask,request, jsonify
from flask_cors import CORS 
from flask_sqlalchemy import SQLAlchemy
from dataclasses import dataclass
from sqlalchemy import UniqueConstraint

import json

from producer import fanout_publish

app = Flask(__name__) 
app.config["SQLALCHEMY_DATABASE_URI"] = 'mysql://root:root@db/user_update'
CORS(app)

db = SQLAlchemy(app)

@dataclass 
class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=False) 
    name = db.Column(db.String(255)) 
    author = db.Column(db.String(255))
    image = db.Column(db.String(255))
    likes = db.Column(db.Integer)
    price = db.Column(db.Integer)
    is_active = db.Column(db.Boolean, default=True)

@app.route('/user_api/products',methods=['POST']) 
def create():
    data = request.get_json()
    method = "product_create"
    fanout_publish(method, body=data)
    return f'user try to create with body = {json.dumps(data)}' 

@app.route('/user_api/products/<int:id>',methods=['PUT'])
def update(id):
    return f'user try to update product with {id}'

if __name__=="__main__":
    app.run(debug=True, host='0.0.0.0')