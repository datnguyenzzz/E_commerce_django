from flask import Flask 
from flask_cors import CORS 
from flask_sqlalchemy import SQLAlchemy
from dataclasses import dataclass
from sqlalchemy import UniqueConstraint

app = Flask(__name__) 
app.config["SQLALCHEMY_DATABASE_URI"] = 'mysql://root:root@db/user_update'
CORS(app)

db = SQLAlchemy(app)

@dataclass 
class ProductUser(db.Model):
    id = db.Column(db.Integer, primary_key=True) 
    user_id = db.Column(db.Integer) 
    product_id = db.Column(db.Integer)

    UniqueConstraint('user_id', 'product_id', name='user_product_unique')

@app.route('/user_api/products',methods=['POST']) 
def create():
    return 'user try to create' 

@app.route('/user_api/products/<int:id>',methods=['PUT'])
def update(id):
    return f'user try to update product with {id}'

if __name__=="__main__":
    app.run(debug=True, host='0.0.0.0')