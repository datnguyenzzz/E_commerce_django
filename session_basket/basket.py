class Basket():
    def __init__(self, request):
        self.session = request.session 

        if 'skey' not in self.session:
            self.basket = {} 
        else:
            self.basket = self.session.get('skey')

    def save(self):
        self.session.modified = True
    
    def add(self, product, qty):
        product_id = str(product.id) 

        if product_id in self.basket:
            self.basket[product_id]['qty'] = qty 
        else:
            self.basket[product_id] = {'price':str(product.price), 'qty':qty} 
        
        self.save() 
        