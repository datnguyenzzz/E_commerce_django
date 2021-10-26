from e_store.models import Product

class Basket():
    def __init__(self, request):
        self.session = request.session 

        if 'session_key' not in self.session:
            # link session to basket
            self.basket = self.session['session_key'] = {} 
        else:
            self.basket = self.session.get('session_key')

    def save(self):
        self.session.modified = True
    
    def add(self, product, qty):
        product_id = str(product.id) 

        if product_id in self.basket:
            self.basket[product_id]['qty'] = qty 
        else:
            self.basket[product_id] = {'price':str(product.price), 'qty':qty} 
        
        self.save() 
    
    def __len__(self):
        return sum(item['qty'] for item in self.basket.values()) 
    
    def __iter__(self):
        product_ids = self.basket.keys() 
        #filter throught specific manager filter (is_active)
        products = Product.products.filter(id__in = product_ids)
        print(products)
        for item in products:
            print(item)
            yield item

        