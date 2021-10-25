class Basket():
    def __init__(self, request):
        self.session = request.session 

        if 'skey' not in self.session:
            self.basket = {} 
        else:
            self.basket = self.session.get('skey')
        