from .basket import Basket


def session_basket(request):
    return {'basket': Basket(request)}