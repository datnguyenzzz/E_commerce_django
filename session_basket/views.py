from django.shortcuts import render,get_object_or_404
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from .basket import Basket
from e_store.models import Product

# Create your views here.

def basket_all(request):
    basket = Basket(request)
    print(basket.basket)
    return HttpResponse(basket)

def basket_add(request):
    basket = Basket(request) 
    product_id = int(request.POST.get('product_id'))
    product_qty = int(request.POST.get('product_qty'))
    product = get_object_or_404(Product, id=product_id) 
    
    basket.add(product=product, qty=product_qty) 
    print(basket.basket)
    return HttpResponse("Mew Mew add")
