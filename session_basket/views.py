from django.shortcuts import render
from django.http import HttpResponse

from .basket import Basket

# Create your views here.

def basket_all(request):
    basket = Basket(request)
    return HttpResponse(basket)
