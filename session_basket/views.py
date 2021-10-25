from django.shortcuts import render
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from .basket import Basket

# Create your views here.

def basket_all(request):
    basket = Basket(request)
    return HttpResponse("Mew Mew all")

def basket_add(request):
    basket = Basket(request) 
    return HttpResponse("Mew Mew add")
