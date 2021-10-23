from django.shortcuts import render, get_object_or_404
from django.http import HttpResponse

from .models import Category, Product

def all_product(request):
    products = Product.products.all() 
    context = {
        'products_list' : products
    }
    return render(request, 'e_store/home.html', context)

def product_detail(request, slug):
    return HttpResponse(f"product detail {slug}")

# Create your views here.
