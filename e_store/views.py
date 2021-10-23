from django.shortcuts import render, get_object_or_404
from django.http import HttpResponse

from .models import Category, Product

def all_product(request):
    products = Product.products.all() 
    context = {
        'products_list' : products
    }
    return render(request, 'e_store/home.html', context)

def product_detail(request, product_slug):
    product = get_object_or_404(Product, slug=product_slug, in_stock=True)
    return HttpResponse(f"product detail {product} {product_slug} ")

# Create your views here.
