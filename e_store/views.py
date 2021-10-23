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
    return render(request, 'e_store/product/detail.html', {'product': product})

def category_detail(request, category_slug=None):
    category = get_object_or_404(Category, slug=category_slug)
    products = Product.objects.filter(category=category)
    return render(request, 'e_store/product/category.html', {'category': category, 'products': products})


# Create your views here.
