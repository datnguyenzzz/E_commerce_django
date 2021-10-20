from django.contrib import admin
from .models import Category, Product

@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = ['name', 'slug'] 

@admin.register(Product) 
class ProductAdmin(admin.ModelAdmin):
    list_display = ['name', 'author', 'slug', 'price',
                    'in_stock', 'date_created', 'date_updated']
    
    list_filter = ['in_stock', 'is_active'] 
    list_editable = ['price', 'in_stock']

# Register your models here.
