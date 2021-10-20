from django.db import models

# Create your models here.

class ActiveProductManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(is_active=True)

class Category(models.Model):
    name = models.CharField(max_length=255, db_index=True)
    slug = models.SlugField(max_length=255, unique=True)

    def __str__(self):
        return self.name 

class Product(models.Model):
    name = models.CharField(max_length=255) 
    author = models.CharField(max_length=255, default='admin') 
    description = models.TextField(blank=True) 
    slug = models.SlugField(max_length=255)

    price = models.DecimalField(max_digits=5, decimal_places=2) 
    is_active = models.BooleanField(default=True) 
    in_stock = models.BooleanField(default=True)

    date_created = models.DateTimeField(auto_now_add=True) 
    date_updated = models.DateTimeField(auto_now=True)

    category = models.ForeignKey(Category, related_name='product', on_delete=models.CASCADE)
    objects = models.Manager() 
    products = ActiveProductManager()

    def __str__(self):
        return self.name