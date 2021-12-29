from django.db import models
from django.urls import reverse

class ActiveProductManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(is_active=True)

class Products(models.Model):
    name = models.CharField(max_length=255) 
    author = models.CharField(max_length=255, default='admin') 
    image = models.CharField(max_length=255) 
    likes = models.PositiveIntegerField(default=0)

    price = models.DecimalField(max_digits=5, decimal_places=2) 
    is_active = models.BooleanField(default=True) 

    objects = models.Manager() 
    products = ActiveProductManager()

    class Meta:
        verbose_name_plural = 'products' 
    
    def __str__(self):
        return self.name

class User(models.Model):
    is_merchant = models.BooleanField(default=False)