from django.db import models

# Create your models here.

class Category(models.Model):
    name = models.CharField(max_length=255, db_index=True)

    def __str__(self):
        return self.name 

class Product(models.Model):
    name = models.CharField(max_length=255) 
    author = models.CharField(max_length=255, default='admin') 
    description = models.TextField(blank=True) 

    category = models.ForeignKey(Category, related_name='product', on_delete=models.CASCADE)

