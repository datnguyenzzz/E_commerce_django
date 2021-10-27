from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, BaseUserManager
from django_countries.fields import CountryField
from django.utils.translation import gettext_lazy as _

class CustomUserManager(BaseUserManager):
    pass

# Create your models here.
class UserBase(AbstractBaseUser, PermissionsMixin): 
    #basics
    email = models.EmailField(_('email address'), unique=True)
    user_name = models.CharField(max_length=100, unique=True)
    first_name = models.CharField(max_length=100, unique=True) 
    about = models.TextField(_('about'), max_length=500, blank=True) 
    #location 
    country = CountryField() 
    city = models.CharField(max_length=100, blank=True)
    phone_number = models.CharField(max_length=10, blank=True)
    zipcode = models.CharField(max_length=10, blank=True)
    address_line_1 = models.CharField(max_length=100, blank=True) 
    address_line_2 = models.CharField(max_length=100, blank=True) 
    #status 
    is_active = models.BooleanField(default=False) 
    is_staff = models.BooleanField(default=False) 
    created = models.DateTimeField(auto_now_add=True) 
    updated = models.DateTimeField(auto_now_add=True) 

    USERNAME_FIELD = 'email' 
    REQUIRED_FIELDS = ['user_name']

    objects = CustomUserManager()

    class Meta:
        verbose_name = 'Account' 
        verbose_name_plural = 'Accounts' 

    def __str__(self):
        return self.user_name

