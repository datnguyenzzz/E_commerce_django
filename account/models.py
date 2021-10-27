from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, BaseUserManager
from django_countries.fields import CountryField
from django.utils.translation import gettext_lazy as _

class CustomUserManager(BaseUserManager):
    def create_user(self, email, user_name, password, **other_fields):
        if not email:
            raise ValueError(_('You must provide an email'))  
        
        email = self.normalize_email(email) 
        user = self.model(email=email, user_name = user_name, **other_fields) 

        user.set_password(password)
        user.save() 
        return user

    def create_superuser(self, email, user_name, password, **other_fields):
        other_fields.setdefault('is_staff', True) 
        other_fields.setdefault('is_superuser', True) 
        other_fields.setdefault('is_active', True)

        if other_fields.get('is_staff') is not True:
            raise ValueError(
                'Superuser must be assigned to "is_staff=True".')
        if other_fields.get('is_superuser') is not True:
            raise ValueError(
                'Superuser must be assigned to "is_superuser=True".')

        return self.create_user(email, user_name, password, **other_fields)

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

