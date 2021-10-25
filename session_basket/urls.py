from django.urls import path

from . import views

app_name = 'session_basket'

urlpatterns = [
    path('', views.basket_all, name='basket_all')
]
