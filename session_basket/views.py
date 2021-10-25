from django.shortcuts import render
from django.http import HttpResponse

# Create your views here.

def basket_all(request):
    return HttpResponse('<p>This is basket</p>')
