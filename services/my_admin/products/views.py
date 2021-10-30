from django.shortcuts import render
from django.http import HttpResponse
from rest_framework import viewsets, status 
from rest_framework.views import APIView
from rest_framework.response import Response

from .models import Products,User
from .serializers import ProductSerializer

class ProductViewSet(viewsets.ViewSet):
    def get_all(self, request):
        products = Products.objects.all() 
        serializer = ProductSerializer(products, many=True) 
        return Response(serializer.data)

    def create(self, request):
        return HttpResponse("create") 

    def get_wid(self, request, id):
        return HttpResponse("get_wid") 

    def update(self, request, id):
        return HttpResponse("update")

    def destroy(self, request, id):
        return HttpResponse("destroy") 
