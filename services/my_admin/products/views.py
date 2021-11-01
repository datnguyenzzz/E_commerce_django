from django.shortcuts import render
from django.http import HttpResponse
from rest_framework import viewsets, status 
from rest_framework.views import APIView
from rest_framework.response import Response

from .models import Products,User
from .serializers import ProductSerializer

from producer import topic_publish,fanout_publish

import random

class ProductViewSet(viewsets.ViewSet):
    def get_all(self, request):
        products = Products.products.all() 
        serializer = ProductSerializer(products, many=True) 
        return Response(serializer.data)

    def create(self, request):
        print(type(request.data))
        serializer = ProductSerializer(data=request.data) 
        serializer.is_valid(raise_exception=True) 
        serializer.save() 

        method = "product_create"
        fanout_publish(method, serializer.data)

        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def get_wid(self, request, id):
        product = Products.products.get(id=id)
        serializer = ProductSerializer(product)
        return Response(serializer.data)

    def update(self, request, id):
        product = Products.products.get(id=id) 
        serializer = ProductSerializer(instance=product, data=request.data)
        serializer.is_valid(raise_exception=True) 
        serializer.save() 

        method = "product_update"
        fanout_publish(method, serializer.data)

        return Response(serializer.data, status=status.HTTP_202_ACCEPTED)

    def destroy(self, request, id):
        product = Products.products.get(id=id) 
        product.delete() 

        method = "product_delete"
        fanout_publish(method, id)

        return Response(status=status.HTTP_204_NO_CONTENT)

class UserAPIView(APIView):
    def get(self, request):
        users = User.objects.all() 
        user = random.choice(users) 
        return Response({
            'id': user.id
        })
