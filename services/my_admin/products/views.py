from django.shortcuts import render
from django.http import HttpResponse
from rest_framework import viewsets, status 
from rest_framework.views import APIView

class ProductViewSet(viewsets.ViewSet):
    def get_all(self, request):
        return HttpResponse("get_all")  

    def create(self, request):
        return HttpResponse("create") 

    def get_wid(self, request, id):
        return HttpResponse("get_wid") 

    def update(self, request, id):
        return HttpResponse("update")

    def destroy(self, request, id):
        return HttpResponse("destroy") 
