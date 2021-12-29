from django.urls import path 

from .views import ProductViewSet 

urlpatterns = [
    path('products', ProductViewSet.as_view({
        'get': 'get_all'
    })),

    path('products/<str:id>', ProductViewSet.as_view({
        'get': 'get_wid'
    }))
]
