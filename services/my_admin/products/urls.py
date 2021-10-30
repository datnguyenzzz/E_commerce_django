from django.urls import path 

from .views import ProductViewSet, UserAPIView

urlpatterns = [
    path('products', ProductViewSet.as_view({
        'get': 'get_all',
        'post': 'create'
    })), 

    path('products/<str:id>', ProductViewSet.as_view({
        'get': 'get_wid', 
        'put': 'update',
        'delete': 'destroy'
    })),
    path('user', UserAPIView.as_view())
]
