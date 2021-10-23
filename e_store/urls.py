from django.urls import path 

from . import views 

app_name = 'e_store'

urlpatterns = [
    path('',views.all_product, name='product_list'),
    path('<slug:product_slug>', views.product_detail, name='product_detail'),
    path('shop/<slug:category_slug>/', views.category_detail, name='category_list'),
]
