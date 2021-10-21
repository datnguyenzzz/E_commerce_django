from django.test import TestCase 
from django.contrib.auth.models import User

from e_store.models import Category, Product

class TestCategoriesModel(TestCase):
    def setUp(self) :
        #create test DB table
        self.data1 = Category.objects.create(name='test_name', slug='test-slug')
    
    def test_category_model_entry(self):
        data = self.data1
        self.assertTrue(isinstance(data, Category))
        self.assertEqual(str(data), 'test_name')

class TestProductsModel(TestCase):
    def setUp(self):
        Category.objects.create(name='test_name', slug='test-slug')
        User.objects.create(username='admin') 

        self.data1 = Product.objects.create(category_id=1, name='django beginners', created_by_id=1,
                                            slug='django-beginners', price='20.00', image='django')
    
    def test_products_model_entry(self):
        data = self.data1 
        self.assertTrue(isinstance(data, Product))
        self.assertEqual(str(data), 'django beginners')