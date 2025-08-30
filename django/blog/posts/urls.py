from django.urls import path
from . import views

urlpatterns = [
    path('',views.home, name='home'),
    path('<int:id>/',views.post, name='post'),
    path('search/',views.search,name='search'),
    path('tags/<int:id>/',views.Tags, name='tag'),
]