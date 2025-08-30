from django.urls import path
from . import views
from django.views.generic.base import RedirectView
urlpatterns = [
    path('',views.HomeView.as_view(),name='home'), ##Whenever its a class based view we will use .as_view()
    path('add/',views.AddView.as_view()),
    #path('about/',TemplateView.as_view(template_view='todo/about.html')) ##no need of render and all the stuff
    path('about/',views.AboutView.as_view()),
    path('redirect/',views.RedirectView.as_view()),
    #path('todo/<int:id>',views.details,name='todo'), ##For function based detail view
    path('todo/<int:pk>/',views.DetailView.as_view(),name='todo'),
    path('update/<int:pk>/'.views.UpdateTodoView.as_view(),name='up'),
    path('update/<int:pk>/'.views.DeleteTodoView.as_view(),name='delete')
]