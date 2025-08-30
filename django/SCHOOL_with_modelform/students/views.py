from django.shortcuts import render
from django.http import HttpResponse
#from django.template.response import TemplateResponse
# Create your views here.

def set(request):
    #raise Exception("Set raises an exception") ## Just for demonstration of middleware hooks
    print('set view is called')
#    response = TemplateResponse(request,"students/home.html")
    response = render(request,"students/home.html")
    response.set_cookie('theme','dark',max_age=5)
    response.set_cookie('name','Rahul')
    return response

def get(request):
    print(request)
    theme = request.COOKIES['name']
    return HttpResponse(f"<h1>GET</h1>{theme}")

def delete(request):
    response = HttpResponse("Deleted")
    response.delete_cookie("name")
    return response

def update(request):
    response = HttpResponse('<h1>Updated</h1>')
    response.delete_cookie("name",'Simar')
    return response