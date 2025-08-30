from django.shortcuts import render
from django.http import HttpResponse
# Create your views here.

def set(request):
    request.session['name'] = {'nam1':'simar','name2':'rahul'}
    request.session['fatherName'] = 'GOD'
    request.session.set_expiry(50) ##The session will expire after numbers of seconds
    return HttpResponse("Hello World")

def get(request):
    name = request.session['name']
    father_name = request.session['fatherName']
    request.session['name'] = 'Rahul'
    print(name)
    print(father_name)
    print(request.session.get_expiry_age)
    return HttpResponse(f'<h1>GET VIEW</h1> {name}')

def delete(request):
    #del request.session['name'] ##This method only deletes from the client end to delete it from the database as well we will delete it using flush
    #del request.session['fatherName']
    request.session.flush() ##It deletes the current session from the database
    request.session.clear_flushed() ##It deletes the expired session from the database
    return HttpResponse('<h1>Delete View</h1>')

def update(request):
    request.session['name']['nam1'] = 'John'
    request.session.modiefied = True
    return HttpResponse("update page")