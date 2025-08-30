from django.shortcuts import render
from django.http import HttpResponse,HttpResponseRedirect
from .forms import RegisterForm, LoginForm
from django.urls import reverse
from django.contrib.auth import authenticate,login,logout
# Create your views here.
def register(request):
    if request.user.is_authenticated:
        return HttpResponseRedirect('/post/home')
    else:
        if request.method == 'POST':
            form = RegisterForm(request.POST)
            if form.is_valid():
                form.save()
                homeurl = reverse('home')
                return HttpResponseRedirect(homeurl)
        else:
            form = RegisterForm()
        return render(request,'accounts/register.html',{
            'form':form
    })

def auth_login(request):
    if request.user.is_authenticated:
        return HttpResponseRedirect('/post/home')
    else:
        if request.method == 'POST':
            form = LoginForm(request=request,data=request.POST)
            if form.is_valid():
                username = form.cleaned_data['username']
                password = form.cleaned_data['password']
                user = authenticate(username=username,password=password)
                if user is not None:
                    login(request,user)
                    homeurl = reverse('home')
                    return HttpResponseRedirect(homeurl)
        else:
            form = LoginForm()
        return render(request,'accounts/login.html',{'form':form})
    
def auth_logout(request):
    logout(request)
    return HttpResponseRedirect("/accounts/login/")