from django.shortcuts import render
from django.http import HttpResponse,HttpResponseRedirect
from .forms import UserDataForm
from .models import UserData
# Create your views here.
def home(request):
    if request.method == 'POST':
        form = UserDataForm(request.POST,request.FILES)
        if form.is_valid():
            form.save()
            return HttpResponseRedirect("/f/home/")
    else:
        form = UserDataForm()
        return render(request,"fileuploads/home.html",{'form':form})
    
def server(request):
    userdata = UserData.objects.all()
    return render(request,'fileuploads/server.html',{'userdata':userdata})