from django.shortcuts import render
from .forms import TeachersForm
from django.http import HttpResponseRedirect, HttpResponse
from .models import Teacher
# Create your views here.
def home(request):
    if request.method == 'POST':
        form = TeachersForm(request.POST)
        if form.is_valid():
            form.save()
            return HttpResponseRedirect('/teacher/thank-you/')
    else:
        form = TeachersForm()
    return render(request,'teachers/home.html',{'form':form})

def thank(request):
    return HttpResponse("FORM SUBMITTED!!!")

def data(request):
    alldata = Teacher.objects.all()
    print(request.COOKIES['name'])
    return render(request, 'teachers/alldata.html',{
        'alldata':alldata
    })

def update(request,id):
    teacher = Teacher.objects.get(id=id)
    if request.method == 'POST':
        form = TeachersForm(request.POST,instance=teacher)
        if form.is_valid():
            form.save()
            return HttpResponseRedirect('/teacher/all-data/')
    else:        
        form = TeachersForm(instance=teacher)

def delete(request,id):
    if request.method == 'POST':
        teacher = Teacher.objects.get(id=id)
        teacher.delete()
    return HttpResponseRedirect('/teacher/home/')