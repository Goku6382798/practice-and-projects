from django import forms
from django.core import validators
from .models import Teacher

def start_s(value):
    if value and value[0].lower() != 's':
        raise forms.ValidationError("Name should start with letter s")

class TeachersForm(forms.ModelForm):
    class Meta:
        #name = forms.CharField(validators=[validators.MaxLengthValidator(10),start_s],widget=forms.TextInput(attrs={'class':'form-control'}),label='Your Name')    ###we can also use this way and clean data method as we used in another SCHOOL project
        model = Teacher
        fields = ['name','email','phone_number','bio']  ##we can also use '__all__'
        #exclude = ['email']
        labels = {
            'name':'Your Name',
            'email':'Your Email',
            'bio':'Your Details',
            'phone_number':'Contact Number' 
        }
        widgets = {
            'name':forms.TextInput(attrs={'class':'form-control'}),
            'email':forms.EmailInput(attrs={'class':'form-control'}),
            'phone_number':forms.NumberInput(attrs={'class':'form-control'}),
            'bio':forms.Textarea(attrs={'class':'form-control'})
        }
        help_texts = {
            'email':'We only accept gmails'
        }
        error_messages = {
            'name':{
                'required':'Name field is required'
            }
        }