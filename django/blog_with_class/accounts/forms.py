from django.contrib.auth.forms import UserCreationForm, AuthenticationForm, UsernameField
from django.contrib.auth.models import User
from django import forms
class RegisterForm(UserCreationForm):
    email = forms.EmailField(required=True,widget=forms.EmailInput(attrs={'class':'form-control'}))
    first_name = forms.CharField(required=True,widget=forms.TextInput(attrs={'class':'form-control'}))
    last_name = forms.CharField(required=True,widget=forms.TextInput(attrs={'class':'form-control'}))
    password1 = forms.CharField(required=True,widget=forms.PasswordInput(attrs={'class':'form-control'}))
    password2 = forms.CharField(required=True,widget=forms.PasswordInput(attrs={'class':'form-control'}))
    class Meta:
        model = User
        fields = ['username','email','first_name','last_name']
        widgets = {
            'username':forms.TextInput(attrs={'class':'form-control'})
        }

class LoginForm(AuthenticationForm):
    username = UsernameField(widget=forms.TextInput(attrs={"autofocus": True,'class':'form-control','placeholder':'Username'}))
    password = forms.CharField(widget=forms.PasswordInput(attrs={'class':'form-control','placeholder':'Password'}))