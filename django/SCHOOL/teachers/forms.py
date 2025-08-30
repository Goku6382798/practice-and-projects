from django import forms
from django.core import validators

def start_s(value):
    if value and value[0].lower() != 's':
        raise forms.ValidationError("Name should start with letter s")

class TeachersForm(forms.Form):
    name = forms.CharField(
        validators=[validators.MaxLengthValidator(10),start_s],
        min_length=5,
        label='Your Name',
        label_suffix="",
        error_messages={'required': "Your name field cannot be empty."},
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )

    email = forms.EmailField(
        required=False,
        label='Your Email',
        label_suffix="",
        help_text="We only accept email from gmail.com",
        widget=forms.EmailInput(attrs={'class': 'form-control'}),
        validators=[start_s]
    )

    phone_number = forms.IntegerField(
        label='Contact Number',
        label_suffix="",
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )

    Bio = forms.CharField(
        widget=forms.Textarea(attrs={'cols':5,'placeholder':'Bio','class':'form-control'})
    )

    #def clean(self):
        #cleaned_data = super().clean()
        #name = self.cleaned_data['name']
        #email = self.cleaned_data['email']

        #if name[0] != 's' and name[0] != 'S':
            #raise forms.ValidationError("First letter of name should be s")
        #if email[0] != 's' and email[0] != 'S':
            #raise forms.ValidationError("Email should start with letter S")
