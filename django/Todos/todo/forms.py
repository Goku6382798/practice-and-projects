from django import forms
from .models import Todos

class TodoForm(forms.ModelForm):
    class Meta:
        model = Todos
        fields = '__all__'
        widgets = {
            'todo':forms.TextInput(attrs={'class':'modelform-class'})
        }