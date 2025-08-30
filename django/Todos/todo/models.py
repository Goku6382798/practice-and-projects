from django.db import models

# Create your models here.
class Todos(models.Model):
    todo = models.CharField(max_length=255)