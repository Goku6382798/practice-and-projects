from django.db import models

# Create your models here.
class Husband(models.Model):
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name

class Wife(models.Model):
    name = models.CharField(max_length=255)
    husband = models.OneToOneField(Husband,on_delete=models.CASCADE)  #in on_delete=models.PROTECT/CASCADE/SET_NULL,null=TRUE